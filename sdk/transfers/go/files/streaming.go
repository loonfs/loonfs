package files

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/capabilities"
	"github.com/loonfs/loonfs-sdk-go/core"
	"github.com/loonfs/loonfs-sdk-go/internal"
)

const defaultTransferTimeout = 60 * time.Second
const transferChunkBytes = 64 * 1024

// FileDownloadStream holds a live response. Read Content to successful EOF to
// verify the size and checksum; Close releases it without asserting verification.
type FileDownloadStream struct {
	Content     io.ReadCloser
	NamespaceID loonfs.NamespaceID
	Path        loonfs.AbsolutePath
	RevisionNo  loonfs.RevisionNo
	ContentRef  *loonfs.ContentRef
}

func transferContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, defaultTransferTimeout)
}

// DownloadStream opens a verified stream with backpressure. The caller's context
// covers discovery and the body; without a deadline the operation has 60 seconds.
// Closing Content also cancels the operation. No failed body is replayed.
func (c *Client) DownloadStream(ctx context.Context, in DownloadInput) (*FileDownloadStream, error) {
	if c == nil {
		return nil, fmt.Errorf("transfers: client is nil")
	}
	ctx, cancel := transferContext(ctx)
	opened := false
	defer func() {
		if !opened {
			cancel()
		}
	}()
	capabilities, err := capabilities.NewClient(c.options).Retrieve(ctx)
	if err != nil {
		return nil, err
	}
	var result *FileDownloadStream
	if capabilities != nil && capabilities.Features[featureDirectGet] {
		grant, err := c.CreateDownload(ctx, &loonfs.BeginDownloadRequest{NamespaceID: string(in.NamespaceID), Path: in.Path, RevisionNo: in.RevisionNo})
		if err != nil {
			return nil, err
		}
		if grant == nil || grant.ContentRef == nil {
			return nil, fmt.Errorf("transfers: download grant has no content reference")
		}
		response, err := sendPresignedWithClient(ctx, c.transferHTTPClient(), grant.Access, http.MethodGet, nil, 0)
		if err != nil {
			return nil, err
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			defer response.Body.Close()
			return nil, responseStatusError(response)
		}
		result = &FileDownloadStream{Content: response.Body, NamespaceID: grant.NamespaceID, Path: grant.Path, RevisionNo: grant.RevisionNo, ContentRef: grant.ContentRef}
	} else {
		result, err = c.downloadProxiedStream(ctx, in)
		if err != nil {
			return nil, err
		}
	}
	verified, err := newVerifiedReader(ctx, result.Content, result.ContentRef, cancel)
	if err != nil {
		result.Content.Close()
		return nil, err
	}
	result.Content = verified
	opened = true
	return result, nil
}

func (c *Client) transferHTTPClient() core.HTTPClient {
	if c.options.HTTPClient != nil {
		if configured, ok := c.options.HTTPClient.(*http.Client); ok {
			copied := *configured
			copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
			return &copied
		}
		return c.options.HTTPClient
	}
	return presignedHTTPClient
}

func (c *Client) downloadProxiedStream(ctx context.Context, in DownloadInput) (*FileDownloadStream, error) {
	revisionNo := in.RevisionNo
	var claim *loonfs.ContentRef
	if revisionNo == nil {
		entry, err := c.Retrieve(ctx, &loonfs.GetPathEntryRequest{NamespaceID: string(in.NamespaceID), Path: string(in.Path)})
		if err != nil {
			return nil, err
		}
		file, err := fileProjection(entry)
		if err != nil {
			return nil, err
		}
		revision := file.RevisionNo
		revisionNo, claim = &revision, file.ContentRef
	} else {
		page, err := c.ListRevisions(ctx, &loonfs.ListFileRevisionsRequest{NamespaceID: string(in.NamespaceID), Path: string(in.Path)})
		if err != nil {
			return nil, err
		}
		iterator := page.Iterator()
		for iterator.Next(ctx) {
			revision := iterator.Current()
			if revision.RevisionNo == *revisionNo {
				claim = revision.ContentRef
				break
			}
		}
		if err := iterator.Err(); err != nil {
			return nil, err
		}
	}
	if claim == nil {
		return nil, fmt.Errorf("transfers: revision not found for %s", in.Path)
	}
	query := url.Values{"path": {string(in.Path)}, "revision_no": {strconv.FormatInt(int64(*revisionNo), 10)}}
	endpoint := strings.TrimRight(c.baseURL, "/") + "/v0/namespaces/" + url.PathEscape(string(in.NamespaceID)) + "/filesystem/content?" + query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	request.Header = c.options.ToHeader()
	response, err := c.transferHTTPClient().Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		return nil, internal.NewErrorDecoder(loonfs.ErrorCodes)(response.StatusCode, response.Header, io.LimitReader(response.Body, 64*1024))
	}
	return &FileDownloadStream{Content: response.Body, NamespaceID: in.NamespaceID, Path: in.Path, RevisionNo: *revisionNo, ContentRef: claim}, nil
}

func newChecksum(algorithm loonfs.ChecksumAlgorithm) (hash.Hash, error) {
	switch algorithm {
	case loonfs.ChecksumAlgorithmSha256:
		return sha256.New(), nil
	case loonfs.ChecksumAlgorithmCrc32C:
		return crc32.New(crc32CTable), nil
	case loonfs.ChecksumAlgorithmCrc64Nvme:
		return crc64.New(crc64NVMeTable), nil
	default:
		return nil, fmt.Errorf("unsupported checksum algorithm %q", algorithm)
	}
}

type verifiedReader struct {
	ctx      context.Context
	body     io.ReadCloser
	cancel   context.CancelFunc
	hash     hash.Hash
	expected *loonfs.ContentRef
	count    int64
	terminal error
	closed   bool
}

func newVerifiedReader(ctx context.Context, body io.ReadCloser, expected *loonfs.ContentRef, cancel context.CancelFunc) (*verifiedReader, error) {
	if expected == nil || expected.Checksum == nil || expected.SizeBytes < 0 {
		return nil, fmt.Errorf("transfers: invalid content reference")
	}
	digest, err := newChecksum(expected.Checksum.Algorithm)
	if err != nil {
		return nil, err
	}
	return &verifiedReader{ctx: ctx, body: body, cancel: cancel, hash: digest, expected: expected}, nil
}

func (r *verifiedReader) Read(buffer []byte) (int, error) {
	if r.terminal != nil {
		return 0, r.terminal
	}
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if err := r.ctx.Err(); err != nil {
		r.terminal = err
		r.Close()
		return 0, err
	}
	if len(buffer) > transferChunkBytes {
		buffer = buffer[:transferChunkBytes]
	}
	n, err := r.body.Read(buffer)
	r.count += int64(n)
	if r.count > r.expected.SizeBytes {
		r.terminal = fmt.Errorf("transfers: download exceeded expected size %d", r.expected.SizeBytes)
		r.Close()
		return 0, r.terminal
	}
	r.hash.Write(buffer[:n])
	if err == io.EOF {
		if r.count != r.expected.SizeBytes {
			err = fmt.Errorf("transfers: download returned %d bytes, expected %d", r.count, r.expected.SizeBytes)
		} else if hex.EncodeToString(r.hash.Sum(nil)) != r.expected.Checksum.Value {
			err = fmt.Errorf("transfers: download checksum mismatch")
		}
	}
	if err != nil {
		r.terminal = err
		r.Close()
	}
	return n, err
}

func (r *verifiedReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.cancel()
	return r.body.Close()
}
