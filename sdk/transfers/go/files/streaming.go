package files

import (
	"context"
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

type DownloadStream struct {
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

func (c *Client) DownloadStream(ctx context.Context, in DownloadInput) (*DownloadStream, error) {
	if c == nil {
		return nil, fmt.Errorf("client is nil")
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
	var result *DownloadStream
	if capabilities != nil && capabilities.Features[featureDirectGet] {
		grant, err := c.CreateDownload(ctx, &loonfs.CreateDownloadRequest{NamespaceID: string(in.NamespaceID), Path: in.Path, RevisionNo: in.RevisionNo})
		if err != nil {
			return nil, err
		}
		if grant == nil || grant.ContentRef == nil {
			return nil, fmt.Errorf("download grant has no content reference")
		}
		if err := validateDownloadRanges(grant.Ranges, grant.ContentRef.SizeBytes); err != nil {
			return nil, err
		}
		result = &DownloadStream{
			Content:     &downloadRangeReader{ctx: ctx, client: c.transferHTTPClient(), ranges: grant.Ranges},
			NamespaceID: grant.NamespaceID, Path: grant.Path, RevisionNo: grant.RevisionNo, ContentRef: grant.ContentRef,
		}
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

func (c *Client) downloadProxiedStream(ctx context.Context, in DownloadInput) (*DownloadStream, error) {
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
		return nil, fmt.Errorf("revision %d not found for %s", *revisionNo, in.Path)
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
	return &DownloadStream{Content: response.Body, NamespaceID: in.NamespaceID, Path: in.Path, RevisionNo: *revisionNo, ContentRef: claim}, nil
}

func newChecksum(algorithm loonfs.ChecksumAlgorithm) (hash.Hash, error) {
	switch algorithm {
	case loonfs.ChecksumAlgorithmCrc32C:
		return crc32.New(crc32CTable), nil
	case loonfs.ChecksumAlgorithmCrc64Nvme:
		return crc64.New(crc64NVMeTable), nil
	default:
		return nil, fmt.Errorf("unsupported checksum algorithm %s", algorithm)
	}
}

type verifiedReader struct {
	ctx              context.Context
	body             io.ReadCloser
	cancel           context.CancelFunc
	hash             hash.Hash
	expectedSize     int64
	expectedChecksum string
	count            int64
	terminal         error
	closed           bool
}

func newVerifiedReader(ctx context.Context, body io.ReadCloser, expected *loonfs.ContentRef, cancel context.CancelFunc) (*verifiedReader, error) {
	if expected == nil || expected.Checksum == nil {
		return nil, fmt.Errorf("invalid content reference")
	}
	if expected.SizeBytes < 0 {
		return nil, fmt.Errorf("invalid download size")
	}
	digest, err := newChecksum(expected.Checksum.Algorithm)
	if err != nil {
		return nil, err
	}
	return &verifiedReader{ctx: ctx, body: body, cancel: cancel, hash: digest, expectedSize: expected.SizeBytes, expectedChecksum: expected.Checksum.Value}, nil
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
	if err != nil && err != io.EOF {
		r.terminal = err
		r.Close()
		return n, err
	}
	r.count += int64(n)
	if r.count > r.expectedSize {
		r.terminal = fmt.Errorf("download exceeded expected size %d", r.expectedSize)
		r.Close()
		return 0, r.terminal
	}
	r.hash.Write(buffer[:n])
	if err == io.EOF {
		if r.count != r.expectedSize {
			err = fmt.Errorf("download returned %d bytes, expected %d", r.count, r.expectedSize)
		} else if hex.EncodeToString(r.hash.Sum(nil)) != r.expectedChecksum {
			err = fmt.Errorf("download checksum mismatch")
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

func validateDownloadRanges(ranges []*loonfs.DownloadRange, sizeBytes int64) error {
	if len(ranges) == 0 || (sizeBytes == 0 && len(ranges) != 1) {
		return fmt.Errorf("download grant has invalid ranges")
	}
	var offset int64
	for _, part := range ranges {
		if part == nil || part.StartOffset != offset || part.Length < 0 ||
			(part.Length == 0 && sizeBytes != 0) || part.Length > sizeBytes-offset {
			return fmt.Errorf("download grant has invalid ranges")
		}
		if part.Access == nil || part.Access.PresignedURL == nil || part.Access.PresignedURL.Method != http.MethodGet {
			return fmt.Errorf("download grant must use GET")
		}
		offset += part.Length
	}
	if offset != sizeBytes {
		return fmt.Errorf("download grant has invalid ranges")
	}
	return nil
}

type downloadRangeReader struct {
	ctx       context.Context
	client    core.HTTPClient
	ranges    []*loonfs.DownloadRange
	body      io.ReadCloser
	remaining int64
	closed    bool
}

func (r *downloadRangeReader) Read(buffer []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if len(buffer) == 0 {
		return 0, nil
	}
	for {
		if r.body == nil {
			if len(r.ranges) == 0 {
				return 0, io.EOF
			}
			part := r.ranges[0]
			r.ranges = r.ranges[1:]
			if part.Length == 0 {
				continue
			}
			response, err := sendPresignedWithClient(r.ctx, r.client, part.Access, http.MethodGet, nil, 0)
			if err != nil {
				return 0, err
			}
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				defer response.Body.Close()
				return 0, responseStatusError(response)
			}
			r.body, r.remaining = response.Body, part.Length
		}
		n, err := r.body.Read(buffer)
		if err != nil && err != io.EOF {
			return n, err
		}
		r.remaining -= int64(n)
		if r.remaining < 0 {
			return 0, fmt.Errorf("download range exceeded its declared length")
		}
		if err == io.EOF {
			r.body.Close()
			r.body = nil
			if r.remaining != 0 {
				return n, fmt.Errorf("download range ended before its declared length")
			}
			if n == 0 {
				continue
			}
			return n, nil
		}
		return n, err
	}
}

func (r *downloadRangeReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}
