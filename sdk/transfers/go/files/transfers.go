package files

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"strings"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/commits"
	"github.com/loonfs/loonfs-sdk-go/core"
)

const (
	maxInlineBytes        = 64 * 1024
	maxAppendBytes        = 256 * 1024
	multipartMinimumBytes = 8 * 1024 * 1024

	featureDirectGet           = "filesystem.downloads.direct_get"
	featureDirectPut           = "filesystem.uploads.direct_put"
	featureDirectMultipart     = "filesystem.uploads.direct_multipart"
	limitUploadMaximumBytes    = "upload.service_proxied.max_content_bytes"
	limitDirectPutMaximumBytes = "upload.direct_put.max_content_bytes"

	crc64NVMePolynomial = 0x9a6c9329ac4bc9b5
)

var (
	crc32CTable         = crc32.MakeTable(crc32.Castagnoli)
	crc64NVMeTable      = crc64.MakeTable(crc64NVMePolynomial)
	presignedHTTPClient = &http.Client{
		Transport: http.DefaultTransport.(*http.Transport).Clone(),
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
)

type UploadInput struct {
	NamespaceID        loonfs.NamespaceID
	Path               loonfs.AbsolutePath
	Content            []byte
	CommitID           loonfs.CommitID
	Message            *string
	Behavior           loonfs.DestinationBehavior
	ExpectedInodeID    *loonfs.InodeID
	ExpectedRevisionNo *loonfs.RevisionNo
}

type PreparedContent struct {
	ContentRef   *loonfs.ContentRef
	ContentToken *loonfs.ContentToken
}

type PreparedFile interface {
	preparedFile()
}

func (*PreparedContent) preparedFile() {}

type InlinePreparedContent struct {
	content string
}

func (*InlinePreparedContent) preparedFile() {}

type PreparedUploadInput struct {
	NamespaceID        loonfs.NamespaceID
	Path               loonfs.AbsolutePath
	Prepared           PreparedFile
	CommitID           loonfs.CommitID
	Message            *string
	Behavior           loonfs.DestinationBehavior
	ExpectedInodeID    *loonfs.InodeID
	ExpectedRevisionNo *loonfs.RevisionNo
}

type AppendInput struct {
	NamespaceID        loonfs.NamespaceID
	Path               loonfs.AbsolutePath
	Content            []byte
	CommitID           loonfs.CommitID
	Message            *string
	ExpectedInodeID    *loonfs.InodeID
	ExpectedRevisionNo *loonfs.RevisionNo
}

type DownloadInput struct {
	NamespaceID loonfs.NamespaceID
	Path        loonfs.AbsolutePath
	RevisionNo  *loonfs.RevisionNo
}

type DownloadResult struct {
	Content     []byte
	NamespaceID loonfs.NamespaceID
	Path        loonfs.AbsolutePath
	RevisionNo  loonfs.RevisionNo
	ContentRef  *loonfs.ContentRef
}

func (c *Client) Upload(ctx context.Context, in UploadInput, opts ...core.RequestOption) (*loonfs.Commit, error) {
	size := int64(len(in.Content))
	return c.UploadStream(ctx, StreamUploadInput{
		NamespaceID: in.NamespaceID, Path: in.Path,
		Content: bytes.NewReader(in.Content), SizeBytes: &size,
		CommitID: in.CommitID, Message: in.Message, Behavior: in.Behavior,
		ExpectedInodeID: in.ExpectedInodeID, ExpectedRevisionNo: in.ExpectedRevisionNo,
	}, opts...)
}

func (c *Client) Prepare(ctx context.Context, namespaceID loonfs.NamespaceID, content []byte) (PreparedFile, error) {
	size := int64(len(content))
	return c.PrepareStream(ctx, namespaceID, bytes.NewReader(content), &size)
}

func (c *Client) UploadPrepared(ctx context.Context, in PreparedUploadInput, opts ...core.RequestOption) (*loonfs.Commit, error) {
	ctx, cancel := transferContext(ctx)
	defer cancel()
	commitID, err := c.publicationID(in.CommitID)
	if err != nil {
		return nil, err
	}
	var contentRef *loonfs.ContentRef
	var inlineContent *string
	var contentTokens []*loonfs.ContentToken
	switch prepared := in.Prepared.(type) {
	case *InlinePreparedContent:
		if prepared == nil {
			return nil, fmt.Errorf("prepared content is required")
		}
		inlineContent = &prepared.content
	case *PreparedContent:
		if prepared == nil || prepared.ContentRef == nil {
			return nil, fmt.Errorf("prepared content is required")
		}
		contentRef = prepared.ContentRef
		if prepared.ContentToken != nil {
			contentTokens = []*loonfs.ContentToken{prepared.ContentToken}
		}
	default:
		return nil, fmt.Errorf("prepared content is required")
	}
	commitsClient := commits.NewClient(c.options)
	behavior := in.Behavior
	if behavior == "" {
		behavior = loonfs.DestinationBehaviorNoReplace
	}
	committed, err := commitsClient.Create(ctx, &loonfs.CommitRequest{
		NamespaceID:   string(in.NamespaceID),
		CommitID:      commitID,
		ContentTokens: contentTokens,
		Message:       in.Message,
		Operations: []*loonfs.FilesystemOperation{
			{
				PutFile: &loonfs.FilesystemOperationPutFile{
					Behavior:           &behavior,
					ContentRef:         contentRef,
					InlineContent:      inlineContent,
					ExpectedInodeID:    in.ExpectedInodeID,
					ExpectedRevisionNo: in.ExpectedRevisionNo,
					Path:               in.Path,
				},
			},
		},
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("commit file: %w", err)
	}
	return committed, nil
}

func (c *Client) Append(ctx context.Context, in AppendInput, opts ...core.RequestOption) (*loonfs.Commit, error) {
	if len(in.Content) == 0 {
		return nil, fmt.Errorf("append content is empty")
	}
	if len(in.Content) > maxAppendBytes {
		return nil, fmt.Errorf("%d-byte append is larger than the %d-byte limit", len(in.Content), maxAppendBytes)
	}
	commitID, err := c.publicationID(in.CommitID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := transferContext(ctx)
	defer cancel()
	committed, err := commits.NewClient(c.options).Create(ctx, &loonfs.CommitRequest{
		NamespaceID: string(in.NamespaceID),
		CommitID:    commitID,
		Message:     in.Message,
		Operations: []*loonfs.FilesystemOperation{
			{
				AppendFile: &loonfs.FilesystemOperationAppendFile{
					Path:               in.Path,
					InlineContent:      base64.StdEncoding.EncodeToString(in.Content),
					ExpectedInodeID:    in.ExpectedInodeID,
					ExpectedRevisionNo: in.ExpectedRevisionNo,
				},
			},
		},
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("commit append: %w", err)
	}
	return committed, nil
}

func (c *Client) publicationID(commitID loonfs.CommitID) (loonfs.CommitID, error) {
	if c == nil {
		return "", fmt.Errorf("client is nil")
	}
	if commitID == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", fmt.Errorf("generate commit_id: %w", err)
		}
		commitID = loonfs.CommitID("c_" + hex.EncodeToString(random[:]))
	}
	return commitID, nil
}

func (c *Client) Download(ctx context.Context, in DownloadInput) (*DownloadResult, error) {
	stream, err := c.DownloadStream(ctx, in)
	if err != nil {
		return nil, err
	}
	defer stream.Content.Close()
	content, err := io.ReadAll(stream.Content)
	if err != nil {
		return nil, err
	}
	return &DownloadResult{Content: content, NamespaceID: stream.NamespaceID, Path: stream.Path, RevisionNo: stream.RevisionNo, ContentRef: stream.ContentRef}, nil
}

func fileProjection(entry *loonfs.PathEntry) (*loonfs.PathEntryFile, error) {
	if entry == nil {
		return nil, fmt.Errorf("path entry is nil")
	}
	if entry.File == nil {
		return nil, fmt.Errorf("path is a %s, not a file", entry.InodeKind)
	}
	return entry.File, nil
}

func createUploadRequest(
	capabilities *loonfs.CapabilityDocument,
	namespaceID loonfs.NamespaceID,
	sizeBytes int64,
) (*loonfs.CreateUploadRequest, error) {
	if capabilities == nil {
		return nil, fmt.Errorf("capability response is nil")
	}
	request := &loonfs.CreateUploadBody{}
	largeUpload := sizeBytes >= multipartMinimumBytes
	if largeUpload && capabilities.Features[featureDirectMultipart] {
		request.DirectMultipart = &loonfs.CreateUploadBodyDirectMultipart{}
	} else {
		proxyLimit, hasProxyLimit := capabilities.Limits[limitUploadMaximumBytes]
		fitsProxy := !hasProxyLimit || sizeBytes <= proxyLimit
		directPutLimit, hasDirectPutLimit := capabilities.Limits[limitDirectPutMaximumBytes]
		fitsDirectPut := !hasDirectPutLimit || sizeBytes <= directPutLimit
		switch {
		case (largeUpload || !fitsProxy) && capabilities.Features[featureDirectPut] && fitsDirectPut:
			request.DirectPut = &loonfs.CreateUploadBodyDirectPut{
				SizeBytes: &sizeBytes,
			}
		case fitsProxy:
			request.ServiceProxied = &loonfs.CreateUploadBodyServiceProxied{}
		default:
			return nil, fmt.Errorf("source fits no advertised upload transport")
		}
	}
	return &loonfs.CreateUploadRequest{
		NamespaceID: string(namespaceID),
		Body:        request,
	}, nil
}

func completedUploadStatus(response *loonfs.UploadSession) (*loonfs.UploadSessionStatusCompleted, error) {
	if response == nil {
		return nil, fmt.Errorf("upload session response is nil")
	}
	if response.Completed == nil || response.Completed.ContentRef == nil {
		return nil, fmt.Errorf("upload is %s, not completed", response.Status)
	}
	return response.Completed, nil
}

func sendPresignedWithClient(ctx context.Context, client core.HTTPClient, access *loonfs.ObjectTransferAccess, expectedMethod string, body io.Reader, contentLength int64) (*http.Response, error) {
	if access == nil || access.PresignedURL == nil {
		return nil, fmt.Errorf("unsupported object transfer access")
	}
	presigned := access.PresignedURL
	if presigned.Method != expectedMethod {
		operation := "upload"
		if expectedMethod == http.MethodGet {
			operation = "download"
		}
		return nil, fmt.Errorf("%s grant must use %s", operation, expectedMethod)
	}
	request, err := http.NewRequestWithContext(ctx, expectedMethod, presigned.URL, body)
	if err != nil {
		return nil, fmt.Errorf("build presigned request: %w", err)
	}
	if body != nil {
		request.ContentLength = contentLength
	}
	for name, value := range presigned.Headers {
		if strings.EqualFold(name, "host") {
			request.Host = value
			continue
		}
		request.Header.Set(name, value)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send presigned request: %w", err)
	}
	return response, nil
}

func responseStatusError(response *http.Response) error {
	return fmt.Errorf("presigned request failed with HTTP %d", response.StatusCode)
}

func computeChecksum(algorithm loonfs.ChecksumAlgorithm, payload []byte) (*loonfs.Checksum, error) {
	digest, err := newChecksum(algorithm)
	if err != nil {
		return nil, err
	}
	digest.Write(payload)
	return &loonfs.Checksum{Algorithm: algorithm, Value: hex.EncodeToString(digest.Sum(nil))}, nil
}
