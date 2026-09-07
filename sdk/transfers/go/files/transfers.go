package files

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"strings"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/capabilities"
	"github.com/loonfs/loonfs-sdk-go/commits"
	"github.com/loonfs/loonfs-sdk-go/core"
	"github.com/loonfs/loonfs-sdk-go/option"
	"github.com/loonfs/loonfs-sdk-go/uploads"
)

const (
	multipartMinimumBytes = 8 * 1024 * 1024

	featureDirectGet           = "filesystem.downloads.direct_get"
	featureDirectPut           = "filesystem.uploads.direct_put"
	featureDirectMultipart     = "filesystem.uploads.direct_multipart"
	limitUploadMaximumBytes    = "upload.max_content_bytes"
	limitDirectPutMaximumBytes = "upload.direct_put_max_content_bytes"

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

// UploadInput describes one in-memory file write.
type UploadInput struct {
	NamespaceID        loonfs.NamespaceID
	Path               loonfs.AbsolutePath
	Content            []byte
	Actor              *loonfs.ActorRef
	CommitID           loonfs.CommitID
	Message            *string
	Behavior           loonfs.DestinationBehavior
	ExpectedInodeID    *string
	ExpectedRevisionNo *loonfs.RevisionNo
}

// PreparedFileContent is a completed upload retained for publication retries.
// Preparation does not publish a file or extend the upload lifetime.
// Treat its reference and token as immutable.
type PreparedFileContent struct {
	ContentRef   *loonfs.ContentRef
	ContentToken *loonfs.ContentToken
}

// PreparedUploadInput publishes completed content without uploading again.
type PreparedUploadInput struct {
	NamespaceID        loonfs.NamespaceID
	Path               loonfs.AbsolutePath
	Prepared           *PreparedFileContent
	Actor              *loonfs.ActorRef
	CommitID           loonfs.CommitID
	Message            *string
	Behavior           loonfs.DestinationBehavior
	ExpectedInodeID    *string
	ExpectedRevisionNo *loonfs.RevisionNo
}

// UploadResult identifies the commit that made the new revision visible.
type UploadResult struct {
	NamespaceID  loonfs.NamespaceID
	CommitID     loonfs.CommitID
	CommittedSeq loonfs.ChangeSeq
}

// DownloadInput describes one current or retained file revision to read.
type DownloadInput struct {
	NamespaceID loonfs.NamespaceID
	Path        loonfs.AbsolutePath
	RevisionNo  *loonfs.RevisionNo
}

// DownloadResult contains file bytes and the resolved revision facts.
type DownloadResult struct {
	Content     []byte
	NamespaceID loonfs.NamespaceID
	Path        loonfs.AbsolutePath
	RevisionNo  loonfs.RevisionNo
	ContentRef  *loonfs.ContentRef
}

// Upload uploads fresh content and publishes it. For publication retries,
// retain PrepareFileBytes output and use PutFilePrepared with unchanged inputs.
func (c *Client) Upload(ctx context.Context, in UploadInput) (*UploadResult, error) {
	if c == nil {
		return nil, fmt.Errorf("transfers: client is nil")
	}
	if in.Actor == nil {
		return nil, fmt.Errorf("transfers: actor is required")
	}

	if in.CommitID == "" {
		return nil, fmt.Errorf("transfers: commit id is required")
	}
	prepared, err := c.PrepareFileBytes(ctx, in.NamespaceID, in.Content)
	if err != nil {
		return nil, err
	}
	return c.PutFilePrepared(ctx, PreparedUploadInput{
		NamespaceID: in.NamespaceID, Path: in.Path, Prepared: prepared, Actor: in.Actor,
		CommitID: in.CommitID, Message: in.Message, Behavior: in.Behavior,
		ExpectedInodeID: in.ExpectedInodeID, ExpectedRevisionNo: in.ExpectedRevisionNo,
	})
}

// PrepareFileBytes uploads once without publishing; retain the result for retries.
func (c *Client) PrepareFileBytes(ctx context.Context, namespaceID loonfs.NamespaceID, content []byte) (*PreparedFileContent, error) {
	if c == nil {
		return nil, fmt.Errorf("transfers: client is nil")
	}
	capabilitiesClient := capabilities.NewClient(c.options)
	uploadsClient := uploads.NewClient(c.options)
	capabilities, err := capabilitiesClient.Retrieve(ctx)
	if err != nil {
		return nil, fmt.Errorf("transfers: read capabilities: %w", err)
	}
	createRequest, err := createUploadRequest(capabilities, namespaceID, int64(len(content)))
	if err != nil {
		return nil, err
	}
	begin, err := uploadsClient.Create(ctx, createRequest)
	if err != nil {
		return nil, fmt.Errorf("transfers: begin upload: %w", err)
	}

	completed, err := transferAndComplete(ctx, uploadsClient, namespaceID, content, begin)
	if err != nil {
		return nil, err
	}
	status, err := completedUploadStatus(completed)
	if err != nil {
		return nil, err
	}
	return &PreparedFileContent{ContentRef: status.ContentRef, ContentToken: status.ContentToken}, nil
}

// PutFilePrepared publishes retained content without starting another upload.
// Reuse unchanged inputs to retry the same commit.
func (c *Client) PutFilePrepared(ctx context.Context, in PreparedUploadInput) (*UploadResult, error) {
	if c == nil {
		return nil, fmt.Errorf("transfers: client is nil")
	}
	if in.Actor == nil {
		return nil, fmt.Errorf("transfers: actor is required")
	}
	if in.CommitID == "" {
		return nil, fmt.Errorf("transfers: commit id is required")
	}
	if in.Prepared == nil || in.Prepared.ContentRef == nil {
		return nil, fmt.Errorf("transfers: prepared content is required")
	}
	commitsClient := commits.NewClient(c.options)
	behavior := in.Behavior
	if behavior == "" {
		behavior = loonfs.DestinationBehaviorNoReplace
	}
	contentTokens := []*loonfs.ContentToken(nil)
	if in.Prepared.ContentToken != nil {
		contentTokens = []*loonfs.ContentToken{in.Prepared.ContentToken}
	}
	committed, err := commitsClient.Create(ctx, &loonfs.CommitRequest{
		NamespaceID:   string(in.NamespaceID),
		Actor:         in.Actor,
		CommitID:      in.CommitID,
		ContentTokens: contentTokens,
		Message:       in.Message,
		Operations: []*loonfs.FilesystemOperation{
			{
				PutFile: &loonfs.FilesystemOperationPutFile{
					Behavior:           &behavior,
					ContentRef:         in.Prepared.ContentRef,
					ExpectedInodeID:    in.ExpectedInodeID,
					ExpectedRevisionNo: in.ExpectedRevisionNo,
					Path:               in.Path,
				},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("transfers: commit file: %w", err)
	}
	return &UploadResult{
		NamespaceID:  committed.NamespaceID,
		CommitID:     committed.CommitID,
		CommittedSeq: committed.CommittedSeq,
	}, nil
}

// Download collects DownloadStream for callers that want a whole byte slice.
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

// fileProjection reads the file half of a path entry union.
func fileProjection(entry *loonfs.PathEntry) (*loonfs.PathEntryFile, error) {
	if entry == nil {
		return nil, fmt.Errorf("transfers: path entry is nil")
	}
	if entry.File == nil {
		return nil, fmt.Errorf("transfers: path is a %s, not a file", entry.InodeKind)
	}
	return entry.File, nil
}

func createUploadRequest(
	capabilities *loonfs.CapabilityDocument,
	namespaceID loonfs.NamespaceID,
	sizeBytes int64,
) (*loonfs.CreateUploadRequest, error) {
	if capabilities == nil {
		return nil, fmt.Errorf("transfers: capability response is nil")
	}
	request := &loonfs.BeginUploadRequest{}
	worthCutting := sizeBytes >= multipartMinimumBytes
	if worthCutting && capabilities.Features[featureDirectMultipart] {
		request.DirectMultipart = &loonfs.BeginUploadDirectMultipart{}
	} else {
		proxyLimit, hasProxyLimit := capabilities.Limits[limitUploadMaximumBytes]
		fitsProxy := !hasProxyLimit || sizeBytes <= proxyLimit
		directPutLimit, hasDirectPutLimit := capabilities.Limits[limitDirectPutMaximumBytes]
		fitsDirectPut := !hasDirectPutLimit || sizeBytes <= directPutLimit
		switch {
		case (worthCutting || !fitsProxy) && capabilities.Features[featureDirectPut] && fitsDirectPut:
			request.DirectPut = &loonfs.BeginUploadDirectPut{
				SizeBytes: &sizeBytes,
			}
		case fitsProxy:
			request.ServiceProxied = &loonfs.BeginUploadServiceProxied{}
		default:
			return nil, fmt.Errorf(
				"transfers: %d-byte upload fits no advertised transport",
				sizeBytes,
			)
		}
	}
	return &loonfs.CreateUploadRequest{
		NamespaceID: string(namespaceID),
		Body:        request,
	}, nil
}

func transferAndComplete(
	ctx context.Context,
	uploadsClient *uploads.Client,
	namespaceID loonfs.NamespaceID,
	payload []byte,
	begin *loonfs.BeginUploadResponse,
) (*loonfs.UploadSession, error) {
	if begin == nil {
		return nil, fmt.Errorf("transfers: begin upload response is nil")
	}
	switch {
	case begin.DirectPut != nil:
		return transferDirectPut(ctx, uploadsClient, namespaceID, payload, begin.DirectPut)
	case begin.DirectMultipart != nil:
		return transferDirectMultipart(ctx, uploadsClient, namespaceID, payload, begin.DirectMultipart)
	case begin.ServiceProxied != nil:
		return transferServiceProxied(ctx, uploadsClient, namespaceID, payload, begin.ServiceProxied)
	default:
		return nil, fmt.Errorf("transfers: unsupported begin upload mode %q", begin.Mode)
	}
}

func transferDirectPut(
	ctx context.Context,
	uploadsClient *uploads.Client,
	namespaceID loonfs.NamespaceID,
	payload []byte,
	begin *loonfs.BeginUploadResponseDirectPut,
) (*loonfs.UploadSession, error) {
	checksum, err := computeChecksum(begin.ChecksumAlgorithm, payload)
	if err != nil {
		abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: direct PUT: %w", err)
	}
	if _, err := putPresigned(ctx, begin.Access, payload); err != nil {
		abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: direct PUT: %w", err)
	}
	content := &loonfs.UploadContentClaim{
		Checksum:  checksum,
		SizeBytes: int64(len(payload)),
	}
	completed, err := uploadsClient.Complete(ctx, &loonfs.CompleteUploadRequest{
		NamespaceID: string(namespaceID),
		UploadID:    string(begin.UploadID),
		Body: &loonfs.UploadCompletion{
			DirectPut: &loonfs.CompleteUploadDirectPut{Content: content},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("transfers: complete direct PUT: %w", err)
	}
	return completed, nil
}

func transferDirectMultipart(
	ctx context.Context,
	uploadsClient *uploads.Client,
	namespaceID loonfs.NamespaceID,
	payload []byte,
	begin *loonfs.BeginUploadResponseDirectMultipart,
) (*loonfs.UploadSession, error) {
	partSizeBytes := begin.PartSizeBytes
	partSize := int(partSizeBytes)
	if partSizeBytes <= 0 || int64(partSize) != partSizeBytes {
		return nil, fmt.Errorf("transfers: invalid multipart part size %d", partSizeBytes)
	}
	parts := splitParts(payload, partSize)
	if len(parts) == 0 {
		abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: multipart response selected for an empty payload")
	}
	claims := make([]*loonfs.UploadPartChecksumClaim, len(parts))
	for index, part := range parts {
		checksum, checksumErr := computeChecksum(begin.ChecksumAlgorithm, part)
		if checksumErr != nil {
			abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
			return nil, fmt.Errorf("transfers: checksum part %d: %w", index+1, checksumErr)
		}
		claims[index] = &loonfs.UploadPartChecksumClaim{
			Checksum:   checksum,
			PartNumber: index + 1,
		}
	}
	signed, err := uploadsClient.SignParts(ctx, &loonfs.SignUploadPartsRequest{
		NamespaceID: string(namespaceID),
		UploadID:    string(begin.UploadID),
		Parts:       claims,
	})
	if err != nil {
		abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: sign multipart parts: %w", err)
	}
	if signed == nil {
		abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: sign multipart parts returned no response")
	}
	accessByPart := make(map[int]*loonfs.SignedUploadPart, len(signed.Parts))
	for _, signedPart := range signed.Parts {
		accessByPart[signedPart.PartNumber] = signedPart
	}

	completedParts := make([]*loonfs.CompletedUploadPart, 0, len(parts))
	for index, part := range parts {
		partNumber := index + 1
		signedPart := accessByPart[partNumber]
		if signedPart == nil {
			abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
			return nil, fmt.Errorf("transfers: server did not sign multipart part %d", partNumber)
		}
		etag, putErr := putPresigned(ctx, signedPart.Access, part)
		if putErr != nil {
			abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
			return nil, fmt.Errorf("transfers: upload part %d: %w", partNumber, putErr)
		}
		if etag == "" {
			abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
			return nil, fmt.Errorf("transfers: upload part %d: presigned PUT response has no ETag", partNumber)
		}
		completedParts = append(completedParts, &loonfs.CompletedUploadPart{
			Checksum:   claims[index].Checksum,
			Etag:       etag,
			PartNumber: partNumber,
		})
	}
	wholeChecksum, err := computeChecksum(begin.ChecksumAlgorithm, payload)
	if err != nil {
		abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: checksum multipart payload: %w", err)
	}
	completed, err := uploadsClient.Complete(ctx, &loonfs.CompleteUploadRequest{
		NamespaceID: string(namespaceID),
		UploadID:    string(begin.UploadID),
		Body: &loonfs.UploadCompletion{
			DirectMultipart: &loonfs.CompleteUploadDirectMultipart{
				Content: &loonfs.UploadContentClaim{
					Checksum:  wholeChecksum,
					SizeBytes: int64(len(payload)),
				},
				Parts: completedParts,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("transfers: complete multipart upload: %w", err)
	}
	return completed, nil
}

func transferServiceProxied(
	ctx context.Context,
	uploadsClient *uploads.Client,
	namespaceID loonfs.NamespaceID,
	payload []byte,
	begin *loonfs.BeginUploadResponseServiceProxied,
) (*loonfs.UploadSession, error) {
	if _, err := uploadsClient.PutContent(
		ctx,
		string(namespaceID),
		string(begin.UploadID),
		bytes.NewReader(payload),
		option.WithHTTPHeader(http.Header{"Content-Type": []string{"application/octet-stream"}}),
	); err != nil {
		abortUpload(ctx, uploadsClient, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: upload proxied content: %w", err)
	}
	completed, err := uploadsClient.Complete(ctx, &loonfs.CompleteUploadRequest{
		NamespaceID: string(namespaceID),
		UploadID:    string(begin.UploadID),
		Body: &loonfs.UploadCompletion{
			ServiceProxied: &loonfs.CompleteUploadServiceProxied{},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("transfers: complete proxied upload: %w", err)
	}
	return completed, nil
}

func abortUpload(ctx context.Context, uploadsClient *uploads.Client, namespaceID loonfs.NamespaceID, uploadID loonfs.UploadID) {
	_, _ = uploadsClient.Abort(ctx, &loonfs.AbortUploadRequest{
		NamespaceID: string(namespaceID),
		UploadID:    string(uploadID),
	})
}

func completedUploadStatus(response *loonfs.UploadSession) (*loonfs.UploadSessionStatusCompleted, error) {
	if response == nil {
		return nil, fmt.Errorf("transfers: upload session response is nil")
	}
	if response.Completed == nil || response.Completed.ContentRef == nil {
		return nil, fmt.Errorf("transfers: upload is %s, not completed", response.Status)
	}
	return response.Completed, nil
}

func splitParts(payload []byte, partSize int) [][]byte {
	parts := make([][]byte, 0, (len(payload)+partSize-1)/partSize)
	for offset := 0; offset < len(payload); offset += partSize {
		end := offset + partSize
		if end > len(payload) {
			end = len(payload)
		}
		parts = append(parts, payload[offset:end])
	}
	return parts
}

func putPresigned(
	ctx context.Context,
	access *loonfs.ObjectTransferAccess,
	payload []byte,
) (string, error) {
	response, err := sendPresigned(ctx, access, http.MethodPut, bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", responseStatusError(response)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	return response.Header.Get("ETag"), nil
}

func sendPresigned(
	ctx context.Context,
	access *loonfs.ObjectTransferAccess,
	expectedMethod string,
	body io.Reader,
	contentLength int64,
) (*http.Response, error) {
	return sendPresignedWithClient(ctx, presignedHTTPClient, access, expectedMethod, body, contentLength)
}

func sendPresignedWithClient(ctx context.Context, client core.HTTPClient, access *loonfs.ObjectTransferAccess, expectedMethod string, body io.Reader, contentLength int64) (*http.Response, error) {
	if access == nil || access.PresignedURL == nil {
		return nil, fmt.Errorf("unsupported object transfer access")
	}
	presigned := access.PresignedURL
	if presigned.Method != expectedMethod {
		return nil, fmt.Errorf("presigned request uses method %q, expected %q", presigned.Method, expectedMethod)
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
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4*1024))
	detail := strings.TrimSpace(string(body))
	if detail == "" {
		return fmt.Errorf("presigned request returned %s", response.Status)
	}
	return fmt.Errorf("presigned request returned %s: %s", response.Status, detail)
}

func computeChecksum(algorithm loonfs.ChecksumAlgorithm, payload []byte) (*loonfs.Checksum, error) {
	var value string
	switch algorithm {
	case loonfs.ChecksumAlgorithmSha256:
		digest := sha256.Sum256(payload)
		value = hex.EncodeToString(digest[:])
	case loonfs.ChecksumAlgorithmCrc64Nvme:
		value = fmt.Sprintf("%016x", crc64.Checksum(payload, crc64NVMeTable))
	case loonfs.ChecksumAlgorithmCrc32C:
		value = fmt.Sprintf("%08x", crc32.Checksum(payload, crc32CTable))
	default:
		return nil, fmt.Errorf("unsupported checksum algorithm %q", algorithm)
	}
	return &loonfs.Checksum{
		Algorithm: algorithm,
		Value:     value,
	}, nil
}
