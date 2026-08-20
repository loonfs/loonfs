// Package transfers provides in-memory file transfer orchestration for the Go SDK.
package transfers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"sort"
	"strings"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/client"
	"github.com/loonfs/loonfs-sdk-go/option"
)

const (
	multipartMinimumBytes = 8 * 1024 * 1024
	multipartMaximumParts = 10_000

	featureDirectPut           = "core.uploads.direct_put"
	featureDirectMultipart     = "core.uploads.direct_multipart"
	limitUploadMaximumBytes    = "upload.max_content_bytes"
	limitDirectPutMaximumBytes = "upload.direct_put_max_content_bytes"

	crc64NVMePolynomial = 0x9a6c9329ac4bc9b5
)

var (
	crc32CTable         = crc32.MakeTable(crc32.Castagnoli)
	crc64NVMeTable      = crc64.MakeTable(crc64NVMePolynomial)
	presignedHTTPClient = newPresignedHTTPClient()
)

// PutFileInput describes one in-memory file write.
type PutFileInput struct {
	NamespaceID        loonfs.NamespaceID
	Path               loonfs.AbsolutePath
	Bytes              []byte
	Actor              *loonfs.ActorRef
	CommitID           loonfs.CommitID
	Message            *string
	Behavior           loonfs.DestinationBehavior
	ExpectedRevisionNo *loonfs.RevisionNo
}

// PutFileResult identifies the commit that made the new revision visible.
type PutFileResult struct {
	NamespaceID  loonfs.NamespaceID
	CommitID     loonfs.CommitID
	CommittedSeq loonfs.ChangeSeq
}

// GetFileInput describes one current or retained file revision to read.
type GetFileInput struct {
	NamespaceID loonfs.NamespaceID
	Path        loonfs.AbsolutePath
	RevisionNo  *loonfs.RevisionNo
}

// GetFileResult contains file bytes and the resolved revision facts.
type GetFileResult struct {
	Bytes       []byte
	NamespaceID loonfs.NamespaceID
	Path        loonfs.AbsolutePath
	RevisionNo  loonfs.RevisionNo
	ContentRef  *loonfs.ContentRef
}

// PutFile uploads bytes, completes the upload, and commits the content to a path.
// Streaming and resume are follow-ups.
func PutFile(ctx context.Context, c *client.Client, in PutFileInput) (*PutFileResult, error) {
	if c == nil {
		return nil, fmt.Errorf("transfers: client is nil")
	}
	if in.Actor == nil {
		return nil, fmt.Errorf("transfers: actor is required")
	}

	capabilities, err := c.Capabilities(ctx)
	if err != nil {
		return nil, fmt.Errorf("transfers: read capabilities: %w", err)
	}
	transport, err := chooseUploadTransport(capabilities, int64(len(in.Bytes)))
	if err != nil {
		return nil, err
	}

	beginRequest, err := beginUploadRequest(in.NamespaceID, int64(len(in.Bytes)), transport)
	if err != nil {
		return nil, err
	}
	begin, err := c.Uploads.BeginUpload(ctx, beginRequest)
	if err != nil {
		return nil, fmt.Errorf("transfers: begin upload: %w", err)
	}

	completed, err := transferAndComplete(ctx, c, in.NamespaceID, in.Bytes, begin)
	if err != nil {
		return nil, err
	}
	status, err := completedUploadStatus(completed)
	if err != nil {
		return nil, err
	}

	if in.CommitID == "" {
		return nil, fmt.Errorf("transfers: commit id is required; a caller that keeps its commit id can replay a lost response safely")
	}
	behavior := in.Behavior
	if behavior == "" {
		behavior = loonfs.DestinationBehaviorNoReplace
	}
	contentTokens := []*loonfs.ContentToken(nil)
	if status.ContentToken != nil {
		contentTokens = []*loonfs.ContentToken{status.ContentToken}
	}
	committed, err := c.Filesystem.ApplyCommit(ctx, &loonfs.CommitRequest{
		NamespaceID:   string(in.NamespaceID),
		Actor:         in.Actor,
		CommitID:      in.CommitID,
		ContentTokens: contentTokens,
		Message:       in.Message,
		Operations: []*loonfs.FilesystemOperation{
			{
				PutFile: &loonfs.FsOpPutFile{
					Behavior:           &behavior,
					ContentRef:         status.ContentRef,
					ExpectedRevisionNo: in.ExpectedRevisionNo,
					Path:               in.Path,
				},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("transfers: commit file: %w", err)
	}
	return &PutFileResult{
		NamespaceID:  committed.NamespaceID,
		CommitID:     committed.CommitID,
		CommittedSeq: committed.CommittedSeq,
	}, nil
}

// GetFile downloads one file revision into memory and verifies its content reference.
// Streaming and resume are follow-ups.
func GetFile(ctx context.Context, c *client.Client, in GetFileInput) (*GetFileResult, error) {
	if c == nil {
		return nil, fmt.Errorf("transfers: client is nil")
	}
	grant, err := c.Filesystem.BeginDownload(ctx, &loonfs.BeginDownloadRequest{
		NamespaceID: string(in.NamespaceID),
		Path:        in.Path,
		RevisionNo:  in.RevisionNo,
	})
	if err != nil {
		return nil, fmt.Errorf("transfers: begin download: %w", err)
	}
	if grant.ContentRef == nil {
		return nil, fmt.Errorf("transfers: download grant has no content reference")
	}

	payload, err := getPresigned(ctx, grant.Access)
	if err != nil {
		return nil, fmt.Errorf("transfers: download content: %w", err)
	}
	if int64(len(payload)) != grant.ContentRef.SizeBytes {
		return nil, fmt.Errorf(
			"transfers: downloaded %d bytes, expected %d",
			len(payload),
			grant.ContentRef.SizeBytes,
		)
	}
	if err := verifyChecksum(grant.ContentRef.Checksum, payload); err != nil {
		return nil, fmt.Errorf("transfers: verify download: %w", err)
	}

	return &GetFileResult{
		Bytes:       payload,
		NamespaceID: grant.NamespaceID,
		Path:        grant.Path,
		RevisionNo:  grant.RevisionNo,
		ContentRef:  grant.ContentRef,
	}, nil
}

type uploadTransport int

const (
	uploadServiceProxied uploadTransport = iota
	uploadDirectPut
	uploadDirectMultipart
)

func chooseUploadTransport(
	capabilities *loonfs.CapabilityDocument,
	sizeBytes int64,
) (uploadTransport, error) {
	if capabilities == nil {
		return 0, fmt.Errorf("transfers: capability response is nil")
	}
	worthCutting := sizeBytes >= multipartMinimumBytes
	if worthCutting && capabilities.Features[featureDirectMultipart] {
		return uploadDirectMultipart, nil
	}

	proxyLimit, hasProxyLimit := capabilities.Limits[limitUploadMaximumBytes]
	fitsProxy := !hasProxyLimit || sizeBytes <= proxyLimit
	directPutLimit, hasDirectPutLimit := capabilities.Limits[limitDirectPutMaximumBytes]
	fitsDirectPut := !hasDirectPutLimit || sizeBytes <= directPutLimit
	if (worthCutting || !fitsProxy) && capabilities.Features[featureDirectPut] && fitsDirectPut {
		return uploadDirectPut, nil
	}
	if fitsProxy {
		return uploadServiceProxied, nil
	}
	return 0, fmt.Errorf(
		"transfers: %d-byte upload fits no advertised transport",
		sizeBytes,
	)
}

func beginUploadRequest(
	namespaceID loonfs.NamespaceID,
	sizeBytes int64,
	transport uploadTransport,
) (*loonfs.BeginUploadBody, error) {
	request := &loonfs.BeginUploadRequest{}
	switch transport {
	case uploadServiceProxied:
		request.ServiceProxied = &loonfs.BeginUploadServiceProxied{}
	case uploadDirectPut:
		request.DirectPut = &loonfs.BeginUploadDirectPut{
			SizeBytes: &sizeBytes,
		}
	case uploadDirectMultipart:
		request.DirectMultipart = &loonfs.BeginUploadDirectMultipart{
			Multipart: &loonfs.DirectMultipartUploadOptions{},
		}
	default:
		return nil, fmt.Errorf("transfers: unsupported upload transport")
	}
	return &loonfs.BeginUploadBody{
		NamespaceID: string(namespaceID),
		Body:        request,
	}, nil
}

func transferAndComplete(
	ctx context.Context,
	c *client.Client,
	namespaceID loonfs.NamespaceID,
	payload []byte,
	begin *loonfs.BeginUploadResponse,
) (*loonfs.UploadSessionResponse, error) {
	if begin == nil {
		return nil, fmt.Errorf("transfers: begin upload response is nil")
	}
	switch {
	case begin.DirectPut != nil:
		return transferDirectPut(ctx, c, namespaceID, payload, begin.DirectPut)
	case begin.DirectMultipart != nil:
		return transferDirectMultipart(ctx, c, namespaceID, payload, begin.DirectMultipart)
	case begin.ServiceProxied != nil:
		return transferServiceProxied(ctx, c, namespaceID, payload, begin.ServiceProxied)
	default:
		return nil, fmt.Errorf("transfers: unsupported begin upload mode %q", begin.Mode)
	}
}

func transferDirectPut(
	ctx context.Context,
	c *client.Client,
	namespaceID loonfs.NamespaceID,
	payload []byte,
	begin *loonfs.BeginUploadResponseDirectPut,
) (*loonfs.UploadSessionResponse, error) {
	if begin.DirectPut == nil {
		return nil, fmt.Errorf("transfers: direct PUT response is incomplete")
	}
	content, err := putPresignedWithChecksum(
		ctx,
		begin.DirectPut.Access,
		payload,
		begin.DirectPut.ChecksumAlgorithm,
	)
	if err != nil {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: direct PUT: %w", err)
	}
	completed, err := c.Uploads.CompleteUpload(ctx, &loonfs.CompleteUploadBody{
		NamespaceID: string(namespaceID),
		UploadID:    string(begin.UploadID),
		Body: &loonfs.CompleteUploadRequest{
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
	c *client.Client,
	namespaceID loonfs.NamespaceID,
	payload []byte,
	begin *loonfs.BeginUploadResponseDirectMultipart,
) (*loonfs.UploadSessionResponse, error) {
	if begin.DirectMultipart == nil {
		return nil, fmt.Errorf("transfers: multipart response is incomplete")
	}
	partSize, err := checkedPartSize(begin.DirectMultipart.PartSizeBytes)
	if err != nil {
		return nil, err
	}
	parts := splitParts(payload, partSize)
	if len(parts) == 0 {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: multipart response selected for an empty payload")
	}
	if len(parts) > multipartMaximumParts {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: multipart upload requires %d parts, maximum is %d", len(parts), multipartMaximumParts)
	}

	claims := make([]*loonfs.UploadPartChecksumClaim, len(parts))
	for index, part := range parts {
		checksum, checksumErr := computeChecksum(begin.DirectMultipart.ChecksumAlgorithm, part)
		if checksumErr != nil {
			abortUpload(ctx, c, namespaceID, begin.UploadID)
			return nil, fmt.Errorf("transfers: checksum part %d: %w", index+1, checksumErr)
		}
		claims[index] = &loonfs.UploadPartChecksumClaim{
			Checksum:   checksum,
			PartNumber: index + 1,
		}
	}
	signed, err := c.Uploads.SignUploadParts(ctx, &loonfs.SignUploadPartsRequest{
		NamespaceID: string(namespaceID),
		UploadID:    string(begin.UploadID),
		Parts:       claims,
	})
	if err != nil {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: sign multipart parts: %w", err)
	}
	if signed == nil {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: sign multipart parts returned no response")
	}
	if len(signed.Parts) != len(parts) {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: server signed %d parts, expected %d", len(signed.Parts), len(parts))
	}

	completedParts := make([]*loonfs.CompletedUploadPart, 0, len(parts))
	for _, signedPart := range signed.Parts {
		partNumber := signedPart.PartNumber
		if partNumber < 1 || partNumber > len(parts) {
			abortUpload(ctx, c, namespaceID, begin.UploadID)
			return nil, fmt.Errorf("transfers: server signed invalid part number %d", partNumber)
		}
		etag, putErr := putPresigned(ctx, signedPart.Access, parts[partNumber-1], true)
		if putErr != nil {
			abortUpload(ctx, c, namespaceID, begin.UploadID)
			return nil, fmt.Errorf("transfers: upload part %d: %w", partNumber, putErr)
		}
		completedParts = append(completedParts, &loonfs.CompletedUploadPart{
			Checksum:   claims[partNumber-1].Checksum,
			Etag:       etag,
			PartNumber: partNumber,
		})
	}
	sort.Slice(completedParts, func(left, right int) bool {
		return completedParts[left].PartNumber < completedParts[right].PartNumber
	})
	wholeChecksum, err := computeChecksum(begin.DirectMultipart.ChecksumAlgorithm, payload)
	if err != nil {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: checksum multipart payload: %w", err)
	}
	completed, err := c.Uploads.CompleteUpload(ctx, &loonfs.CompleteUploadBody{
		NamespaceID: string(namespaceID),
		UploadID:    string(begin.UploadID),
		Body: &loonfs.CompleteUploadRequest{
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
	c *client.Client,
	namespaceID loonfs.NamespaceID,
	payload []byte,
	begin *loonfs.BeginUploadResponseServiceProxied,
) (*loonfs.UploadSessionResponse, error) {
	if _, err := c.Uploads.UploadContent(
		ctx,
		string(namespaceID),
		string(begin.UploadID),
		bytes.NewReader(payload),
		option.WithHTTPHeader(http.Header{"Content-Type": []string{"application/octet-stream"}}),
	); err != nil {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: upload proxied content: %w", err)
	}
	completed, err := c.Uploads.CompleteUpload(ctx, &loonfs.CompleteUploadBody{
		NamespaceID: string(namespaceID),
		UploadID:    string(begin.UploadID),
		Body: &loonfs.CompleteUploadRequest{
			ServiceProxied: &loonfs.CompleteUploadServiceProxied{},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("transfers: complete proxied upload: %w", err)
	}
	return completed, nil
}

func abortUpload(ctx context.Context, c *client.Client, namespaceID loonfs.NamespaceID, uploadID loonfs.UploadID) {
	_, _ = c.Uploads.AbortUpload(ctx, &loonfs.AbortUploadRequest{
		NamespaceID: string(namespaceID),
		UploadID:    string(uploadID),
	})
}

func completedUploadStatus(response *loonfs.UploadSessionResponse) (*loonfs.UploadSessionStatusCompleted, error) {
	status, err := uploadSessionStatus(response)
	if err != nil {
		return nil, err
	}
	if status.Completed == nil || status.Completed.ContentRef == nil {
		return nil, fmt.Errorf("transfers: upload did not complete")
	}
	return status.Completed, nil
}

func uploadSessionStatus(response *loonfs.UploadSessionResponse) (*loonfs.UploadSessionStatus, error) {
	if response == nil {
		return nil, fmt.Errorf("transfers: upload session response is nil")
	}
	data, err := json.Marshal(response.GetExtraProperties())
	if err != nil {
		return nil, fmt.Errorf("transfers: encode upload session response: %w", err)
	}
	var status loonfs.UploadSessionStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, fmt.Errorf("transfers: decode upload status: %w", err)
	}
	return &status, nil
}

func checkedPartSize(sizeBytes int64) (int, error) {
	maximumInt := int(^uint(0) >> 1)
	if sizeBytes <= 0 || uint64(sizeBytes) > uint64(maximumInt) {
		return 0, fmt.Errorf("transfers: invalid multipart part size %d", sizeBytes)
	}
	return int(sizeBytes), nil
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
	requireETag bool,
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
	etag := response.Header.Get("ETag")
	if requireETag && etag == "" {
		return "", fmt.Errorf("presigned PUT response has no ETag")
	}
	return etag, nil
}

type checksumReader struct {
	reader    io.Reader
	hasher    hash.Hash
	sizeBytes int64
}

func (r *checksumReader) Read(buffer []byte) (int, error) {
	read, err := r.reader.Read(buffer)
	if read > 0 {
		_, _ = r.hasher.Write(buffer[:read])
		r.sizeBytes += int64(read)
	}
	return read, err
}

func putPresignedWithChecksum(
	ctx context.Context,
	access *loonfs.ObjectTransferAccess,
	payload []byte,
	algorithm loonfs.ChecksumAlgorithm,
) (*loonfs.UploadContentClaim, error) {
	hasher, err := checksumHasher(algorithm)
	if err != nil {
		return nil, err
	}
	body := &checksumReader{reader: bytes.NewReader(payload), hasher: hasher}
	response, err := sendPresigned(ctx, access, http.MethodPut, body, int64(len(payload)))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, responseStatusError(response)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	return &loonfs.UploadContentClaim{
		Checksum: &loonfs.Checksum{
			Algorithm: algorithm,
			Value:     hex.EncodeToString(hasher.Sum(nil)),
		},
		SizeBytes: body.sizeBytes,
	}, nil
}

func getPresigned(ctx context.Context, access *loonfs.ObjectTransferAccess) ([]byte, error) {
	response, err := sendPresigned(ctx, access, http.MethodGet, nil, 0)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, responseStatusError(response)
	}
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read presigned GET response: %w", err)
	}
	return payload, nil
}

func sendPresigned(
	ctx context.Context,
	access *loonfs.ObjectTransferAccess,
	expectedMethod string,
	body io.Reader,
	contentLength int64,
) (*http.Response, error) {
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
	response, err := presignedHTTPClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send presigned request: %w", err)
	}
	return response, nil
}

func newPresignedHTTPClient() *http.Client {
	return &http.Client{
		Transport: http.DefaultTransport.(*http.Transport).Clone(),
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func responseStatusError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4*1024))
	detail := strings.TrimSpace(string(body))
	if detail == "" {
		return fmt.Errorf("presigned request returned %s", response.Status)
	}
	return fmt.Errorf("presigned request returned %s: %s", response.Status, detail)
}

func verifyChecksum(expected *loonfs.Checksum, payload []byte) error {
	if expected == nil {
		return fmt.Errorf("content reference has no checksum")
	}
	actual, err := computeChecksum(expected.Algorithm, payload)
	if err != nil {
		return err
	}
	if actual.Value != expected.Value {
		return fmt.Errorf("checksum mismatch: got %s, expected %s", actual.Value, expected.Value)
	}
	return nil
}

func computeChecksum(algorithm loonfs.ChecksumAlgorithm, payload []byte) (*loonfs.Checksum, error) {
	hasher, err := checksumHasher(algorithm)
	if err != nil {
		return nil, err
	}
	_, _ = hasher.Write(payload)
	return &loonfs.Checksum{
		Algorithm: algorithm,
		Value:     hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func checksumHasher(algorithm loonfs.ChecksumAlgorithm) (hash.Hash, error) {
	switch algorithm {
	case loonfs.ChecksumAlgorithmSha256:
		return sha256.New(), nil
	case loonfs.ChecksumAlgorithmCrc64Nvme:
		return crc64.New(crc64NVMeTable), nil
	case loonfs.ChecksumAlgorithmCrc32C:
		return crc32.New(crc32CTable), nil
	default:
		return nil, fmt.Errorf("unsupported checksum algorithm %q", algorithm)
	}
}
