// Package transfers provides in-memory file transfer orchestration for the Go SDK.
package transfers

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
	"github.com/loonfs/loonfs-sdk-go/client"
	"github.com/loonfs/loonfs-sdk-go/option"
)

const (
	multipartMinimumBytes = 8 * 1024 * 1024

	featureDirectGet           = "core.downloads.direct_get"
	featureDirectPut           = "core.uploads.direct_put"
	featureDirectMultipart     = "core.uploads.direct_multipart"
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

	capabilities, err := c.System.GetCapabilities(ctx)
	if err != nil {
		return nil, fmt.Errorf("transfers: read capabilities: %w", err)
	}
	createRequest, err := createUploadRequest(capabilities, in.NamespaceID, int64(len(in.Bytes)))
	if err != nil {
		return nil, err
	}
	begin, err := c.Uploads.CreateUpload(ctx, createRequest)
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
	committed, err := c.Filesystem.CreateCommit(ctx, &loonfs.CommitRequest{
		NamespaceID:   string(in.NamespaceID),
		Actor:         in.Actor,
		CommitID:      in.CommitID,
		ContentTokens: contentTokens,
		Message:       in.Message,
		Operations: []*loonfs.FilesystemOperation{
			{
				PutFile: &loonfs.FilesystemOperationPutFile{
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
	capabilities, err := c.System.GetCapabilities(ctx)
	if err != nil {
		return nil, fmt.Errorf("transfers: read capabilities: %w", err)
	}
	if capabilities == nil || !capabilities.Features[featureDirectGet] {
		return getFileProxied(ctx, c, in)
	}
	grant, err := c.Filesystem.CreateDownload(ctx, &loonfs.BeginDownloadRequest{
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

// getFileProxied reads through LoonFS when direct reads are unavailable.
// It loads the content reference first, then requests the exact revision so
// the reference and returned bytes describe the same file version.
func getFileProxied(ctx context.Context, c *client.Client, in GetFileInput) (*GetFileResult, error) {
	revisionNo := in.RevisionNo
	var claim *loonfs.ContentRef
	if revisionNo == nil {
		entry, err := c.Filesystem.GetPathEntry(ctx, &loonfs.GetPathEntryRequest{
			NamespaceID: string(in.NamespaceID),
			Path:        string(in.Path),
		})
		if err != nil {
			return nil, fmt.Errorf("transfers: stat path: %w", err)
		}
		file, err := fileProjection(entry)
		if err != nil {
			return nil, err
		}
		headRevision := file.RevisionNo
		revisionNo = &headRevision
		claim = file.ContentRef
	} else {
		page, err := c.Filesystem.ListFileRevisions(ctx, &loonfs.ListFileRevisionsRequest{
			NamespaceID: string(in.NamespaceID),
			Path:        string(in.Path),
		})
		if err != nil {
			return nil, fmt.Errorf("transfers: list file revisions: %w", err)
		}
		iterator := page.Iterator()
		for iterator.Next(ctx) {
			revision := iterator.Current()
			if revision.RevisionNo == *revisionNo {
				claim = revision.ContentRef
				break
			}
		}
		if claim == nil {
			return nil, fmt.Errorf("transfers: revision %d not found for %s", *revisionNo, in.Path)
		}
	}
	if claim == nil {
		return nil, fmt.Errorf("transfers: revision has no content reference")
	}

	reader, err := c.Filesystem.GetFileBytes(ctx, &loonfs.GetFileBytesRequest{
		NamespaceID: string(in.NamespaceID),
		Path:        string(in.Path),
		RevisionNo:  revisionNo,
	})
	if err != nil {
		return nil, fmt.Errorf("transfers: read content: %w", err)
	}
	payload, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("transfers: read content: %w", err)
	}
	if int64(len(payload)) != claim.SizeBytes {
		return nil, fmt.Errorf(
			"transfers: proxied read returned %d bytes, expected %d",
			len(payload),
			claim.SizeBytes,
		)
	}
	if err := verifyChecksum(claim.Checksum, payload); err != nil {
		return nil, fmt.Errorf("transfers: verify proxied read: %w", err)
	}

	return &GetFileResult{
		Bytes:       payload,
		NamespaceID: in.NamespaceID,
		Path:        in.Path,
		RevisionNo:  *revisionNo,
		ContentRef:  claim,
	}, nil
}

// fileProjection reads the file half of a path entry union.
func fileProjection(entry *loonfs.AuthoritativePathEntry) (*loonfs.AuthoritativePathEntryFile, error) {
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
	checksum, err := computeChecksum(begin.ChecksumAlgorithm, payload)
	if err != nil {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: direct PUT: %w", err)
	}
	if _, err := putPresigned(ctx, begin.Access, payload); err != nil {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: direct PUT: %w", err)
	}
	content := &loonfs.UploadContentClaim{
		Checksum:  checksum,
		SizeBytes: int64(len(payload)),
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
	partSizeBytes := begin.PartSizeBytes
	partSize := int(partSizeBytes)
	if partSizeBytes <= 0 || int64(partSize) != partSizeBytes {
		return nil, fmt.Errorf("transfers: invalid multipart part size %d", partSizeBytes)
	}
	parts := splitParts(payload, partSize)
	if len(parts) == 0 {
		abortUpload(ctx, c, namespaceID, begin.UploadID)
		return nil, fmt.Errorf("transfers: multipart response selected for an empty payload")
	}
	claims := make([]*loonfs.UploadPartChecksumClaim, len(parts))
	for index, part := range parts {
		checksum, checksumErr := computeChecksum(begin.ChecksumAlgorithm, part)
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
	accessByPart := make(map[int]*loonfs.SignedUploadPart, len(signed.Parts))
	for _, signedPart := range signed.Parts {
		accessByPart[signedPart.PartNumber] = signedPart
	}

	completedParts := make([]*loonfs.CompletedUploadPart, 0, len(parts))
	for index, part := range parts {
		partNumber := index + 1
		signedPart := accessByPart[partNumber]
		if signedPart == nil {
			abortUpload(ctx, c, namespaceID, begin.UploadID)
			return nil, fmt.Errorf("transfers: server did not sign multipart part %d", partNumber)
		}
		etag, putErr := putPresigned(ctx, signedPart.Access, part)
		if putErr != nil {
			abortUpload(ctx, c, namespaceID, begin.UploadID)
			return nil, fmt.Errorf("transfers: upload part %d: %w", partNumber, putErr)
		}
		if etag == "" {
			abortUpload(ctx, c, namespaceID, begin.UploadID)
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
	if _, err := c.Uploads.PutUploadContent(
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
