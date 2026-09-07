package files

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"time"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/capabilities"
	"github.com/loonfs/loonfs-sdk-go/option"
	"github.com/loonfs/loonfs-sdk-go/uploads"
)

// StreamUploadInput consumes Content once. SizeBytes is optional; supply it when
// known to validate the source and select a single PUT for small files.
// The caller owns Content and is responsible for closing it.
type StreamUploadInput struct {
	NamespaceID        loonfs.NamespaceID
	Path               loonfs.AbsolutePath
	Content            io.Reader
	SizeBytes          *int64
	Actor              *loonfs.ActorRef
	CommitID           loonfs.CommitID
	Message            *string
	Behavior           loonfs.DestinationBehavior
	ExpectedInodeID    *string
	ExpectedRevisionNo *loonfs.RevisionNo
}

func (c *Client) UploadStream(ctx context.Context, in StreamUploadInput) (*UploadResult, error) {
	if in.Actor == nil || in.CommitID == "" {
		return nil, fmt.Errorf("transfers: actor and commit id are required")
	}
	ctx, cancel := transferContext(ctx)
	defer cancel()
	prepared, err := c.PrepareFileStream(ctx, in.NamespaceID, in.Content, in.SizeBytes)
	if err != nil {
		return nil, err
	}
	return c.PutFilePrepared(ctx, PreparedUploadInput{
		NamespaceID: in.NamespaceID, Path: in.Path, Prepared: prepared,
		Actor: in.Actor, CommitID: in.CommitID, Message: in.Message, Behavior: in.Behavior,
		ExpectedInodeID: in.ExpectedInodeID, ExpectedRevisionNo: in.ExpectedRevisionNo,
	})
}

// PrepareFileStream stages a source once with bounded memory and no payload
// retries. Retain its result for PutFilePrepared publication retries.
func (c *Client) PrepareFileStream(ctx context.Context, namespaceID loonfs.NamespaceID, source io.Reader, sizeBytes *int64) (*PreparedFileContent, error) {
	if c == nil {
		return nil, fmt.Errorf("transfers: client is nil")
	}
	if source == nil || (sizeBytes != nil && *sizeBytes < 0) {
		return nil, fmt.Errorf("transfers: source and nonnegative size are required")
	}
	ctx, cancel := transferContext(ctx)
	defer cancel()
	capabilities, err := capabilities.NewClient(c.options).Retrieve(ctx)
	if err != nil {
		return nil, err
	}
	if capabilities == nil {
		return nil, fmt.Errorf("transfers: no capabilities")
	}
	if sizeBytes == nil {
		// Distinguish an empty source before selecting multipart. Retain at
		// most one byte, not the whole source or every multipart part.
		var first [1]byte
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var n int
		var err error
		for attempts := 0; attempts < 100 && n == 0 && err == nil; attempts++ {
			if err = ctx.Err(); err == nil {
				n, err = source.Read(first[:])
			}
		}
		if n == 0 && err == nil {
			return nil, io.ErrNoProgress
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			size := int64(0)
			sizeBytes = &size
		} else {
			source = io.MultiReader(bytes.NewReader(first[:n]), source)
		}
	}
	var request *loonfs.CreateUploadRequest
	if sizeBytes != nil {
		request, err = createUploadRequest(capabilities, namespaceID, *sizeBytes)
		if err != nil {
			return nil, err
		}
	} else {
		body := &loonfs.BeginUploadRequest{}
		if capabilities.Features[featureDirectMultipart] {
			body.DirectMultipart = &loonfs.BeginUploadDirectMultipart{}
		} else {
			body.ServiceProxied = &loonfs.BeginUploadServiceProxied{}
		}
		request = &loonfs.CreateUploadRequest{NamespaceID: string(namespaceID), Body: body}
	}
	uploadsClient := uploads.NewClient(c.options)
	begin, err := uploadsClient.Create(ctx, request, option.WithMaxAttempts(1))
	if err != nil {
		return nil, err
	}
	if begin == nil {
		return nil, fmt.Errorf("transfers: no upload session")
	}
	reader := &uploadReader{ctx: ctx, source: source, expected: sizeBytes}
	var uploadID loonfs.UploadID
	var completion *loonfs.UploadCompletion
	switch {
	case begin.ServiceProxied != nil:
		uploadID = begin.ServiceProxied.UploadID
		if limit, ok := capabilities.Limits[limitUploadMaximumBytes]; ok {
			reader.limit = &limit
		}
		_, err = uploadsClient.PutContent(ctx, string(namespaceID), string(uploadID), reader,
			option.WithHTTPHeader(http.Header{"Content-Type": {"application/octet-stream"}}), option.WithMaxAttempts(1))
		completion = &loonfs.UploadCompletion{ServiceProxied: &loonfs.CompleteUploadServiceProxied{}}
	case begin.DirectPut != nil:
		uploadID = begin.DirectPut.UploadID
		reader.digest, err = newChecksum(begin.DirectPut.ChecksumAlgorithm)
		if err == nil {
			length := int64(-1)
			if sizeBytes != nil {
				length = *sizeBytes
			}
			_, err = c.putStream(ctx, begin.DirectPut.Access, reader, length)
		}
		if err == nil {
			completion = &loonfs.UploadCompletion{DirectPut: &loonfs.CompleteUploadDirectPut{Content: reader.claim(begin.DirectPut.ChecksumAlgorithm)}}
		}
	case begin.DirectMultipart != nil:
		uploadID = begin.DirectMultipart.UploadID
		completion, err = c.streamMultipart(ctx, uploadsClient, namespaceID, begin.DirectMultipart, reader)
	default:
		return nil, fmt.Errorf("transfers: unsupported upload mode %q", begin.Mode)
	}
	if err == nil {
		err = reader.finish()
	}
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		abortUpload(cleanup, uploadsClient, namespaceID, uploadID)
		return nil, err
	}
	// A lost completion response may still have completed the upload. Keep
	// that session available for inspection rather than aborting it.
	completed, err := uploadsClient.Complete(ctx, &loonfs.CompleteUploadRequest{NamespaceID: string(namespaceID), UploadID: string(uploadID), Body: completion}, option.WithMaxAttempts(1))
	if err != nil {
		return nil, fmt.Errorf("transfers: complete upload %s: %w", uploadID, err)
	}
	status, err := completedUploadStatus(completed)
	if err != nil {
		return nil, err
	}
	if status.ContentRef.SizeBytes != reader.count {
		return nil, fmt.Errorf("transfers: completed upload size mismatch")
	}
	return &PreparedFileContent{ContentRef: status.ContentRef, ContentToken: status.ContentToken}, nil
}

func (c *Client) putStream(ctx context.Context, access *loonfs.ObjectTransferAccess, source io.Reader, length int64) (string, error) {
	response, err := sendPresignedWithClient(ctx, c.transferHTTPClient(), access, http.MethodPut, source, length)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", responseStatusError(response)
	}
	if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024)); err != nil {
		return "", err
	}
	return response.Header.Get("ETag"), nil
}

func (c *Client) streamMultipart(ctx context.Context, client *uploads.Client, namespaceID loonfs.NamespaceID, begin *loonfs.BeginUploadResponseDirectMultipart, reader *uploadReader) (*loonfs.UploadCompletion, error) {
	partSize := int(begin.PartSizeBytes)
	if partSize <= 0 || int64(partSize) != begin.PartSizeBytes {
		return nil, fmt.Errorf("transfers: invalid multipart part size")
	}
	digest, err := newChecksum(begin.ChecksumAlgorithm)
	if err != nil {
		return nil, err
	}
	reader.digest = digest
	buffer := make([]byte, partSize)
	parts := make([]*loonfs.CompletedUploadPart, 0)
	for {
		n, err := io.ReadFull(reader, buffer)
		if reader.failure != nil {
			return nil, reader.failure
		}
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, err
		}
		if n == 0 {
			break
		}
		if len(parts) == 10000 {
			return nil, fmt.Errorf("transfers: multipart upload exceeds 10000 parts")
		}
		part := buffer[:n]
		number := len(parts) + 1
		checksum, err := computeChecksum(begin.ChecksumAlgorithm, part)
		if err != nil {
			return nil, err
		}
		signed, err := client.SignParts(ctx, &loonfs.SignUploadPartsRequest{NamespaceID: string(namespaceID), UploadID: string(begin.UploadID), Parts: []*loonfs.UploadPartChecksumClaim{{PartNumber: number, Checksum: checksum}}}, option.WithMaxAttempts(1))
		if err != nil {
			return nil, err
		}
		if signed == nil || len(signed.Parts) != 1 || signed.Parts[0].PartNumber != number {
			return nil, fmt.Errorf("transfers: server did not sign requested part %d", number)
		}
		etag, err := c.putStream(ctx, signed.Parts[0].Access, bytes.NewReader(part), int64(n))
		if err != nil {
			return nil, err
		}
		if etag == "" {
			return nil, fmt.Errorf("transfers: part %d has no ETag", number)
		}
		parts = append(parts, &loonfs.CompletedUploadPart{PartNumber: number, Checksum: checksum, Etag: etag})
	}
	return &loonfs.UploadCompletion{DirectMultipart: &loonfs.CompleteUploadDirectMultipart{Content: reader.claim(begin.ChecksumAlgorithm), Parts: parts}}, nil
}

type uploadReader struct {
	ctx             context.Context
	source          io.Reader
	expected, limit *int64
	digest          hash.Hash
	count           int64
	ended           bool
	failure         error
}

func (r *uploadReader) Read(buffer []byte) (int, error) {
	if r.ended {
		return 0, io.EOF
	}
	if r.failure != nil {
		return 0, r.failure
	}
	if err := r.ctx.Err(); err != nil {
		r.failure = err
		return 0, err
	}
	if len(buffer) > transferChunkBytes {
		buffer = buffer[:transferChunkBytes]
	}
	n, err := r.source.Read(buffer)
	r.count += int64(n)
	if r.limit != nil && r.count > *r.limit {
		err = fmt.Errorf("transfers: source exceeds advertised proxy limit")
	}
	if r.expected != nil && (r.count > *r.expected || (err == io.EOF && r.count != *r.expected)) {
		err = fmt.Errorf("transfers: source does not match declared size")
	}
	if err != nil && err != io.EOF {
		r.failure = err
		return 0, err
	}
	if r.digest != nil {
		r.digest.Write(buffer[:n])
	}
	if err == io.EOF {
		r.ended = true
	}
	return n, err
}
func (r *uploadReader) finish() error {
	if r.failure != nil {
		return r.failure
	}
	if !r.ended {
		return fmt.Errorf("transfers: successful response before source reached EOF")
	}
	return nil
}
func (r *uploadReader) claim(algorithm loonfs.ChecksumAlgorithm) *loonfs.UploadContentClaim {
	return &loonfs.UploadContentClaim{SizeBytes: r.count, Checksum: &loonfs.Checksum{Algorithm: algorithm, Value: hex.EncodeToString(r.digest.Sum(nil))}}
}
