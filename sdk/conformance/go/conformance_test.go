package conformance_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/client"
	"github.com/loonfs/loonfs-sdk-go/option"
	loonfsproxy "github.com/loonfs/loonfs-sdk-go/proxy"
	"github.com/loonfs/loonfs-sdk-go/transfers"
)

type conformanceCase struct {
	Name     string          `json:"name"`
	Intent   string          `json:"intent"`
	Request  json.RawMessage `json:"request"`
	Expected json.RawMessage `json:"expected"`
}

var expectedCases = []string{
	"changes",
	"commit_replay",
	"download",
	"end_to_end",
	"error_contract",
	"pagination",
	"proxy",
	"upload_abort",
	"upload_direct_put",
	"upload_multipart",
}

type harness struct {
	client          *client.Client
	unauthenticated *client.Client
	serverBaseURL   string
	serverToken     string
}

func TestSDKConformance(t *testing.T) {
	baseURL := os.Getenv("LOONFS_CONFORMANCE_URL")
	if baseURL == "" {
		t.Skip("run scripts/run-sdk-conformance.sh go")
	}
	token := os.Getenv("LOONFS_CONFORMANCE_TOKEN")
	if token == "" {
		t.Fatal("LOONFS_CONFORMANCE_TOKEN is not set")
	}
	casesDirectory := os.Getenv("LOONFS_CONFORMANCE_CASES")
	if casesDirectory == "" {
		t.Fatal("LOONFS_CONFORMANCE_CASES is not set")
	}

	cases, err := loadCases(casesDirectory)
	if err != nil {
		t.Fatalf("load conformance cases: %v", err)
	}
	h := &harness{
		client: client.NewClient(
			option.WithBaseURL(baseURL),
			option.WithToken(token),
		),
		unauthenticated: client.NewClient(option.WithBaseURL(baseURL)),
		serverBaseURL:   baseURL,
		serverToken:     token,
	}

	for _, testCase := range cases {
		t.Run(testCase.Name, func(t *testing.T) {
			switch testCase.Name {
			case "error_contract":
				runErrorContract(t, h, testCase)
			case "commit_replay":
				runCommitReplay(t, h, testCase)
			case "pagination":
				runPagination(t, h, testCase)
			case "proxy":
				runProxy(t, h, testCase)
			case "changes":
				runChanges(t, h, testCase)
			case "upload_direct_put":
				runDirectPut(t, h, testCase)
			case "upload_multipart":
				runMultipart(t, h, testCase)
			case "upload_abort":
				runAbort(t, h, testCase)
			case "download":
				runDownload(t, h, testCase)
			case "end_to_end":
				runEndToEnd(t, h, testCase)
			default:
				t.Fatalf("unknown case %q", testCase.Name)
			}
		})
	}
}

func loadCases(directory string) ([]conformanceCase, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	cases := make([]conformanceCase, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		testCase, err := decodeStrict[conformanceCase](data)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", path, err)
		}
		if err := validateCase(path, &testCase); err != nil {
			return nil, err
		}
		cases = append(cases, testCase)
	}
	sort.Slice(cases, func(i, j int) bool {
		return cases[i].Name < cases[j].Name
	})
	if len(cases) != len(expectedCases) {
		return nil, fmt.Errorf("fixture corpus requires %d cases, found %d", len(expectedCases), len(cases))
	}
	for index, expected := range expectedCases {
		if cases[index].Name != expected {
			return nil, fmt.Errorf("expected %q, found %q", expected, cases[index].Name)
		}
	}
	return cases, nil
}

func validateCase(path string, testCase *conformanceCase) error {
	stem := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	if testCase.Name != stem {
		return fmt.Errorf("invalid fixture %s: name is %q, expected %q", path, testCase.Name, stem)
	}
	if strings.TrimSpace(testCase.Intent) == "" {
		return fmt.Errorf("invalid fixture %s: intent must not be empty", path)
	}
	if !isJSONObject(testCase.Request) {
		return fmt.Errorf("invalid fixture %s: request field must be a JSON object", path)
	}
	if !isJSONObject(testCase.Expected) {
		return fmt.Errorf("invalid fixture %s: expected field must be a JSON object", path)
	}
	return nil
}

func isJSONObject(data []byte) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal(data, &object) == nil && object != nil
}

func decodeStrict[T any](data []byte) (T, error) {
	var value T
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return value, errors.New("JSON contains a second value")
		}
		return value, err
	}
	return value, nil
}

func decodeCaseValues[R, E any](t *testing.T, testCase conformanceCase) (R, E) {
	t.Helper()
	request, err := decodeStrict[R](testCase.Request)
	if err != nil {
		t.Fatalf("decode %s request: %v", testCase.Name, err)
	}
	expected, err := decodeStrict[E](testCase.Expected)
	if err != nil {
		t.Fatalf("decode %s expected values: %v", testCase.Name, err)
	}
	return request, expected
}

type errorContractRequest struct {
	NamespaceID string `json:"namespace_id"`
}

type errorContractExpected struct {
	Unauthenticated errorStatusExpected `json:"unauthenticated"`
}

type errorStatusExpected struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
}

func runErrorContract(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[errorContractRequest, errorContractExpected](t, testCase)
	_, err := h.unauthenticated.Namespaces.GetNamespace(
		context.Background(),
		&loonfs.GetNamespaceRequest{NamespaceID: request.NamespaceID},
	)
	var unauthorized *loonfs.UnauthorizedError
	if !errors.As(err, &unauthorized) {
		t.Fatalf("expected UnauthorizedError, found %T: %v", err, err)
	}
	if unauthorized.StatusCode != expected.Unauthenticated.Status {
		t.Errorf("unauthenticated status = %d, want %d", unauthorized.StatusCode, expected.Unauthenticated.Status)
	}
	if unauthorized.Body == nil {
		t.Fatal("unauthenticated error has no body")
	}
	if unauthorized.Body.Code != expected.Unauthenticated.Code {
		t.Errorf("unauthenticated code = %q, want %q", unauthorized.Body.Code, expected.Unauthenticated.Code)
	}
	if unauthorized.Body.RequestID == nil {
		t.Error("unauthenticated error has no request_id")
	}
}

type commitReplayRequest struct {
	NamespaceID string          `json:"namespace_id"`
	CommitID    string          `json:"commit_id"`
	Actor       loonfs.ActorRef `json:"actor"`
	Message     string          `json:"message"`
	Path        string          `json:"path"`
}

type commitReplayExpected struct {
	CommittedSeq int64 `json:"committed_seq"`
}

func runCommitReplay(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[commitReplayRequest, commitReplayExpected](t, testCase)
	createNamespace(t, h.client, request.NamespaceID)
	commit := createDirectoryCommit(
		request.NamespaceID,
		request.CommitID,
		&request.Actor,
		request.Path,
		&request.Message,
	)
	first, err := h.client.Filesystem.ApplyCommit(context.Background(), commit)
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	replayed, err := h.client.Filesystem.ApplyCommit(context.Background(), commit)
	if err != nil {
		t.Fatalf("replayed commit: %v", err)
	}

	if int64(first.CommittedSeq) != expected.CommittedSeq {
		t.Errorf("first committed_seq = %d, want %d", first.CommittedSeq, expected.CommittedSeq)
	}
	if string(first.CommitID) != request.CommitID {
		t.Errorf("first commit_id = %q, want %q", first.CommitID, request.CommitID)
	}
	if replayed.CommittedSeq != first.CommittedSeq {
		t.Errorf("replayed committed_seq = %d, want %d", replayed.CommittedSeq, first.CommittedSeq)
	}
	if replayed.CommitID != first.CommitID ||
		replayed.CommittedSeq != first.CommittedSeq ||
		replayed.NamespaceID != first.NamespaceID {
		t.Errorf("replayed commit = %#v, want %#v", replayed, first)
	}
}

type directPutRequest struct {
	NamespaceID string          `json:"namespace_id"`
	Path        string          `json:"path"`
	CommitID    string          `json:"commit_id"`
	Actor       loonfs.ActorRef `json:"actor"`
	ContentUTF8 string          `json:"content_utf8"`
}

type directPutExpected struct {
	Mode              string `json:"mode"`
	SizeBytes         int64  `json:"size_bytes"`
	ChecksumAlgorithm string `json:"checksum_algorithm"`
	CommittedSeq      int64  `json:"committed_seq"`
}

func runDirectPut(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[directPutRequest, directPutExpected](t, testCase)
	createNamespace(t, h.client, request.NamespaceID)
	payload := []byte(request.ContentUTF8)
	sizeBytes := int64(len(payload))
	begin, err := h.client.Uploads.BeginUpload(context.Background(), &loonfs.BeginUploadBody{
		NamespaceID: request.NamespaceID,
		Body: &loonfs.BeginUploadRequest{
			DirectPut: &loonfs.BeginUploadDirectPut{SizeBytes: &sizeBytes},
		},
	})
	if err != nil {
		t.Fatalf("begin direct PUT: %v", err)
	}
	if begin.DirectPut == nil || begin.DirectPut.DirectPut == nil {
		t.Fatalf("begin upload mode = %q, want %q", begin.Mode, expected.Mode)
	}
	directPut := begin.DirectPut.DirectPut
	if begin.Mode != expected.Mode {
		t.Errorf("begin upload mode = %q, want %q", begin.Mode, expected.Mode)
	}
	if string(directPut.ChecksumAlgorithm) != expected.ChecksumAlgorithm {
		t.Errorf("direct PUT checksum_algorithm = %q, want %q", directPut.ChecksumAlgorithm, expected.ChecksumAlgorithm)
	}

	putPresigned(t, directPut.Access, payload, false)
	claim := &loonfs.UploadContentClaim{
		Checksum:  mustChecksum(t, directPut.ChecksumAlgorithm, payload),
		SizeBytes: int64(len(payload)),
	}
	completed, err := h.client.Uploads.CompleteUpload(context.Background(), &loonfs.CompleteUploadBody{
		NamespaceID: request.NamespaceID,
		UploadID:    string(begin.DirectPut.UploadID),
		Body: &loonfs.CompleteUploadRequest{
			DirectPut: &loonfs.CompleteUploadDirectPut{Content: claim},
		},
	})
	if err != nil {
		t.Fatalf("complete direct PUT: %v", err)
	}
	completedStatus := requireCompletedStatus(t, completed)
	if completedStatus.ContentRef.SizeBytes != expected.SizeBytes {
		t.Errorf("direct PUT size_bytes = %d, want %d", completedStatus.ContentRef.SizeBytes, expected.SizeBytes)
	}
	if completedStatus.ContentRef.Checksum == nil || string(completedStatus.ContentRef.Checksum.Algorithm) != expected.ChecksumAlgorithm {
		t.Errorf("direct PUT checksum = %#v, want algorithm %q", completedStatus.ContentRef.Checksum, expected.ChecksumAlgorithm)
	}
	assertChecksumEqual(t, completedStatus.ContentRef.Checksum, claim.Checksum)
	assertChecksum(t, completedStatus.ContentRef.Checksum, payload)

	committed := commitCompletedFile(
		t,
		h.client,
		request.NamespaceID,
		request.Path,
		request.CommitID,
		&request.Actor,
		completedStatus.ContentRef,
		completedStatus.ContentToken,
	)
	if int64(committed.CommittedSeq) != expected.CommittedSeq {
		t.Errorf("committed_seq = %d, want %d", committed.CommittedSeq, expected.CommittedSeq)
	}
	stat := statPath(t, h.client, request.NamespaceID, request.Path)
	file := requireFileProjection(t, stat)
	assertContentRefEqual(t, file.ContentRef, completedStatus.ContentRef)
	readback := getFile(t, h.client, request.NamespaceID, request.Path)
	if !bytes.Equal(readback.Bytes, payload) {
		t.Error("direct PUT readback did not match payload")
	}
}

type multipartRequest struct {
	NamespaceID    string          `json:"namespace_id"`
	Path           string          `json:"path"`
	CommitID       string          `json:"commit_id"`
	Actor          loonfs.ActorRef `json:"actor"`
	PartSizeBytes  int64           `json:"part_size_bytes"`
	ContentPattern bytePattern     `json:"content_pattern"`
}

type bytePattern struct {
	Length  int64 `json:"length"`
	Modulus uint8 `json:"modulus"`
}

type multipartExpected struct {
	Mode              string `json:"mode"`
	PartCount         int    `json:"part_count"`
	SizeBytes         int64  `json:"size_bytes"`
	ChecksumAlgorithm string `json:"checksum_algorithm"`
	CommittedSeq      int64  `json:"committed_seq"`
}

func runMultipart(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[multipartRequest, multipartExpected](t, testCase)
	createNamespace(t, h.client, request.NamespaceID)
	payload := makeBytePattern(t, request.ContentPattern)
	begin, err := h.client.Uploads.BeginUpload(context.Background(), &loonfs.BeginUploadBody{
		NamespaceID: request.NamespaceID,
		Body: &loonfs.BeginUploadRequest{
			DirectMultipart: &loonfs.BeginUploadDirectMultipart{
				Multipart: &loonfs.DirectMultipartUploadOptions{
					PartSizeBytes: &request.PartSizeBytes,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("begin multipart upload: %v", err)
	}
	if begin.DirectMultipart == nil || begin.DirectMultipart.DirectMultipart == nil {
		t.Fatalf("begin upload mode = %q, want %q", begin.Mode, expected.Mode)
	}
	multipart := begin.DirectMultipart.DirectMultipart
	if begin.Mode != expected.Mode {
		t.Errorf("begin upload mode = %q, want %q", begin.Mode, expected.Mode)
	}
	if multipart.PartSizeBytes != request.PartSizeBytes {
		t.Errorf("part_size_bytes = %d, want %d", multipart.PartSizeBytes, request.PartSizeBytes)
	}
	if string(multipart.ChecksumAlgorithm) != expected.ChecksumAlgorithm {
		t.Errorf("checksum_algorithm = %q, want %q", multipart.ChecksumAlgorithm, expected.ChecksumAlgorithm)
	}
	parts := splitPayload(t, payload, multipart.PartSizeBytes)
	if len(parts) != expected.PartCount {
		t.Fatalf("part count = %d, want %d", len(parts), expected.PartCount)
	}
	claims := make([]*loonfs.UploadPartChecksumClaim, len(parts))
	for index, part := range parts {
		claims[index] = &loonfs.UploadPartChecksumClaim{
			Checksum:   mustChecksum(t, multipart.ChecksumAlgorithm, part),
			PartNumber: index + 1,
		}
	}
	signed, err := h.client.Uploads.SignUploadParts(context.Background(), &loonfs.SignUploadPartsRequest{
		NamespaceID: request.NamespaceID,
		UploadID:    string(begin.DirectMultipart.UploadID),
		Parts:       claims,
	})
	if err != nil {
		t.Fatalf("sign multipart parts: %v", err)
	}
	if len(signed.Parts) != expected.PartCount {
		t.Fatalf("signed part count = %d, want %d", len(signed.Parts), expected.PartCount)
	}
	completedParts := make([]*loonfs.CompletedUploadPart, 0, len(parts))
	for _, signedPart := range signed.Parts {
		partNumber := signedPart.PartNumber
		if partNumber < 1 || partNumber > len(parts) {
			t.Fatalf("signed invalid part number %d", partNumber)
		}
		etag := putPresigned(t, signedPart.Access, parts[partNumber-1], true)
		completedParts = append(completedParts, &loonfs.CompletedUploadPart{
			Checksum:   claims[partNumber-1].Checksum,
			Etag:       etag,
			PartNumber: partNumber,
		})
	}
	sort.Slice(completedParts, func(left, right int) bool {
		return completedParts[left].PartNumber < completedParts[right].PartNumber
	})
	wholeChecksum := mustChecksum(t, multipart.ChecksumAlgorithm, payload)
	completionRequest := &loonfs.CompleteUploadBody{
		NamespaceID: request.NamespaceID,
		UploadID:    string(begin.DirectMultipart.UploadID),
		Body: &loonfs.CompleteUploadRequest{
			DirectMultipart: &loonfs.CompleteUploadDirectMultipart{
				Content: &loonfs.UploadContentClaim{
					Checksum:  wholeChecksum,
					SizeBytes: int64(len(payload)),
				},
				Parts: completedParts,
			},
		},
	}
	first, err := h.client.Uploads.CompleteUpload(context.Background(), completionRequest)
	if err != nil {
		t.Fatalf("complete multipart upload: %v", err)
	}
	firstStatus := requireCompletedStatus(t, first)
	replayed, err := h.client.Uploads.CompleteUpload(context.Background(), completionRequest)
	if err != nil {
		t.Fatalf("replay multipart completion: %v", err)
	}
	replayedStatus := requireCompletedStatus(t, replayed)
	if replayed.NamespaceID != first.NamespaceID || replayed.UploadID != first.UploadID || replayed.Mode != first.Mode {
		t.Errorf("replayed upload identity = %#v, want %#v", replayed, first)
	}
	assertContentRefEqual(t, replayedStatus.ContentRef, firstStatus.ContentRef)
	if replayedStatus.CompletedAtMs != firstStatus.CompletedAtMs {
		t.Errorf("replayed completed_at_ms = %d, want %d", replayedStatus.CompletedAtMs, firstStatus.CompletedAtMs)
	}
	if firstStatus.ContentRef.SizeBytes != expected.SizeBytes {
		t.Errorf("multipart size_bytes = %d, want %d", firstStatus.ContentRef.SizeBytes, expected.SizeBytes)
	}
	assertChecksumEqual(t, firstStatus.ContentRef.Checksum, wholeChecksum)
	assertChecksum(t, firstStatus.ContentRef.Checksum, payload)

	committed := commitCompletedFile(
		t,
		h.client,
		request.NamespaceID,
		request.Path,
		request.CommitID,
		&request.Actor,
		firstStatus.ContentRef,
		replayedStatus.ContentToken,
	)
	if int64(committed.CommittedSeq) != expected.CommittedSeq {
		t.Errorf("committed_seq = %d, want %d", committed.CommittedSeq, expected.CommittedSeq)
	}
	readback := getFile(t, h.client, request.NamespaceID, request.Path)
	if !bytes.Equal(readback.Bytes, payload) {
		t.Error("multipart readback did not match payload")
	}

	// The same content through the high-level helper: the payload exceeds the
	// part size, so this exercises PutFile's multipart branch.
	helperPath := request.Path + "-helper"
	helperCommit, err := transfers.PutFile(context.Background(), h.client, transfers.PutFileInput{
		NamespaceID: loonfs.NamespaceID(request.NamespaceID),
		Path:        loonfs.AbsolutePath(helperPath),
		Bytes:       payload,
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID(request.CommitID + "-helper"),
	})
	if err != nil {
		t.Fatalf("helper multipart put: %v", err)
	}
	if helperCommit.CommittedSeq == 0 {
		t.Error("helper multipart put reported no committed_seq")
	}
	helperReadback := getFile(t, h.client, request.NamespaceID, helperPath)
	if !bytes.Equal(helperReadback.Bytes, payload) {
		t.Error("helper multipart readback did not match payload")
	}
	// Content ids are random per upload and the helper may choose a different
	// checksum algorithm; the comparable content fact is the size.
	if helperReadback.ContentRef == nil || helperReadback.ContentRef.SizeBytes != firstStatus.ContentRef.SizeBytes {
		t.Error("helper multipart size did not match the manual upload")
	}
}

type abortRequest struct {
	NamespaceID string `json:"namespace_id"`
}

type abortExpected struct {
	Mode   string `json:"mode"`
	Status string `json:"status"`
}

func runAbort(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[abortRequest, abortExpected](t, testCase)
	createNamespace(t, h.client, request.NamespaceID)
	begin, err := h.client.Uploads.BeginUpload(context.Background(), &loonfs.BeginUploadBody{
		NamespaceID: request.NamespaceID,
		Body: &loonfs.BeginUploadRequest{
			ServiceProxied: &loonfs.BeginUploadServiceProxied{},
		},
	})
	if err != nil {
		t.Fatalf("begin abortable upload: %v", err)
	}
	if begin.ServiceProxied == nil {
		t.Fatalf("begin upload mode = %q, want %q", begin.Mode, expected.Mode)
	}
	if begin.Mode != expected.Mode {
		t.Errorf("begin upload mode = %q, want %q", begin.Mode, expected.Mode)
	}
	abort := &loonfs.AbortUploadRequest{
		NamespaceID: request.NamespaceID,
		UploadID:    string(begin.ServiceProxied.UploadID),
	}
	first, err := h.client.Uploads.AbortUpload(context.Background(), abort)
	if err != nil {
		t.Fatalf("abort upload: %v", err)
	}
	replayed, err := h.client.Uploads.AbortUpload(context.Background(), abort)
	if err != nil {
		t.Fatalf("replay abort: %v", err)
	}
	firstStatusEnvelope := uploadSessionStatus(t, first)
	if firstStatusEnvelope.Status != expected.Status {
		t.Errorf("upload status = %q, want %q", firstStatusEnvelope.Status, expected.Status)
	}
	if firstStatusEnvelope.Aborted == nil {
		t.Fatalf("upload status = %q, want aborted", firstStatusEnvelope.Status)
	}
	firstStatus := firstStatusEnvelope.Aborted
	replayedStatus := requireAbortedStatus(t, replayed)
	if replayed.NamespaceID != first.NamespaceID || replayed.UploadID != first.UploadID || replayed.Mode != first.Mode {
		t.Errorf("replayed abort identity = %#v, want %#v", replayed, first)
	}
	if replayedStatus.AbortedAtMs != firstStatus.AbortedAtMs {
		t.Errorf("replayed aborted_at_ms = %d, want %d", replayedStatus.AbortedAtMs, firstStatus.AbortedAtMs)
	}
}

type downloadRequest struct {
	NamespaceID string          `json:"namespace_id"`
	Path        string          `json:"path"`
	CommitID    string          `json:"commit_id"`
	Actor       loonfs.ActorRef `json:"actor"`
	ContentUTF8 string          `json:"content_utf8"`
}

type downloadExpected struct {
	SizeBytes         int64  `json:"size_bytes"`
	ChecksumAlgorithm string `json:"checksum_algorithm"`
	CommittedSeq      int64  `json:"committed_seq"`
}

func runDownload(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[downloadRequest, downloadExpected](t, testCase)
	createNamespace(t, h.client, request.NamespaceID)
	payload := []byte(request.ContentUTF8)
	commitID := loonfs.CommitID(request.CommitID)
	committed, err := transfers.PutFile(context.Background(), h.client, transfers.PutFileInput{
		NamespaceID: loonfs.NamespaceID(request.NamespaceID),
		Path:        loonfs.AbsolutePath(request.Path),
		Bytes:       payload,
		Actor:       &request.Actor,
		CommitID:    commitID,
	})
	if err != nil {
		t.Fatalf("put download file: %v", err)
	}
	if int64(committed.CommittedSeq) != expected.CommittedSeq {
		t.Errorf("committed_seq = %d, want %d", committed.CommittedSeq, expected.CommittedSeq)
	}
	stat := statPath(t, h.client, request.NamespaceID, request.Path)
	file := requireFileProjection(t, stat)
	grant, err := h.client.Filesystem.BeginDownload(context.Background(), &loonfs.BeginDownloadRequest{
		NamespaceID: request.NamespaceID,
		Path:        loonfs.AbsolutePath(request.Path),
	})
	if err != nil {
		t.Fatalf("begin direct download: %v", err)
	}
	if grant == nil || grant.ContentRef == nil {
		t.Fatal("download grant has no content_ref")
	}
	assertContentRefEqual(t, file.ContentRef, grant.ContentRef)
	if grant.ContentRef.SizeBytes != expected.SizeBytes {
		t.Errorf("download size_bytes = %d, want %d", grant.ContentRef.SizeBytes, expected.SizeBytes)
	}
	if grant.ContentRef.Checksum == nil || string(grant.ContentRef.Checksum.Algorithm) != expected.ChecksumAlgorithm {
		t.Errorf("download checksum = %#v, want algorithm %q", grant.ContentRef.Checksum, expected.ChecksumAlgorithm)
	}
	readback := getPresigned(t, grant.Access)
	if int64(len(readback)) != grant.ContentRef.SizeBytes {
		t.Errorf("downloaded bytes = %d, want %d", len(readback), grant.ContentRef.SizeBytes)
	}
	assertChecksum(t, grant.ContentRef.Checksum, readback)
	if !bytes.Equal(readback, payload) {
		t.Error("downloaded content did not match payload")
	}
}

type endToEndRequest struct {
	NamespaceID string            `json:"namespace_id"`
	Directory   string            `json:"directory"`
	UploadPath  string            `json:"upload_path"`
	MovedPath   string            `json:"moved_path"`
	Actor       loonfs.ActorRef   `json:"actor"`
	ContentUTF8 string            `json:"content_utf8"`
	CommitIDs   endToEndCommitIDs `json:"commit_ids"`
}

type endToEndCommitIDs struct {
	Mkdir  string `json:"mkdir"`
	Upload string `json:"upload"`
	Move   string `json:"move"`
	Remove string `json:"remove"`
}

type endToEndExpected struct {
	MkdirCommittedSeq  int64 `json:"mkdir_committed_seq"`
	UploadCommittedSeq int64 `json:"upload_committed_seq"`
	MoveCommittedSeq   int64 `json:"move_committed_seq"`
	RemoveCommittedSeq int64 `json:"remove_committed_seq"`
	SizeBytes          int64 `json:"size_bytes"`
	RevisionCount      int   `json:"revision_count"`
	ChangeCount        int   `json:"change_count"`
}

func runEndToEnd(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[endToEndRequest, endToEndExpected](t, testCase)
	createNamespace(t, h.client, request.NamespaceID)
	mkdir := applyCommit(t, h.client, createDirectoryCommit(
		request.NamespaceID,
		request.CommitIDs.Mkdir,
		&request.Actor,
		request.Directory,
		nil,
	))
	if int64(mkdir.CommittedSeq) != expected.MkdirCommittedSeq {
		t.Errorf("mkdir committed_seq = %d, want %d", mkdir.CommittedSeq, expected.MkdirCommittedSeq)
	}

	uploadCommitID := loonfs.CommitID(request.CommitIDs.Upload)
	upload, err := transfers.PutFile(context.Background(), h.client, transfers.PutFileInput{
		NamespaceID: loonfs.NamespaceID(request.NamespaceID),
		Path:        loonfs.AbsolutePath(request.UploadPath),
		Bytes:       []byte(request.ContentUTF8),
		Actor:       &request.Actor,
		CommitID:    uploadCommitID,
	})
	if err != nil {
		t.Fatalf("upload end-to-end file: %v", err)
	}
	if int64(upload.CommittedSeq) != expected.UploadCommittedSeq {
		t.Errorf("upload committed_seq = %d, want %d", upload.CommittedSeq, expected.UploadCommittedSeq)
	}
	stat := statPath(t, h.client, request.NamespaceID, request.UploadPath)
	file := requireFileProjection(t, stat)
	if file.SizeBytes != expected.SizeBytes {
		t.Errorf("uploaded size_bytes = %d, want %d", file.SizeBytes, expected.SizeBytes)
	}
	uploadedInode := stat.InodeID

	initialListing := listPathEntries(t, h.client, request.NamespaceID, request.Directory)
	if !listingContainsPath(initialListing, request.UploadPath) {
		t.Errorf("initial listing does not contain %q", request.UploadPath)
	}
	downloaded := getFile(t, h.client, request.NamespaceID, request.UploadPath)
	if !bytes.Equal(downloaded.Bytes, []byte(request.ContentUTF8)) {
		t.Error("end-to-end download did not match payload")
	}

	noReplace := loonfs.DestinationBehaviorNoReplace
	moved := applyCommit(t, h.client, &loonfs.CommitRequest{
		NamespaceID: request.NamespaceID,
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID(request.CommitIDs.Move),
		Operations: []*loonfs.FilesystemOperation{
			{
				MovePath: &loonfs.FsOpMovePath{
					Behavior: &noReplace,
					FromPath: loonfs.AbsolutePath(request.UploadPath),
					ToPath:   loonfs.AbsolutePath(request.MovedPath),
				},
			},
		},
	})
	if int64(moved.CommittedSeq) != expected.MoveCommittedSeq {
		t.Errorf("move committed_seq = %d, want %d", moved.CommittedSeq, expected.MoveCommittedSeq)
	}
	movedListing := listPathEntries(t, h.client, request.NamespaceID, request.Directory)
	if !listingContainsPath(movedListing, request.MovedPath) {
		t.Errorf("moved listing does not contain %q", request.MovedPath)
	}

	revisions, err := h.client.Filesystem.ListFileRevisions(context.Background(), &loonfs.ListFileRevisionsRequest{
		NamespaceID: request.NamespaceID,
		Path:        request.MovedPath,
	})
	if err != nil {
		t.Fatalf("list end-to-end revisions: %v", err)
	}
	if len(revisions.Results) != expected.RevisionCount {
		t.Fatalf("revision count = %d, want %d", len(revisions.Results), expected.RevisionCount)
	}
	if string(revisions.Results[0].CommitID) != request.CommitIDs.Upload {
		t.Errorf("revision commit_id = %q, want %q", revisions.Results[0].CommitID, request.CommitIDs.Upload)
	}

	changesBeforeRemove := listChanges(t, h.client, request.NamespaceID)
	if len(changesBeforeRemove.Changes) != expected.ChangeCount-1 {
		t.Errorf("change count before remove = %d, want %d", len(changesBeforeRemove.Changes), expected.ChangeCount-1)
	}
	nonRecursive := loonfs.DeleteDirectoryBehaviorNonRecursive
	removed := applyCommit(t, h.client, &loonfs.CommitRequest{
		NamespaceID: request.NamespaceID,
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID(request.CommitIDs.Remove),
		Operations: []*loonfs.FilesystemOperation{
			{
				DeletePath: &loonfs.FsOpDeletePath{
					Behavior: &nonRecursive,
					Path:     loonfs.AbsolutePath(request.MovedPath),
				},
			},
		},
	})
	if int64(removed.CommittedSeq) != expected.RemoveCommittedSeq {
		t.Errorf("remove committed_seq = %d, want %d", removed.CommittedSeq, expected.RemoveCommittedSeq)
	}

	changes := listChanges(t, h.client, request.NamespaceID)
	if len(changes.Changes) != expected.ChangeCount {
		t.Fatalf("change count = %d, want %d", len(changes.Changes), expected.ChangeCount)
	}
	expectedIDs := []string{
		request.CommitIDs.Mkdir,
		request.CommitIDs.Upload,
		request.CommitIDs.Move,
		request.CommitIDs.Remove,
	}
	for index, change := range changes.Changes {
		if string(change.CommitID) != expectedIDs[index] {
			t.Errorf("change %d commit_id = %q, want %q", index, change.CommitID, expectedIDs[index])
		}
		if !actorsEqual(change.CommittedBy, &request.Actor) {
			t.Errorf("change %d committed_by = %#v, want %#v", index, change.CommittedBy, request.Actor)
		}
	}

	trash, err := h.client.Filesystem.ListTrash(context.Background(), &loonfs.ListTrashRequest{
		NamespaceID: request.NamespaceID,
	})
	if err != nil {
		t.Fatalf("list end-to-end trash: %v", err)
	}
	var removedEntry *loonfs.TrashEntry
	for _, entry := range trash.Results {
		if entry.InodeID == uploadedInode {
			removedEntry = entry
			break
		}
	}
	if removedEntry == nil {
		t.Fatalf("trash does not contain removed inode %q", uploadedInode)
	}
	if removedEntry.DeletionSeq != removed.CommittedSeq {
		t.Errorf("trash deletion_seq = %d, want %d", removedEntry.DeletionSeq, removed.CommittedSeq)
	}
}

type paginationRequest struct {
	NamespaceID     string          `json:"namespace_id"`
	Directory       string          `json:"directory"`
	Actor           loonfs.ActorRef `json:"actor"`
	EntryNames      []string        `json:"entry_names"`
	PageSize        int             `json:"page_size"`
	ResumeAfterPage int             `json:"resume_after_page"`
}

type paginationExpected struct {
	EntryCount       int   `json:"entry_count"`
	MinimumPageCount int   `json:"minimum_page_count"`
	HeadSeq          int64 `json:"head_seq"`
}

func runPagination(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[paginationRequest, paginationExpected](t, testCase)
	createNamespace(t, h.client, request.NamespaceID)
	applyCreateDirectory(
		t,
		h.client,
		request.NamespaceID,
		"conf-pagination-directory",
		&request.Actor,
		request.Directory,
	)
	for index, name := range request.EntryNames {
		applyCreateDirectory(
			t,
			h.client,
			request.NamespaceID,
			fmt.Sprintf("conf-pagination-entry-%02d", index),
			&request.Actor,
			request.Directory+"/"+name,
		)
	}

	observed := make([]string, 0, len(request.EntryNames))
	pageCount := 0
	var savedCursor *string
	resumeOffset := -1
	ctx := context.Background()
	page, err := h.client.Filesystem.ListPathEntries(
		ctx,
		&loonfs.ListPathEntriesRequest{
			NamespaceID: request.NamespaceID,
			Path:        request.Directory,
			Limit:       &request.PageSize,
		},
	)
	if err != nil {
		t.Fatalf("list first pagination page: %v", err)
	}
	var cursor *string
	for {
		if page.Response == nil {
			t.Fatal("pagination page has no response")
		}
		pageCount++
		if int64(page.Response.HeadSeq) != expected.HeadSeq {
			t.Errorf("page %d head_seq = %d, want %d", pageCount, page.Response.HeadSeq, expected.HeadSeq)
		}
		observed = append(observed, listedNames(t, page.Results)...)
		cursor = page.Response.NextCursor
		if pageCount == request.ResumeAfterPage {
			if cursor != nil {
				value := *cursor
				savedCursor = &value
			}
			resumeOffset = len(observed)
		}
		if cursor == nil {
			break
		}
		page, err = page.GetNextPage(ctx)
		if err != nil {
			t.Fatalf("list next pagination page: %v", err)
		}
	}
	if len(observed) != expected.EntryCount {
		t.Errorf("entry count = %d, want %d", len(observed), expected.EntryCount)
	}
	if pageCount < expected.MinimumPageCount {
		t.Errorf("page count = %d, want at least %d", pageCount, expected.MinimumPageCount)
	}
	if cursor != nil {
		t.Error("final cursor is not nil")
	}
	if savedCursor == nil {
		t.Fatal("resume cursor was not recorded")
	}
	if resumeOffset < 0 {
		t.Fatal("resume position was not recorded")
	}

	resumed := make([]string, 0, len(request.EntryNames)-resumeOffset)
	page, err = h.client.Filesystem.ListPathEntries(
		ctx,
		&loonfs.ListPathEntriesRequest{
			NamespaceID: request.NamespaceID,
			Path:        request.Directory,
			Limit:       &request.PageSize,
			Cursor:      savedCursor,
		},
	)
	if err != nil {
		t.Fatalf("resume pagination: %v", err)
	}
	for {
		if page.Response == nil {
			t.Fatal("resumed pagination page has no response")
		}
		resumed = append(resumed, listedNames(t, page.Results)...)
		cursor = page.Response.NextCursor
		if cursor == nil {
			break
		}
		page, err = page.GetNextPage(ctx)
		if err != nil {
			t.Fatalf("resume next pagination page: %v", err)
		}
	}
	if err := validatePageWalk(request.EntryNames, observed, resumeOffset, resumed); err != nil {
		t.Fatalf("pagination invariants: %v", err)
	}
}

func listedNames(t *testing.T, entries []*loonfs.AuthoritativePathEntry) []string {
	t.Helper()
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.DisplayName == nil {
			t.Fatal("listed entry has no display_name")
		}
		names = append(names, string(*entry.DisplayName))
	}
	return names
}

func validatePageWalk(expected, observed []string, resumeOffset int, resumed []string) error {
	seen := make(map[string]struct{}, len(observed))
	for _, name := range observed {
		if _, exists := seen[name]; exists {
			return fmt.Errorf("pagination returned %q more than once", name)
		}
		seen[name] = struct{}{}
	}
	if !equalStrings(observed, expected) {
		return errors.New("pagination returned unexpected entries")
	}
	if resumeOffset > len(expected) {
		return fmt.Errorf("pagination resume offset %d exceeds %d entries", resumeOffset, len(expected))
	}
	if !equalStrings(resumed, expected[resumeOffset:]) {
		return errors.New("resumed pagination returned unexpected entries")
	}
	return nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

type proxyCaseRequest struct {
	NamespaceAlias        string          `json:"namespace_alias"`
	NamespaceID           string          `json:"namespace_id"`
	UnknownNamespaceAlias string          `json:"unknown_namespace_alias"`
	Actor                 loonfs.ActorRef `json:"actor"`
	Directory             string          `json:"directory"`
	ProxiedPath           string          `json:"proxied_path"`
	DirectPath            string          `json:"direct_path"`
	CommitIDs             proxyCommitIDs  `json:"commit_ids"`
	ContentUTF8           string          `json:"content_utf8"`
	DisallowedPathSuffix  string          `json:"disallowed_path_suffix"`
}

type proxyCommitIDs struct {
	Directory string `json:"directory"`
	Proxied   string `json:"proxied"`
	Direct    string `json:"direct"`
}

type proxyExpected struct {
	MkdirCommittedSeq           int64 `json:"mkdir_committed_seq"`
	ProxiedCommittedSeq         int64 `json:"proxied_committed_seq"`
	DirectCommittedSeq          int64 `json:"direct_committed_seq"`
	EntryCount                  int   `json:"entry_count"`
	UnknownNamespaceAliasStatus int   `json:"unknown_namespace_alias_status"`
	DisallowedStatus            int   `json:"disallowed_route_status"`
}

func runProxy(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[proxyCaseRequest, proxyExpected](t, testCase)
	proxyHandler, err := loonfsproxy.NewHandler(loonfsproxy.Config{
		ServerBaseURL: h.serverBaseURL,
		Token:         h.serverToken,
		NamespaceAliases: map[string]string{
			request.NamespaceAlias: request.NamespaceID,
		},
	})
	if err != nil {
		t.Fatalf("create proxy handler: %v", err)
	}
	proxyServer := httptest.NewServer(proxyHandler)
	defer proxyServer.Close()

	createNamespace(t, h.client, request.NamespaceID)
	namespaceAliasBaseURL := proxyServer.URL + "/v0/namespace-aliases/" + url.PathEscape(request.NamespaceAlias)
	mkdir := proxyApplyCommit(t, proxyServer.Client(), namespaceAliasBaseURL, createDirectoryCommit(
		request.NamespaceID,
		request.CommitIDs.Directory,
		&request.Actor,
		request.Directory,
		nil,
	))
	if int64(mkdir.CommittedSeq) != expected.MkdirCommittedSeq {
		t.Errorf("proxy mkdir committed_seq = %d, want %d", mkdir.CommittedSeq, expected.MkdirCommittedSeq)
	}

	payload := []byte(request.ContentUTF8)
	proxiedBegin := proxyBeginUpload(t, proxyServer.Client(), namespaceAliasBaseURL, &loonfs.BeginUploadRequest{
		ServiceProxied: &loonfs.BeginUploadServiceProxied{},
	})
	if proxiedBegin.ServiceProxied == nil {
		t.Fatalf("proxy begin upload mode = %q, want service_proxied", proxiedBegin.Mode)
	}
	proxiedUploadID := string(proxiedBegin.ServiceProxied.UploadID)
	proxiedContentResponse := sendProxyRequest(
		t,
		proxyServer.Client(),
		http.MethodPut,
		namespaceAliasBaseURL+"/uploads/"+url.PathEscape(proxiedUploadID)+"/content",
		bytes.NewReader(payload),
		"application/octet-stream",
	)
	proxiedContent := decodeProxyJSONResponse[loonfs.UploadContentResponse](t, proxiedContentResponse)
	if proxiedContent.ContentRef == nil {
		t.Fatal("proxy upload content response has no content_ref")
	}
	proxiedCompletion := proxyCompleteUpload(
		t,
		proxyServer.Client(),
		namespaceAliasBaseURL,
		proxiedUploadID,
		&loonfs.CompleteUploadRequest{
			ServiceProxied: &loonfs.CompleteUploadServiceProxied{},
		},
	)
	proxiedStatus := requireCompletedStatus(t, proxiedCompletion)
	assertContentRefEqual(t, proxiedStatus.ContentRef, proxiedContent.ContentRef)
	proxiedCommit := proxyCommitCompletedFile(
		t,
		proxyServer.Client(),
		namespaceAliasBaseURL,
		request.NamespaceID,
		request.ProxiedPath,
		request.CommitIDs.Proxied,
		&request.Actor,
		proxiedStatus.ContentRef,
		proxiedStatus.ContentToken,
	)
	if int64(proxiedCommit.CommittedSeq) != expected.ProxiedCommittedSeq {
		t.Errorf("proxied upload committed_seq = %d, want %d", proxiedCommit.CommittedSeq, expected.ProxiedCommittedSeq)
	}

	sizeBytes := int64(len(payload))
	directBegin := proxyBeginUpload(t, proxyServer.Client(), namespaceAliasBaseURL, &loonfs.BeginUploadRequest{
		DirectPut: &loonfs.BeginUploadDirectPut{SizeBytes: &sizeBytes},
	})
	if directBegin.DirectPut == nil || directBegin.DirectPut.DirectPut == nil {
		t.Fatalf("proxy begin upload mode = %q, want direct_put", directBegin.Mode)
	}
	directUploadID := string(directBegin.DirectPut.UploadID)
	directPut := directBegin.DirectPut.DirectPut
	putPresigned(t, directPut.Access, payload, false)
	directClaim := &loonfs.UploadContentClaim{
		Checksum:  mustChecksum(t, directPut.ChecksumAlgorithm, payload),
		SizeBytes: sizeBytes,
	}
	directCompletion := proxyCompleteUpload(
		t,
		proxyServer.Client(),
		namespaceAliasBaseURL,
		directUploadID,
		&loonfs.CompleteUploadRequest{
			DirectPut: &loonfs.CompleteUploadDirectPut{Content: directClaim},
		},
	)
	directStatus := requireCompletedStatus(t, directCompletion)
	directCommit := proxyCommitCompletedFile(
		t,
		proxyServer.Client(),
		namespaceAliasBaseURL,
		request.NamespaceID,
		request.DirectPath,
		request.CommitIDs.Direct,
		&request.Actor,
		directStatus.ContentRef,
		directStatus.ContentToken,
	)
	if int64(directCommit.CommittedSeq) != expected.DirectCommittedSeq {
		t.Errorf("direct upload committed_seq = %d, want %d", directCommit.CommittedSeq, expected.DirectCommittedSeq)
	}

	listingQuery := url.Values{"path": []string{request.Directory}}
	listing := proxyJSONRequest[loonfs.ListPathEntriesResponse](
		t,
		proxyServer.Client(),
		http.MethodGet,
		namespaceAliasBaseURL+"/filesystem/list?"+listingQuery.Encode(),
		nil,
	)
	if len(listing.Entries) != expected.EntryCount {
		t.Errorf("proxy entry count = %d, want %d", len(listing.Entries), expected.EntryCount)
	}

	readQuery := url.Values{"path": []string{request.ProxiedPath}}
	readResponse := sendProxyRequest(
		t,
		proxyServer.Client(),
		http.MethodGet,
		namespaceAliasBaseURL+"/filesystem/content?"+readQuery.Encode(),
		nil,
		"",
	)
	defer readResponse.Body.Close()
	requireProxySuccess(t, readResponse)
	readback, err := io.ReadAll(readResponse.Body)
	if err != nil {
		t.Fatalf("read proxied file response: %v", err)
	}
	if !bytes.Equal(readback, payload) {
		t.Error("proxied file readback did not match payload")
	}

	unknownStatus := proxyResponseStatus(
		t,
		proxyServer.Client(),
		proxyServer.URL+"/v0/namespace-aliases/"+url.PathEscape(request.UnknownNamespaceAlias)+"/filesystem/list",
	)
	if unknownStatus != expected.UnknownNamespaceAliasStatus {
		t.Errorf("unknown namespace alias status = %d, want %d", unknownStatus, expected.UnknownNamespaceAliasStatus)
	}
	disallowedStatus := proxyResponseStatus(
		t,
		proxyServer.Client(),
		namespaceAliasBaseURL+request.DisallowedPathSuffix,
	)
	if disallowedStatus != expected.DisallowedStatus {
		t.Errorf("disallowed route status = %d, want %d", disallowedStatus, expected.DisallowedStatus)
	}
}

func proxyBeginUpload(
	t *testing.T,
	httpClient *http.Client,
	namespaceAliasBaseURL string,
	request *loonfs.BeginUploadRequest,
) *loonfs.BeginUploadResponse {
	t.Helper()
	return proxyJSONRequest[loonfs.BeginUploadResponse](
		t,
		httpClient,
		http.MethodPost,
		namespaceAliasBaseURL+"/uploads",
		request,
	)
}

func proxyCompleteUpload(
	t *testing.T,
	httpClient *http.Client,
	namespaceAliasBaseURL string,
	uploadID string,
	request *loonfs.CompleteUploadRequest,
) *loonfs.UploadSessionResponse {
	t.Helper()
	return proxyJSONRequest[loonfs.UploadSessionResponse](
		t,
		httpClient,
		http.MethodPost,
		namespaceAliasBaseURL+"/uploads/"+url.PathEscape(uploadID)+"/complete",
		request,
	)
}

func proxyCommitCompletedFile(
	t *testing.T,
	httpClient *http.Client,
	namespaceAliasBaseURL string,
	namespaceID string,
	path string,
	commitID string,
	actor *loonfs.ActorRef,
	contentRef *loonfs.ContentRef,
	contentToken *loonfs.ContentToken,
) *loonfs.CommitResponse {
	t.Helper()
	noReplace := loonfs.DestinationBehaviorNoReplace
	contentTokens := []*loonfs.ContentToken(nil)
	if contentToken != nil {
		contentTokens = []*loonfs.ContentToken{contentToken}
	}
	return proxyApplyCommit(t, httpClient, namespaceAliasBaseURL, &loonfs.CommitRequest{
		NamespaceID:   namespaceID,
		Actor:         actor,
		CommitID:      loonfs.CommitID(commitID),
		ContentTokens: contentTokens,
		Operations: []*loonfs.FilesystemOperation{
			{
				PutFile: &loonfs.FsOpPutFile{
					Behavior:   &noReplace,
					ContentRef: contentRef,
					Path:       loonfs.AbsolutePath(path),
				},
			},
		},
	})
}

func proxyApplyCommit(
	t *testing.T,
	httpClient *http.Client,
	namespaceAliasBaseURL string,
	request *loonfs.CommitRequest,
) *loonfs.CommitResponse {
	t.Helper()
	return proxyJSONRequest[loonfs.CommitResponse](
		t,
		httpClient,
		http.MethodPost,
		namespaceAliasBaseURL+"/commits",
		request,
	)
}

func proxyJSONRequest[T any](
	t *testing.T,
	httpClient *http.Client,
	method string,
	requestURL string,
	body any,
) *T {
	t.Helper()
	var requestBody io.Reader
	contentType := ""
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encode proxy request: %v", err)
		}
		requestBody = bytes.NewReader(payload)
		contentType = "application/json"
	}
	response := sendProxyRequest(t, httpClient, method, requestURL, requestBody, contentType)
	return decodeProxyJSONResponse[T](t, response)
}

func sendProxyRequest(
	t *testing.T,
	httpClient *http.Client,
	method string,
	requestURL string,
	body io.Reader,
	contentType string,
) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, requestURL, body)
	if err != nil {
		t.Fatalf("build proxy request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer browser-token")
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := httpClient.Do(request)
	if err != nil {
		t.Fatalf("send proxy request: %v", err)
	}
	return response
}

func decodeProxyJSONResponse[T any](t *testing.T, response *http.Response) *T {
	t.Helper()
	defer response.Body.Close()
	requireProxySuccess(t, response)
	var decoded T
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode proxy response: %v", err)
	}
	return &decoded
}

func requireProxySuccess(t *testing.T, response *http.Response) {
	t.Helper()
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return
	}
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4*1024))
	detail := strings.TrimSpace(string(body))
	if detail == "" {
		t.Fatalf("proxy request returned %s", response.Status)
	}
	t.Fatalf("proxy request returned %s: %s", response.Status, detail)
}

func proxyResponseStatus(t *testing.T, httpClient *http.Client, requestURL string) int {
	t.Helper()
	response := sendProxyRequest(t, httpClient, http.MethodGet, requestURL, nil, "")
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read proxy routing response: %v", err)
	}
	if len(body) != 0 {
		t.Errorf("proxy routing response body = %q, want empty", body)
	}
	return response.StatusCode
}

type changesRequest struct {
	NamespaceID string          `json:"namespace_id"`
	Path        string          `json:"path"`
	CommitID    string          `json:"commit_id"`
	Actor       loonfs.ActorRef `json:"actor"`
	AfterSeq    int64           `json:"after_seq"`
}

type changesExpected struct {
	CommittedSeq int64 `json:"committed_seq"`
	ChangeCount  int   `json:"change_count"`
}

func runChanges(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[changesRequest, changesExpected](t, testCase)
	createNamespace(t, h.client, request.NamespaceID)
	commit := createDirectoryCommit(
		request.NamespaceID,
		request.CommitID,
		&request.Actor,
		request.Path,
		nil,
	)
	committed, err := h.client.Filesystem.ApplyCommit(context.Background(), commit)
	if err != nil {
		t.Fatalf("commit change: %v", err)
	}
	if int64(committed.CommittedSeq) != expected.CommittedSeq {
		t.Errorf("committed_seq = %d, want %d", committed.CommittedSeq, expected.CommittedSeq)
	}
	feed, err := h.client.Filesystem.ListChanges(
		context.Background(),
		&loonfs.ListChangesRequest{
			NamespaceID: request.NamespaceID,
			AfterSeq:    loonfs.ChangeSeq(request.AfterSeq),
		},
	)
	if err != nil {
		t.Fatalf("list changes: %v", err)
	}
	if len(feed.Changes) != expected.ChangeCount {
		t.Fatalf("change count = %d, want %d", len(feed.Changes), expected.ChangeCount)
	}
	if len(feed.Changes) == 0 {
		t.Fatal("change feed is empty")
	}
	change := feed.Changes[0]
	if string(change.CommitID) != request.CommitID {
		t.Errorf("change commit_id = %q, want %q", change.CommitID, request.CommitID)
	}
	if change.CommittedBy == nil ||
		change.CommittedBy.ID != request.Actor.ID ||
		change.CommittedBy.Kind != request.Actor.Kind {
		t.Errorf("change committed_by = %#v, want %#v", change.CommittedBy, request.Actor)
	}
	if len(change.Events) != 1 || change.Events[0] == nil || change.Events[0].DirectoryCreated == nil {
		t.Errorf("change events = %#v, want one directory_created event", change.Events)
	}
}

var conformanceCRC64NVMeTable = crc64.MakeTable(0x9a6c9329ac4bc9b5)

func makeBytePattern(t *testing.T, pattern bytePattern) []byte {
	t.Helper()
	if pattern.Modulus == 0 {
		t.Fatal("byte pattern modulus must be greater than zero")
	}
	maximumInt := int(^uint(0) >> 1)
	if pattern.Length < 0 || uint64(pattern.Length) > uint64(maximumInt) {
		t.Fatalf("byte pattern length %d does not fit int", pattern.Length)
	}
	payload := make([]byte, int(pattern.Length))
	for offset := range payload {
		payload[offset] = byte(offset % int(pattern.Modulus))
	}
	return payload
}

func splitPayload(t *testing.T, payload []byte, partSizeBytes int64) [][]byte {
	t.Helper()
	maximumInt := int(^uint(0) >> 1)
	if partSizeBytes <= 0 || uint64(partSizeBytes) > uint64(maximumInt) {
		t.Fatalf("invalid part size %d", partSizeBytes)
	}
	partSize := int(partSizeBytes)
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

func mustChecksum(t *testing.T, algorithm loonfs.ChecksumAlgorithm, payload []byte) *loonfs.Checksum {
	t.Helper()
	checksum, err := checksumFor(algorithm, payload)
	if err != nil {
		t.Fatal(err)
	}
	return checksum
}

func checksumFor(algorithm loonfs.ChecksumAlgorithm, payload []byte) (*loonfs.Checksum, error) {
	var value string
	switch algorithm {
	case loonfs.ChecksumAlgorithmSha256:
		digest := sha256.Sum256(payload)
		value = hex.EncodeToString(digest[:])
	case loonfs.ChecksumAlgorithmCrc64Nvme:
		value = fmt.Sprintf("%016x", crc64.Checksum(payload, conformanceCRC64NVMeTable))
	default:
		return nil, fmt.Errorf("unsupported checksum algorithm %q", algorithm)
	}
	return &loonfs.Checksum{Algorithm: algorithm, Value: value}, nil
}

func assertChecksum(t *testing.T, expected *loonfs.Checksum, payload []byte) {
	t.Helper()
	if expected == nil {
		t.Fatal("expected checksum is nil")
	}
	actual := mustChecksum(t, expected.Algorithm, payload)
	assertChecksumEqual(t, actual, expected)
}

func assertChecksumEqual(t *testing.T, actual, expected *loonfs.Checksum) {
	t.Helper()
	if actual == nil || expected == nil {
		if actual != expected {
			t.Errorf("checksum = %#v, want %#v", actual, expected)
		}
		return
	}
	if actual.Algorithm != expected.Algorithm || actual.Value != expected.Value {
		t.Errorf("checksum = %#v, want %#v", actual, expected)
	}
}

func assertContentRefEqual(t *testing.T, actual, expected *loonfs.ContentRef) {
	t.Helper()
	if actual == nil || expected == nil {
		if actual != expected {
			t.Errorf("content_ref = %#v, want %#v", actual, expected)
		}
		return
	}
	if actual.ContentID != expected.ContentID || actual.Kind != expected.Kind || actual.SizeBytes != expected.SizeBytes {
		t.Errorf("content_ref = %#v, want %#v", actual, expected)
	}
	assertChecksumEqual(t, actual.Checksum, expected.Checksum)
}

func putPresigned(
	t *testing.T,
	access *loonfs.ObjectTransferAccess,
	payload []byte,
	requireETag bool,
) string {
	t.Helper()
	response := sendPresigned(t, access, http.MethodPut, bytes.NewReader(payload))
	defer response.Body.Close()
	assertSuccessfulTransfer(t, response)
	_, _ = io.Copy(io.Discard, response.Body)
	etag := response.Header.Get("ETag")
	if requireETag && etag == "" {
		t.Fatal("multipart PUT response has no ETag")
	}
	return etag
}

func getPresigned(t *testing.T, access *loonfs.ObjectTransferAccess) []byte {
	t.Helper()
	response := sendPresigned(t, access, http.MethodGet, nil)
	defer response.Body.Close()
	assertSuccessfulTransfer(t, response)
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read presigned GET response: %v", err)
	}
	return payload
}

func sendPresigned(
	t *testing.T,
	access *loonfs.ObjectTransferAccess,
	expectedMethod string,
	body io.Reader,
) *http.Response {
	t.Helper()
	if access == nil || access.PresignedURL == nil {
		t.Fatal("object transfer access is not a presigned URL")
	}
	presigned := access.PresignedURL
	if presigned.Method != expectedMethod {
		t.Fatalf("presigned method = %q, want %q", presigned.Method, expectedMethod)
	}
	request, err := http.NewRequestWithContext(context.Background(), expectedMethod, presigned.URL, body)
	if err != nil {
		t.Fatalf("build presigned request: %v", err)
	}
	for name, value := range presigned.Headers {
		if strings.EqualFold(name, "host") {
			request.Host = value
			continue
		}
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("send presigned request: %v", err)
	}
	return response
}

func assertSuccessfulTransfer(t *testing.T, response *http.Response) {
	t.Helper()
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return
	}
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4*1024))
	detail := strings.TrimSpace(string(body))
	if detail == "" {
		t.Fatalf("presigned request returned %s", response.Status)
	}
	t.Fatalf("presigned request returned %s: %s", response.Status, detail)
}

func uploadSessionStatus(t *testing.T, response *loonfs.UploadSessionResponse) *loonfs.UploadSessionStatus {
	t.Helper()
	if response == nil {
		t.Fatal("upload session response is nil")
	}
	if _, ok := response.GetExtraProperties()["status"]; !ok {
		t.Fatal("upload session response has no status")
	}
	data, err := json.Marshal(response.GetExtraProperties())
	if err != nil {
		t.Fatalf("encode upload status: %v", err)
	}
	var status loonfs.UploadSessionStatus
	if err := json.Unmarshal(data, &status); err != nil {
		t.Fatalf("decode upload status: %v", err)
	}
	return &status
}

func requireCompletedStatus(
	t *testing.T,
	response *loonfs.UploadSessionResponse,
) *loonfs.UploadSessionStatusCompleted {
	t.Helper()
	status := uploadSessionStatus(t, response)
	if status.Completed == nil || status.Completed.ContentRef == nil {
		t.Fatalf("upload status = %q, want completed", status.Status)
	}
	return status.Completed
}

func requireAbortedStatus(
	t *testing.T,
	response *loonfs.UploadSessionResponse,
) *loonfs.UploadSessionStatusAborted {
	t.Helper()
	status := uploadSessionStatus(t, response)
	if status.Aborted == nil {
		t.Fatalf("upload status = %q, want aborted", status.Status)
	}
	return status.Aborted
}

func requireFileProjection(t *testing.T, entry *loonfs.AuthoritativePathEntry) *loonfs.AuthoritativePathEntryFile {
	t.Helper()
	if entry == nil {
		t.Fatal("path entry is nil")
	}
	data, err := json.Marshal(entry.GetExtraProperties())
	if err != nil {
		t.Fatalf("encode path entry projection: %v", err)
	}
	var projection loonfs.AuthoritativePathEntryKind
	if err := json.Unmarshal(data, &projection); err != nil {
		t.Fatalf("decode path entry projection: %v", err)
	}
	if projection.File == nil {
		t.Fatalf("path entry kind = %q, want file", projection.InodeKind)
	}
	return projection.File
}

func commitCompletedFile(
	t *testing.T,
	sdk *client.Client,
	namespaceID string,
	path string,
	commitID string,
	actor *loonfs.ActorRef,
	contentRef *loonfs.ContentRef,
	contentToken *loonfs.ContentToken,
) *loonfs.CommitResponse {
	t.Helper()
	noReplace := loonfs.DestinationBehaviorNoReplace
	contentTokens := []*loonfs.ContentToken(nil)
	if contentToken != nil {
		contentTokens = []*loonfs.ContentToken{contentToken}
	}
	return applyCommit(t, sdk, &loonfs.CommitRequest{
		NamespaceID:   namespaceID,
		Actor:         actor,
		CommitID:      loonfs.CommitID(commitID),
		ContentTokens: contentTokens,
		Operations: []*loonfs.FilesystemOperation{
			{
				PutFile: &loonfs.FsOpPutFile{
					Behavior:   &noReplace,
					ContentRef: contentRef,
					Path:       loonfs.AbsolutePath(path),
				},
			},
		},
	})
}

func applyCommit(t *testing.T, sdk *client.Client, request *loonfs.CommitRequest) *loonfs.CommitResponse {
	t.Helper()
	response, err := sdk.Filesystem.ApplyCommit(context.Background(), request)
	if err != nil {
		t.Fatalf("apply commit %q: %v", request.CommitID, err)
	}
	return response
}

func statPath(t *testing.T, sdk *client.Client, namespaceID, path string) *loonfs.AuthoritativePathEntry {
	t.Helper()
	entry, err := sdk.Filesystem.StatPath(context.Background(), &loonfs.StatPathRequest{
		NamespaceID: namespaceID,
		Path:        path,
	})
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return entry
}

func getFile(t *testing.T, sdk *client.Client, namespaceID, path string) *transfers.GetFileResult {
	t.Helper()
	result, err := transfers.GetFile(context.Background(), sdk, transfers.GetFileInput{
		NamespaceID: loonfs.NamespaceID(namespaceID),
		Path:        loonfs.AbsolutePath(path),
	})
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	return result
}

func listPathEntries(
	t *testing.T,
	sdk *client.Client,
	namespaceID string,
	path string,
) []*loonfs.AuthoritativePathEntry {
	t.Helper()
	page, err := sdk.Filesystem.ListPathEntries(context.Background(), &loonfs.ListPathEntriesRequest{
		NamespaceID: namespaceID,
		Path:        path,
	})
	if err != nil {
		t.Fatalf("list %s: %v", path, err)
	}
	return page.Results
}

func listingContainsPath(entries []*loonfs.AuthoritativePathEntry, path string) bool {
	for _, entry := range entries {
		if entry != nil && string(entry.Path) == path {
			return true
		}
	}
	return false
}

func listChanges(t *testing.T, sdk *client.Client, namespaceID string) *loonfs.ChangesResponse {
	t.Helper()
	changes, err := sdk.Filesystem.ListChanges(context.Background(), &loonfs.ListChangesRequest{
		NamespaceID: namespaceID,
		AfterSeq:    loonfs.ChangeSeq(0),
	})
	if err != nil {
		t.Fatalf("list changes: %v", err)
	}
	return changes
}

func actorsEqual(left, right *loonfs.ActorRef) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.ID == right.ID && left.Kind == right.Kind
}

func createNamespace(t *testing.T, sdk *client.Client, namespaceID string) {
	t.Helper()
	_, err := sdk.Namespaces.CreateNamespace(
		context.Background(),
		&loonfs.CreateNamespaceRequest{NamespaceID: loonfs.NamespaceID(namespaceID)},
	)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
}

func applyCreateDirectory(
	t *testing.T,
	sdk *client.Client,
	namespaceID string,
	commitID string,
	actor *loonfs.ActorRef,
	path string,
) {
	t.Helper()
	_, err := sdk.Filesystem.ApplyCommit(
		context.Background(),
		createDirectoryCommit(namespaceID, commitID, actor, path, nil),
	)
	if err != nil {
		t.Fatalf("create directory %s: %v", path, err)
	}
}

func createDirectoryCommit(
	namespaceID string,
	commitID string,
	actor *loonfs.ActorRef,
	path string,
	message *string,
) *loonfs.CommitRequest {
	parents := false
	return &loonfs.CommitRequest{
		NamespaceID: namespaceID,
		Actor:       actor,
		CommitID:    loonfs.CommitID(commitID),
		Message:     message,
		Operations: []*loonfs.FilesystemOperation{
			{
				CreateDirectory: &loonfs.FsOpCreateDirectory{
					Parents: &parents,
					Path:    loonfs.AbsolutePath(path),
				},
			},
		},
	}
}

func instantiateDocumentPath(template, namespaceAlias string) string {
	segments := strings.Split(template, "/")
	for index, segment := range segments {
		if segment == "{namespace_alias}" {
			segments[index] = namespaceAlias
		} else if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			segments[index] = "x"
		}
	}
	return strings.Join(segments, "/")
}

func proxyTemplateForServer(template string) string {
	const serverNamespacePrefix = "/v0/namespaces/{namespace_id}"
	const proxyNamespaceAliasPrefix = "/v0/namespace-aliases/{namespace_alias}"
	if template == serverNamespacePrefix || strings.HasPrefix(template, serverNamespacePrefix+"/") {
		return proxyNamespaceAliasPrefix + strings.TrimPrefix(template, serverNamespacePrefix)
	}
	return template
}

// Every proxy route must reach the server.
// Every excluded server route must stop at the proxy.
func TestProxyForwardsEveryDocumentedRoute(t *testing.T) {
	proxyDocumentPath := os.Getenv("LOONFS_PROXY_DOCUMENT")
	if proxyDocumentPath == "" {
		t.Skip("run scripts/run-sdk-conformance.sh go")
	}
	serverDocumentPath := os.Getenv("LOONFS_SERVER_DOCUMENT")
	if serverDocumentPath == "" {
		t.Fatal("LOONFS_SERVER_DOCUMENT is not set")
	}
	casesDirectory := os.Getenv("LOONFS_CONFORMANCE_CASES")
	if casesDirectory == "" {
		t.Fatal("LOONFS_CONFORMANCE_CASES is not set")
	}
	testCases, err := loadCases(casesDirectory)
	if err != nil {
		t.Fatalf("load conformance cases: %v", err)
	}
	var proxyCase *conformanceCase
	for index := range testCases {
		if testCases[index].Name == "proxy" {
			proxyCase = &testCases[index]
			break
		}
	}
	if proxyCase == nil {
		t.Fatal("proxy conformance case is missing")
	}
	fixture, _ := decodeCaseValues[proxyCaseRequest, proxyExpected](t, *proxyCase)

	data, err := os.ReadFile(proxyDocumentPath)
	if err != nil {
		t.Fatalf("read proxy document: %v", err)
	}
	var proxyDocument struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(data, &proxyDocument); err != nil {
		t.Fatalf("decode proxy document: %v", err)
	}
	data, err = os.ReadFile(serverDocumentPath)
	if err != nil {
		t.Fatalf("read server document: %v", err)
	}
	var serverDocument struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(data, &serverDocument); err != nil {
		t.Fatalf("decode server document: %v", err)
	}

	var observedMu sync.Mutex
	observed := map[string]int{}
	stub := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		observedMu.Lock()
		observed[request.Method+" "+request.URL.Path]++
		observedMu.Unlock()
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte("{}"))
	}))
	defer stub.Close()

	proxyHandler, err := loonfsproxy.NewHandler(loonfsproxy.Config{
		ServerBaseURL: stub.URL,
		Token:         "recording-stub-token",
		NamespaceAliases: map[string]string{
			fixture.NamespaceAlias: fixture.NamespaceID,
		},
	})
	if err != nil {
		t.Fatalf("create proxy handler: %v", err)
	}
	proxyServer := httptest.NewServer(proxyHandler)
	defer proxyServer.Close()

	expected := map[string]int{}
	proxyRoutes := map[string]bool{}
	for template, item := range proxyDocument.Paths {
		for documentedMethod := range item {
			method := strings.ToUpper(documentedMethod)
			proxyRoutes[method+" "+template] = true
			path := instantiateDocumentPath(template, fixture.NamespaceAlias)
			forwardedTemplate := strings.Replace(
				template,
				"/v0/namespace-aliases/{namespace_alias}",
				"/v0/namespaces/"+fixture.NamespaceID,
				1,
			)
			forwardedPath := instantiateDocumentPath(forwardedTemplate, fixture.NamespaceAlias)
			expected[method+" "+forwardedPath]++

			forwardRequest, err := http.NewRequest(method, proxyServer.URL+path, nil)
			if err != nil {
				t.Fatalf("create request for %s %s: %v", method, path, err)
			}
			response, err := proxyServer.Client().Do(forwardRequest)
			if err != nil {
				t.Fatalf("send request for %s %s: %v", method, path, err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Errorf("%s %s status = %d, want %d", method, path, response.StatusCode, http.StatusOK)
			}
		}
	}

	observedMu.Lock()
	recorded := make(map[string]int, len(observed))
	for key, count := range observed {
		recorded[key] = count
	}
	observedMu.Unlock()
	for key, count := range expected {
		if recorded[key] != count {
			t.Errorf("stub observed %q %d times, want %d", key, recorded[key], count)
		}
	}
	for key, count := range recorded {
		if _, ok := expected[key]; !ok {
			t.Errorf("stub observed unexpected request %q %d times", key, count)
		}
	}

	observedBefore := 0
	for _, count := range recorded {
		observedBefore += count
	}
	for serverTemplate, item := range serverDocument.Paths {
		proxyTemplate := proxyTemplateForServer(serverTemplate)
		for documentedMethod := range item {
			method := strings.ToUpper(documentedMethod)
			if proxyRoutes[method+" "+proxyTemplate] {
				continue
			}
			path := instantiateDocumentPath(proxyTemplate, fixture.NamespaceAlias)
			request, err := http.NewRequest(method, proxyServer.URL+path, nil)
			if err != nil {
				t.Fatalf("create excluded request for %s %s: %v", method, path, err)
			}
			response, err := proxyServer.Client().Do(request)
			if err != nil {
				t.Fatalf("send excluded request for %s %s: %v", method, path, err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusNotFound {
				t.Errorf(
					"excluded route %s %s status = %d, want %d",
					method,
					path,
					response.StatusCode,
					http.StatusNotFound,
				)
			}
		}
	}
	observedMu.Lock()
	observedAfter := 0
	for _, count := range observed {
		observedAfter += count
	}
	observedMu.Unlock()
	if observedAfter != observedBefore {
		t.Errorf("stub observed %d requests for excluded server routes", observedAfter-observedBefore)
	}
}
