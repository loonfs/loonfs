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
	"github.com/loonfs/loonfs-sdk-go/option"
	loonfsproxy "github.com/loonfs/loonfs-sdk-go/proxy"
	"github.com/loonfs/loonfs-sdk-go/server"
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
	"children_by_inode",
	"commit_replay",
	"download",
	"end_to_end",
	"error_contract",
	"inode_mutations",
	"pagination",
	"proxy",
	"snapshots",
	"upload_abort",
	"upload_direct_put",
	"upload_multipart",
}

type harness struct {
	client          *server.Client
	unauthenticated *server.Client
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
		client: server.NewClient(
			option.WithBaseURL(baseURL),
			option.WithToken(token),
		),
		unauthenticated: server.NewClient(option.WithBaseURL(baseURL)),
		serverBaseURL:   baseURL,
		serverToken:     token,
	}

	for _, testCase := range cases {
		t.Run(testCase.Name, func(t *testing.T) {
			switch testCase.Name {
			case "children_by_inode":
				runChildrenByInode(t, h, testCase)
			case "inode_mutations":
				runInodeMutations(t, h, testCase)
			case "snapshots":
				runSnapshots(t, h, testCase)
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
	_, err := h.unauthenticated.Namespaces.Retrieve(
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
	first, err := h.client.Commits.Create(context.Background(), commit)
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	replayed, err := h.client.Commits.Create(context.Background(), commit)
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
	begin, err := h.client.Uploads.Create(context.Background(), &loonfs.CreateUploadRequest{
		NamespaceID: request.NamespaceID,
		Body: &loonfs.BeginUploadRequest{
			DirectPut: &loonfs.BeginUploadDirectPut{SizeBytes: &sizeBytes},
		},
	})
	if err != nil {
		t.Fatalf("begin direct PUT: %v", err)
	}
	if begin.DirectPut == nil {
		t.Fatalf("begin upload mode = %q, want %q", begin.Mode, expected.Mode)
	}
	directPut := begin.DirectPut
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
	completed, err := h.client.Uploads.Complete(context.Background(), &loonfs.CompleteUploadBody{
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
	begin, err := h.client.Uploads.Create(context.Background(), &loonfs.CreateUploadRequest{
		NamespaceID: request.NamespaceID,
		Body: &loonfs.BeginUploadRequest{
			DirectMultipart: &loonfs.BeginUploadDirectMultipart{
				PartSizeBytes: &request.PartSizeBytes,
			},
		},
	})
	if err != nil {
		t.Fatalf("begin multipart upload: %v", err)
	}
	if begin.DirectMultipart == nil {
		t.Fatalf("begin upload mode = %q, want %q", begin.Mode, expected.Mode)
	}
	multipart := begin.DirectMultipart
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
	signed, err := h.client.Uploads.SignParts(context.Background(), &loonfs.SignUploadPartsRequest{
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
	first, err := h.client.Uploads.Complete(context.Background(), completionRequest)
	if err != nil {
		t.Fatalf("complete multipart upload: %v", err)
	}
	firstStatus := requireCompletedStatus(t, first)
	replayed, err := h.client.Uploads.Complete(context.Background(), completionRequest)
	if err != nil {
		t.Fatalf("replay multipart completion: %v", err)
	}
	replayedStatus := requireCompletedStatus(t, replayed)
	if replayedStatus.NamespaceID != firstStatus.NamespaceID || replayedStatus.UploadID != firstStatus.UploadID || replayedStatus.Mode != firstStatus.Mode {
		t.Errorf("replayed upload identity = %#v, want %#v", replayedStatus, firstStatus)
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
	begin, err := h.client.Uploads.Create(context.Background(), &loonfs.CreateUploadRequest{
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
	first, err := h.client.Uploads.Abort(context.Background(), abort)
	if err != nil {
		t.Fatalf("abort upload: %v", err)
	}
	replayed, err := h.client.Uploads.Abort(context.Background(), abort)
	if err != nil {
		t.Fatalf("replay abort: %v", err)
	}
	if first.Status != expected.Status {
		t.Errorf("upload status = %q, want %q", first.Status, expected.Status)
	}
	firstStatus := requireAbortedStatus(t, first)
	replayedStatus := requireAbortedStatus(t, replayed)
	if replayedStatus.NamespaceID != firstStatus.NamespaceID || replayedStatus.UploadID != firstStatus.UploadID || replayedStatus.Mode != firstStatus.Mode {
		t.Errorf("replayed abort identity = %#v, want %#v", replayedStatus, firstStatus)
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
	grant, err := h.client.Files.CreateDownload(context.Background(), &loonfs.BeginDownloadRequest{
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
	uploadedInode := identityOf(stat).inodeID

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
				MovePath: &loonfs.FilesystemOperationMovePath{
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

	revisions, err := h.client.Files.ListRevisions(context.Background(), &loonfs.ListFileRevisionsRequest{
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
				DeletePath: &loonfs.FilesystemOperationDeletePath{
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

	trash, err := h.client.Trash.List(context.Background(), &loonfs.ListTrashRequest{
		NamespaceID: request.NamespaceID,
	})
	if err != nil {
		t.Fatalf("list end-to-end trash: %v", err)
	}
	var removedEntry *loonfs.TrashEntry
	for _, entry := range trash.Results {
		if string(entry.InodeID) == uploadedInode {
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

type childrenByInodeRequest struct {
	NamespaceID      string          `json:"namespace_id"`
	Directory        string          `json:"directory"`
	RenamedDirectory string          `json:"renamed_directory"`
	RenameCommitID   string          `json:"rename_commit_id"`
	Actor            loonfs.ActorRef `json:"actor"`
	EntryNames       []string        `json:"entry_names"`
	PageSize         int             `json:"page_size"`
	RenameAfterPage  int             `json:"rename_after_page"`
	ResumeAfterPage  int             `json:"resume_after_page"`
}

type childrenByInodeExpected struct {
	EntryCount       int   `json:"entry_count"`
	MinimumPageCount int   `json:"minimum_page_count"`
	InitialHeadSeq   int64 `json:"initial_head_seq"`
	RenamedHeadSeq   int64 `json:"renamed_head_seq"`
}

func runChildrenByInode(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[childrenByInodeRequest, childrenByInodeExpected](t, testCase)
	createNamespace(t, h.client, request.NamespaceID)
	applyCreateDirectory(
		t,
		h.client,
		request.NamespaceID,
		"conf-children-by-inode-directory",
		&request.Actor,
		request.Directory,
	)
	for index := len(request.EntryNames) - 1; index >= 0; index-- {
		name := request.EntryNames[index]
		applyCreateDirectory(
			t,
			h.client,
			request.NamespaceID,
			fmt.Sprintf("conf-children-by-inode-entry-%02d", index),
			&request.Actor,
			request.Directory+"/"+name,
		)
	}

	parentInodeID := identityOf(statPath(t, h.client, request.NamespaceID, request.Directory)).inodeID
	if parentInodeID == "" {
		t.Fatal("children-by-inode parent has no inode_id")
	}
	observed := make([]string, 0, len(request.EntryNames))
	pageCount := 0
	var savedCursor *string
	resumeOffset := -1
	ctx := context.Background()
	page, err := h.client.Inodes.ListChildren(
		ctx,
		&loonfs.ListInodeChildrenRequest{
			NamespaceID: request.NamespaceID,
			InodeID:     parentInodeID,
			Limit:       &request.PageSize,
		},
	)
	if err != nil {
		t.Fatalf("list first children-by-inode page: %v", err)
	}
	for {
		if page.Response == nil {
			t.Fatal("children-by-inode page has no response")
		}
		pageCount++
		if string(page.Response.NamespaceID) != request.NamespaceID {
			t.Errorf("page %d namespace_id = %q, want %q", pageCount, page.Response.NamespaceID, request.NamespaceID)
		}
		if page.Response.ParentInodeID != parentInodeID {
			t.Errorf("page %d parent_inode_id = %q, want %q", pageCount, page.Response.ParentInodeID, parentInodeID)
		}
		expectedHeadSeq := expected.RenamedHeadSeq
		if pageCount <= request.RenameAfterPage {
			expectedHeadSeq = expected.InitialHeadSeq
		}
		if int64(page.Response.HeadSeq) != expectedHeadSeq {
			t.Errorf("page %d head_seq = %d, want %d", pageCount, page.Response.HeadSeq, expectedHeadSeq)
		}
		observed = append(observed, listedNames(t, page.Results)...)
		if pageCount == request.ResumeAfterPage {
			if page.Response.NextCursor != nil {
				value := *page.Response.NextCursor
				savedCursor = &value
			}
			resumeOffset = len(observed)
		}
		if pageCount == request.RenameAfterPage {
			noReplace := loonfs.DestinationBehaviorNoReplace
			renamed := applyCommit(t, h.client, &loonfs.CommitRequest{
				NamespaceID: request.NamespaceID,
				Actor:       &request.Actor,
				CommitID:    loonfs.CommitID(request.RenameCommitID),
				Operations: []*loonfs.FilesystemOperation{
					{
						MovePath: &loonfs.FilesystemOperationMovePath{
							Behavior: &noReplace,
							FromPath: loonfs.AbsolutePath(request.Directory),
							ToPath:   loonfs.AbsolutePath(request.RenamedDirectory),
						},
					},
				},
			})
			if int64(renamed.CommittedSeq) != expected.RenamedHeadSeq {
				t.Errorf("rename committed_seq = %d, want %d", renamed.CommittedSeq, expected.RenamedHeadSeq)
			}
			renamedInodeID := identityOf(statPath(
				t,
				h.client,
				request.NamespaceID,
				request.RenamedDirectory,
			)).inodeID
			if renamedInodeID != parentInodeID {
				t.Errorf("renamed parent inode_id = %q, want %q", renamedInodeID, parentInodeID)
			}
		}
		if page.Response.NextCursor == nil {
			break
		}
		page, err = page.GetNextPage(ctx)
		if err != nil {
			t.Fatalf("list next children-by-inode page: %v", err)
		}
	}
	if len(observed) != expected.EntryCount {
		t.Errorf("entry count = %d, want %d", len(observed), expected.EntryCount)
	}
	if pageCount < expected.MinimumPageCount {
		t.Errorf("page count = %d, want at least %d", pageCount, expected.MinimumPageCount)
	}
	if savedCursor == nil {
		t.Fatal("resume cursor was not recorded")
	}
	if resumeOffset < 0 {
		t.Fatal("resume position was not recorded")
	}

	resumed := make([]string, 0, len(request.EntryNames)-resumeOffset)
	page, err = h.client.Inodes.ListChildren(
		ctx,
		&loonfs.ListInodeChildrenRequest{
			NamespaceID: request.NamespaceID,
			InodeID:     parentInodeID,
			Limit:       &request.PageSize,
			Cursor:      savedCursor,
		},
	)
	if err != nil {
		t.Fatalf("resume children-by-inode pagination: %v", err)
	}
	for {
		if page.Response == nil {
			t.Fatal("resumed children-by-inode page has no response")
		}
		if string(page.Response.NamespaceID) != request.NamespaceID {
			t.Errorf("resumed namespace_id = %q, want %q", page.Response.NamespaceID, request.NamespaceID)
		}
		if page.Response.ParentInodeID != parentInodeID {
			t.Errorf("resumed parent_inode_id = %q, want %q", page.Response.ParentInodeID, parentInodeID)
		}
		if int64(page.Response.HeadSeq) != expected.RenamedHeadSeq {
			t.Errorf("resumed head_seq = %d, want %d", page.Response.HeadSeq, expected.RenamedHeadSeq)
		}
		resumed = append(resumed, listedNames(t, page.Results)...)
		if page.Response.NextCursor == nil {
			break
		}
		page, err = page.GetNextPage(ctx)
		if err != nil {
			t.Fatalf("resume next children-by-inode page: %v", err)
		}
	}
	if err := validatePageWalk(request.EntryNames, observed, resumeOffset, resumed); err != nil {
		t.Fatalf("children-by-inode pagination invariants: %v", err)
	}
}

type inodeMutationsRequest struct {
	NamespaceID                string          `json:"namespace_id"`
	Directory                  string          `json:"directory"`
	Actor                      loonfs.ActorRef `json:"actor"`
	PathDirectoryName          string          `json:"path_directory_name"`
	PathFileName               string          `json:"path_file_name"`
	InodeDirectoryName         string          `json:"inode_directory_name"`
	InodeFileName              string          `json:"inode_file_name"`
	RenamedFileName            string          `json:"renamed_file_name"`
	MovedFileName              string          `json:"moved_file_name"`
	ContentUTF8                string          `json:"content_utf8"`
	RevisedContentUTF8         string          `json:"revised_content_utf8"`
	MalformedBindingGeneration string          `json:"malformed_binding_generation"`
}

type inodeMutationsExpected struct {
	EntryNames                 []string            `json:"entry_names"`
	RevisedRevisionNo          int64               `json:"revised_revision_no"`
	MovedCommittedSeq          int64               `json:"moved_committed_seq"`
	DeletedCommittedSeq        int64               `json:"deleted_committed_seq"`
	StaleBindingGeneration     errorStatusExpected `json:"stale_binding_generation"`
	MalformedBindingGeneration errorStatusExpected `json:"malformed_binding_generation"`
}

func runInodeMutations(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[inodeMutationsRequest, inodeMutationsExpected](t, testCase)
	childPath := func(name string) string { return request.Directory + "/" + name }
	createNamespace(t, h.client, request.NamespaceID)
	applyCreateDirectory(
		t,
		h.client,
		request.NamespaceID,
		"conf-inode-mutations-directory",
		&request.Actor,
		request.Directory,
	)
	applyCreateDirectory(
		t,
		h.client,
		request.NamespaceID,
		"conf-inode-mutations-path-directory",
		&request.Actor,
		childPath(request.PathDirectoryName),
	)
	if _, err := transfers.PutFile(context.Background(), h.client, transfers.PutFileInput{
		NamespaceID: loonfs.NamespaceID(request.NamespaceID),
		Path:        loonfs.AbsolutePath(childPath(request.PathFileName)),
		Bytes:       []byte(request.ContentUTF8),
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID("conf-inode-mutations-path-file"),
	}); err != nil {
		t.Fatalf("put path-addressed file: %v", err)
	}

	parentInodeID := identityOf(statPath(t, h.client, request.NamespaceID, request.Directory)).inodeID
	applyCommit(t, h.client, &loonfs.CommitRequest{
		NamespaceID: request.NamespaceID,
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID("conf-inode-mutations-inode-directory"),
		Operations: []*loonfs.FilesystemOperation{
			{
				CreateDirectoryByInode: &loonfs.FilesystemOperationCreateDirectoryByInode{
					ParentInodeID: parentInodeID,
					DisplayName:   request.InodeDirectoryName,
				},
			},
		},
	})
	contentRef, contentToken := stageContent(t, h.client, request.NamespaceID, []byte(request.ContentUTF8))
	applyCommit(t, h.client, &loonfs.CommitRequest{
		NamespaceID:   request.NamespaceID,
		Actor:         &request.Actor,
		CommitID:      loonfs.CommitID("conf-inode-mutations-inode-file"),
		ContentTokens: contentTokens(contentToken),
		Operations: []*loonfs.FilesystemOperation{
			{
				PutFileByInode: &loonfs.FilesystemOperationPutFileByInode{
					ParentInodeID: parentInodeID,
					DisplayName:   request.InodeFileName,
					ContentRef:    contentRef,
				},
			},
		},
	})

	listing := listPathEntries(t, h.client, request.NamespaceID, request.Directory)
	names := make([]string, 0, len(listing))
	generations := make(map[string]struct{}, len(listing))
	for _, entry := range listing {
		identity := identityOf(entry)
		names = append(names, identity.displayName)
		if identity.bindingGeneration == "" {
			t.Fatalf("listed entry %q has no binding_generation", identity.displayName)
		}
		generations[identity.bindingGeneration] = struct{}{}
	}
	if !equalStrings(names, expected.EntryNames) {
		t.Fatalf("listed names = %v, want %v", names, expected.EntryNames)
	}
	if len(generations) != len(listing) {
		t.Errorf("listing reported %d distinct binding generations, want %d", len(generations), len(listing))
	}
	entryNamed := func(name string) *loonfs.PathEntry {
		for _, entry := range listing {
			if identityOf(entry).displayName == name {
				return entry
			}
		}
		t.Fatalf("listed entry %q is missing", name)
		return nil
	}
	inodeDirectory := entryNamed(request.InodeDirectoryName)
	if inodeDirectory.InodeKind != entryNamed(request.PathDirectoryName).InodeKind {
		t.Errorf("inode-created directory inode_kind = %q, want the path-created kind", inodeDirectory.InodeKind)
	}
	inodeFile := requireFileProjection(t, entryNamed(request.InodeFileName))
	pathFile := requireFileProjection(t, entryNamed(request.PathFileName))
	if inodeFile.SizeBytes != pathFile.SizeBytes {
		t.Errorf("inode-created file size_bytes = %d, want %d", inodeFile.SizeBytes, pathFile.SizeBytes)
	}
	if optionalString(inodeFile.ParentInodeID) != parentInodeID {
		t.Errorf("inode-created file parent_inode_id = %q, want %q", optionalString(inodeFile.ParentInodeID), parentInodeID)
	}

	contentRef, contentToken = stageContent(t, h.client, request.NamespaceID, []byte(request.RevisedContentUTF8))
	applyCommit(t, h.client, &loonfs.CommitRequest{
		NamespaceID:   request.NamespaceID,
		Actor:         &request.Actor,
		CommitID:      loonfs.CommitID("conf-inode-mutations-revision"),
		ContentTokens: contentTokens(contentToken),
		Operations: []*loonfs.FilesystemOperation{
			{
				PutFileRevisionByInode: &loonfs.FilesystemOperationPutFileRevisionByInode{
					InodeID:            inodeFile.InodeID,
					ContentRef:         contentRef,
					ExpectedRevisionNo: inodeFile.RevisionNo,
				},
			},
		},
	})
	revised := requireFileProjection(
		t,
		statPath(t, h.client, request.NamespaceID, childPath(request.InodeFileName)),
	)
	if revised.RevisionNo != expected.RevisedRevisionNo {
		t.Errorf("revised revision_no = %d, want %d", revised.RevisionNo, expected.RevisedRevisionNo)
	}
	readback := getFile(t, h.client, request.NamespaceID, childPath(request.InodeFileName))
	if !bytes.Equal(readback.Bytes, []byte(request.RevisedContentUTF8)) {
		t.Error("inode-addressed revision readback did not match the revised payload")
	}
	staleGeneration := optionalString(revised.BindingGeneration)

	noReplace := loonfs.DestinationBehaviorNoReplace
	applyCommit(t, h.client, &loonfs.CommitRequest{
		NamespaceID: request.NamespaceID,
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID("conf-inode-mutations-rename"),
		Operations: []*loonfs.FilesystemOperation{
			{
				MovePath: &loonfs.FilesystemOperationMovePath{
					Behavior: &noReplace,
					FromPath: loonfs.AbsolutePath(childPath(request.InodeFileName)),
					ToPath:   loonfs.AbsolutePath(childPath(request.RenamedFileName)),
				},
			},
		},
	})
	moveByInode := func(commitID string, generation string) *loonfs.CommitRequest {
		return &loonfs.CommitRequest{
			NamespaceID: request.NamespaceID,
			Actor:       &request.Actor,
			CommitID:    loonfs.CommitID(commitID),
			Operations: []*loonfs.FilesystemOperation{
				{
					MoveByInode: &loonfs.FilesystemOperationMoveByInode{
						Behavior:                  &noReplace,
						InodeID:                   inodeFile.InodeID,
						ExpectedBindingGeneration: generation,
						ToParentInodeID:           identityOf(inodeDirectory).inodeID,
						ToDisplayName:             request.MovedFileName,
					},
				},
			},
		}
	}

	_, err := h.client.Commits.Create(
		context.Background(),
		moveByInode("conf-inode-mutations-stale-move", staleGeneration),
	)
	var conflict *loonfs.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected ConflictError, found %T: %v", err, err)
	}
	if conflict.StatusCode != expected.StaleBindingGeneration.Status {
		t.Errorf("stale move status = %d, want %d", conflict.StatusCode, expected.StaleBindingGeneration.Status)
	}
	if conflict.Body == nil || conflict.Body.Code != expected.StaleBindingGeneration.Code {
		t.Errorf("stale move body = %#v, want code %q", conflict.Body, expected.StaleBindingGeneration.Code)
	}
	_, err = h.client.Commits.Create(
		context.Background(),
		moveByInode("conf-inode-mutations-malformed-move", request.MalformedBindingGeneration),
	)
	var badRequest *loonfs.BadRequestError
	if !errors.As(err, &badRequest) {
		t.Fatalf("expected BadRequestError, found %T: %v", err, err)
	}
	if badRequest.StatusCode != expected.MalformedBindingGeneration.Status {
		t.Errorf("malformed move status = %d, want %d", badRequest.StatusCode, expected.MalformedBindingGeneration.Status)
	}
	if badRequest.Body == nil || badRequest.Body.Code != expected.MalformedBindingGeneration.Code {
		t.Errorf("malformed move body = %#v, want code %q", badRequest.Body, expected.MalformedBindingGeneration.Code)
	}

	freshGeneration := identityOf(statPath(
		t,
		h.client,
		request.NamespaceID,
		childPath(request.RenamedFileName),
	)).bindingGeneration
	moved := applyCommit(t, h.client, moveByInode("conf-inode-mutations-move", freshGeneration))
	if int64(moved.CommittedSeq) != expected.MovedCommittedSeq {
		t.Errorf("move committed_seq = %d, want %d", moved.CommittedSeq, expected.MovedCommittedSeq)
	}
	movedEntry := identityOf(statPath(
		t,
		h.client,
		request.NamespaceID,
		childPath(request.InodeDirectoryName)+"/"+request.MovedFileName,
	))
	if movedEntry.inodeID != inodeFile.InodeID {
		t.Errorf("moved inode_id = %q, want %q", movedEntry.inodeID, inodeFile.InodeID)
	}
	if movedEntry.bindingGeneration == freshGeneration {
		t.Error("move did not mint a new binding generation")
	}

	limit := 1
	feed, err := h.client.Changes.List(context.Background(), &loonfs.ListChangesRequest{
		NamespaceID: request.NamespaceID,
		AfterSeq:    loonfs.ChangeSeq(expected.MovedCommittedSeq - 1),
		Limit:       &limit,
	})
	if err != nil {
		t.Fatalf("list inode-mutations changes: %v", err)
	}
	if len(feed.Changes) != 1 || len(feed.Changes[0].Events) != 1 || feed.Changes[0].Events[0].Moved == nil {
		t.Fatalf("expected one moved event, found %#v", feed.Changes)
	}
	movedEvent := feed.Changes[0].Events[0].Moved
	if movedEvent.BindingGeneration != movedEntry.bindingGeneration {
		t.Errorf(
			"moved event binding_generation = %q, want %q",
			movedEvent.BindingGeneration,
			movedEntry.bindingGeneration,
		)
	}

	nonRecursive := loonfs.DeleteDirectoryBehaviorNonRecursive
	deleted := applyCommit(t, h.client, &loonfs.CommitRequest{
		NamespaceID: request.NamespaceID,
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID("conf-inode-mutations-delete"),
		Operations: []*loonfs.FilesystemOperation{
			{
				DeleteByInode: &loonfs.FilesystemOperationDeleteByInode{
					Behavior:                  &nonRecursive,
					InodeID:                   inodeFile.InodeID,
					ExpectedBindingGeneration: movedEntry.bindingGeneration,
				},
			},
		},
	})
	if int64(deleted.CommittedSeq) != expected.DeletedCommittedSeq {
		t.Errorf("delete committed_seq = %d, want %d", deleted.CommittedSeq, expected.DeletedCommittedSeq)
	}
}

func stageContent(
	t *testing.T,
	sdk *server.Client,
	namespaceID string,
	payload []byte,
) (*loonfs.ContentRef, *loonfs.ContentToken) {
	t.Helper()
	begin, err := sdk.Uploads.Create(context.Background(), &loonfs.CreateUploadRequest{
		NamespaceID: namespaceID,
		Body: &loonfs.BeginUploadRequest{
			ServiceProxied: &loonfs.BeginUploadServiceProxied{},
		},
	})
	if err != nil {
		t.Fatalf("begin service-proxied upload: %v", err)
	}
	if begin.ServiceProxied == nil {
		t.Fatalf("begin upload mode = %q, want service_proxied", begin.Mode)
	}
	uploadID := string(begin.ServiceProxied.UploadID)
	if _, err := sdk.Uploads.PutContent(
		context.Background(),
		namespaceID,
		uploadID,
		bytes.NewReader(payload),
	); err != nil {
		t.Fatalf("stage upload content: %v", err)
	}
	completed, err := sdk.Uploads.Complete(context.Background(), &loonfs.CompleteUploadBody{
		NamespaceID: namespaceID,
		UploadID:    uploadID,
		Body: &loonfs.CompleteUploadRequest{
			ServiceProxied: &loonfs.CompleteUploadServiceProxied{},
		},
	})
	if err != nil {
		t.Fatalf("complete service-proxied upload: %v", err)
	}
	status := requireCompletedStatus(t, completed)
	return status.ContentRef, status.ContentToken
}

func contentTokens(token *loonfs.ContentToken) []*loonfs.ContentToken {
	if token == nil {
		return nil
	}
	return []*loonfs.ContentToken{token}
}

type snapshotsRequest struct {
	NamespaceID         string          `json:"namespace_id"`
	Directory           string          `json:"directory"`
	Actor               loonfs.ActorRef `json:"actor"`
	SnapshotName        string          `json:"snapshot_name"`
	ReplacedFileName    string          `json:"replaced_file_name"`
	DeletedFileName     string          `json:"deleted_file_name"`
	AddedFileName       string          `json:"added_file_name"`
	CapturedContentUTF8 string          `json:"captured_content_utf8"`
	CurrentContentUTF8  string          `json:"current_content_utf8"`
	DeletedContentUTF8  string          `json:"deleted_content_utf8"`
	AddedContentUTF8    string          `json:"added_content_utf8"`
	CreateTTLMs         int64           `json:"create_ttl_ms"`
	ExtendTTLMs         int64           `json:"extend_ttl_ms"`
	UnknownSnapshotID   string          `json:"unknown_snapshot_id"`
}

type snapshotsExpected struct {
	SnapshotHeadSeq      int64               `json:"snapshot_head_seq"`
	CapturedRevisionNo   int64               `json:"captured_revision_no"`
	CapturedEntryNames   []string            `json:"captured_entry_names"`
	CurrentRevisionNo    int64               `json:"current_revision_no"`
	CurrentEntryNames    []string            `json:"current_entry_names"`
	SnapshotChangeSeqs   []int64             `json:"snapshot_change_seqs"`
	SnapshotGone         errorStatusExpected `json:"snapshot_gone"`
	SnapshotNotFound     errorStatusExpected `json:"snapshot_not_found"`
	RevisionWithSnapshot errorStatusExpected `json:"revision_with_snapshot"`
	ZeroTtl              errorStatusExpected `json:"zero_ttl"`
}

func runSnapshots(t *testing.T, h *harness, testCase conformanceCase) {
	t.Helper()
	request, expected := decodeCaseValues[snapshotsRequest, snapshotsExpected](t, testCase)
	ctx := context.Background()
	childPath := func(name string) string { return request.Directory + "/" + name }
	createNamespace(t, h.client, request.NamespaceID)

	applyCommit(
		t,
		h.client,
		createDirectoryCommit(
			request.NamespaceID,
			"conf-snapshots-create-directory",
			&request.Actor,
			request.Directory,
			nil,
		),
	)
	_, err := transfers.PutFile(ctx, h.client, transfers.PutFileInput{
		NamespaceID: loonfs.NamespaceID(request.NamespaceID),
		Path:        loonfs.AbsolutePath(childPath(request.ReplacedFileName)),
		Bytes:       []byte(request.CapturedContentUTF8),
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID("conf-snapshots-create-replaced"),
	})
	if err != nil {
		t.Fatalf("create replaced snapshot file: %v", err)
	}
	_, err = transfers.PutFile(ctx, h.client, transfers.PutFileInput{
		NamespaceID: loonfs.NamespaceID(request.NamespaceID),
		Path:        loonfs.AbsolutePath(childPath(request.DeletedFileName)),
		Bytes:       []byte(request.DeletedContentUTF8),
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID("conf-snapshots-create-deleted"),
	})
	if err != nil {
		t.Fatalf("create deleted snapshot file: %v", err)
	}

	snapshot, err := h.client.Snapshots.Create(ctx, &loonfs.CreateSnapshotRequest{
		NamespaceID: request.NamespaceID,
		Name:        request.SnapshotName,
		TTLMs:       request.CreateTTLMs,
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if string(snapshot.NamespaceID) != request.NamespaceID {
		t.Errorf("snapshot namespace_id = %q, want %q", snapshot.NamespaceID, request.NamespaceID)
	}
	if snapshot.Name != request.SnapshotName {
		t.Errorf("snapshot name = %q, want %q", snapshot.Name, request.SnapshotName)
	}
	if int64(snapshot.HeadSeq) != expected.SnapshotHeadSeq {
		t.Errorf("snapshot head_seq = %d, want %d", snapshot.HeadSeq, expected.SnapshotHeadSeq)
	}
	if snapshot.ExpiresAtMs <= snapshot.CreatedAtMs {
		t.Errorf("snapshot expires_at_ms = %d, want greater than created_at_ms %d", snapshot.ExpiresAtMs, snapshot.CreatedAtMs)
	}

	replace := loonfs.DestinationBehaviorReplace
	_, err = transfers.PutFile(ctx, h.client, transfers.PutFileInput{
		NamespaceID: loonfs.NamespaceID(request.NamespaceID),
		Path:        loonfs.AbsolutePath(childPath(request.ReplacedFileName)),
		Bytes:       []byte(request.CurrentContentUTF8),
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID("conf-snapshots-replace-file"),
		Behavior:    replace,
	})
	if err != nil {
		t.Fatalf("replace snapshot file: %v", err)
	}
	_, err = transfers.PutFile(ctx, h.client, transfers.PutFileInput{
		NamespaceID: loonfs.NamespaceID(request.NamespaceID),
		Path:        loonfs.AbsolutePath(childPath(request.AddedFileName)),
		Bytes:       []byte(request.AddedContentUTF8),
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID("conf-snapshots-add-file"),
	})
	if err != nil {
		t.Fatalf("add file after snapshot: %v", err)
	}
	nonRecursive := loonfs.DeleteDirectoryBehaviorNonRecursive
	applyCommit(t, h.client, &loonfs.CommitRequest{
		NamespaceID: request.NamespaceID,
		Actor:       &request.Actor,
		CommitID:    loonfs.CommitID("conf-snapshots-delete-file"),
		Operations: []*loonfs.FilesystemOperation{
			{
				DeletePath: &loonfs.FilesystemOperationDeletePath{
					Behavior: &nonRecursive,
					Path:     loonfs.AbsolutePath(childPath(request.DeletedFileName)),
				},
			},
		},
	})

	snapshotID := snapshot.SnapshotID
	capturedEntry, err := h.client.Files.Retrieve(ctx, &loonfs.GetPathEntryRequest{
		NamespaceID: request.NamespaceID,
		Path:        childPath(request.ReplacedFileName),
		SnapshotID:  &snapshotID,
	})
	if err != nil {
		t.Fatalf("stat snapshot file: %v", err)
	}
	if revision := requireFileProjection(t, capturedEntry).RevisionNo; int64(revision) != expected.CapturedRevisionNo {
		t.Errorf("captured revision_no = %d, want %d", revision, expected.CapturedRevisionNo)
	}
	currentEntry := statPath(t, h.client, request.NamespaceID, childPath(request.ReplacedFileName))
	if revision := requireFileProjection(t, currentEntry).RevisionNo; int64(revision) != expected.CurrentRevisionNo {
		t.Errorf("current revision_no = %d, want %d", revision, expected.CurrentRevisionNo)
	}

	capturedListing, err := h.client.Files.List(ctx, &loonfs.ListPathEntriesRequest{
		NamespaceID: request.NamespaceID,
		Path:        request.Directory,
		SnapshotID:  &snapshotID,
	})
	if err != nil {
		t.Fatalf("list snapshot directory: %v", err)
	}
	if capturedListing.Response == nil {
		t.Fatal("snapshot directory page has no response")
	}
	if int64(capturedListing.Response.HeadSeq) != expected.SnapshotHeadSeq {
		t.Errorf("snapshot listing head_seq = %d, want %d", capturedListing.Response.HeadSeq, expected.SnapshotHeadSeq)
	}
	if names := listedNames(t, capturedListing.Results); !equalStrings(names, expected.CapturedEntryNames) {
		t.Errorf("snapshot listing names = %v, want %v", names, expected.CapturedEntryNames)
	}
	currentListing := listPathEntries(t, h.client, request.NamespaceID, request.Directory)
	if names := listedNames(t, currentListing); !equalStrings(names, expected.CurrentEntryNames) {
		t.Errorf("current listing names = %v, want %v", names, expected.CurrentEntryNames)
	}

	capturedBytes := readSDKFileBytes(t, h.client, &loonfs.GetFileBytesRequest{
		NamespaceID: request.NamespaceID,
		Path:        childPath(request.ReplacedFileName),
		SnapshotID:  &snapshotID,
	}, "read snapshot content")
	if !bytes.Equal(capturedBytes, []byte(request.CapturedContentUTF8)) {
		t.Error("snapshot content did not match the captured payload")
	}
	currentBytes := readSDKFileBytes(t, h.client, &loonfs.GetFileBytesRequest{
		NamespaceID: request.NamespaceID,
		Path:        childPath(request.ReplacedFileName),
	}, "read current content")
	if !bytes.Equal(currentBytes, []byte(request.CurrentContentUTF8)) {
		t.Error("current content did not match the replacement payload")
	}

	limit := 100
	feed, err := h.client.Changes.List(ctx, &loonfs.ListChangesRequest{
		NamespaceID: request.NamespaceID,
		AfterSeq:    loonfs.ChangeSeq(0),
		Limit:       &limit,
		SnapshotID:  &snapshotID,
	})
	if err != nil {
		t.Fatalf("list snapshot changes: %v", err)
	}
	if int64(feed.ThroughSeq) != expected.SnapshotHeadSeq {
		t.Errorf("snapshot changes through_seq = %d, want %d", feed.ThroughSeq, expected.SnapshotHeadSeq)
	}
	if feed.NextAfterSeq != nil {
		t.Errorf("snapshot changes next_after_seq = %d, want nil", *feed.NextAfterSeq)
	}
	changeSeqs := make([]int64, 0, len(feed.Changes))
	for _, change := range feed.Changes {
		changeSeqs = append(changeSeqs, int64(change.CommittedSeq))
	}
	if !equalInt64s(changeSeqs, expected.SnapshotChangeSeqs) {
		t.Errorf("snapshot change seqs = %v, want %v", changeSeqs, expected.SnapshotChangeSeqs)
	}

	extended, err := h.client.Snapshots.Extend(ctx, &loonfs.ExtendSnapshotRequest{
		NamespaceID: request.NamespaceID,
		SnapshotID:  snapshotID,
		TTLMs:       request.ExtendTTLMs,
	})
	if err != nil {
		t.Fatalf("extend snapshot: %v", err)
	}
	if extended.SnapshotID != snapshotID || int64(extended.HeadSeq) != expected.SnapshotHeadSeq || extended.Name != request.SnapshotName {
		t.Errorf("extended snapshot = %#v, want the created snapshot", extended)
	}
	if extended.ExpiresAtMs <= snapshot.ExpiresAtMs {
		t.Errorf("extended expires_at_ms = %d, want greater than %d", extended.ExpiresAtMs, snapshot.ExpiresAtMs)
	}

	listed, err := h.client.Snapshots.List(ctx, &loonfs.ListSnapshotsRequest{
		NamespaceID: request.NamespaceID,
	})
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if listed.Response == nil {
		t.Fatal("snapshot page has no response")
	}
	if string(listed.Response.NamespaceID) != request.NamespaceID || listed.Response.NextCursor != nil {
		t.Errorf("snapshot page response = %#v", listed.Response)
	}
	if len(listed.Results) != 1 || listed.Results[0].SnapshotID != snapshotID {
		t.Errorf("listed snapshots = %#v, want only %q", listed.Results, snapshotID)
	}

	releaseRequest := &loonfs.ReleaseSnapshotRequest{
		NamespaceID: request.NamespaceID,
		SnapshotID:  snapshotID,
	}
	for _, label := range []string{"release snapshot", "release snapshot again"} {
		released, releaseErr := h.client.Snapshots.Release(ctx, releaseRequest)
		if releaseErr != nil {
			t.Fatalf("%s: %v", label, releaseErr)
		}
		if string(released.NamespaceID) != request.NamespaceID || released.SnapshotID != snapshotID {
			t.Errorf("%s response = %#v", label, released)
		}
	}

	_, err = h.client.Files.Retrieve(ctx, &loonfs.GetPathEntryRequest{
		NamespaceID: request.NamespaceID,
		Path:        childPath(request.ReplacedFileName),
		SnapshotID:  &snapshotID,
	})
	assertGoneError(t, err, expected.SnapshotGone)
	_, err = h.client.Snapshots.Extend(ctx, &loonfs.ExtendSnapshotRequest{
		NamespaceID: request.NamespaceID,
		SnapshotID:  snapshotID,
		TTLMs:       request.ExtendTTLMs,
	})
	assertGoneError(t, err, expected.SnapshotGone)

	unknownSnapshotID := loonfs.CheckpointID(request.UnknownSnapshotID)
	_, err = h.client.Files.Retrieve(ctx, &loonfs.GetPathEntryRequest{
		NamespaceID: request.NamespaceID,
		Path:        childPath(request.ReplacedFileName),
		SnapshotID:  &unknownSnapshotID,
	})
	var notFound *loonfs.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected NotFoundError, found %T: %v", err, err)
	}
	if notFound.StatusCode != expected.SnapshotNotFound.Status || notFound.Body == nil || notFound.Body.Code != expected.SnapshotNotFound.Code {
		t.Errorf("unknown snapshot error = %#v, want %#v", notFound, expected.SnapshotNotFound)
	}

	revision := loonfs.RevisionNo(expected.CapturedRevisionNo)
	_, err = h.client.Files.Content(ctx, &loonfs.GetFileBytesRequest{
		NamespaceID: request.NamespaceID,
		Path:        childPath(request.ReplacedFileName),
		RevisionNo:  &revision,
		SnapshotID:  &snapshotID,
	})
	assertBadRequestError(t, err, expected.RevisionWithSnapshot)
	_, err = h.client.Snapshots.Create(ctx, &loonfs.CreateSnapshotRequest{
		NamespaceID: request.NamespaceID,
		Name:        request.SnapshotName,
		TTLMs:       0,
	})
	assertBadRequestError(t, err, expected.ZeroTtl)
}

func readSDKFileBytes(
	t *testing.T,
	sdk *server.Client,
	request *loonfs.GetFileBytesRequest,
	label string,
) []byte {
	t.Helper()
	reader, err := sdk.Files.Content(context.Background(), request)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	return content
}

func assertGoneError(t *testing.T, err error, expected errorStatusExpected) {
	t.Helper()
	var gone *loonfs.GoneError
	if !errors.As(err, &gone) {
		t.Fatalf("expected GoneError, found %T: %v", err, err)
	}
	if gone.StatusCode != expected.Status || gone.Body == nil || gone.Body.Code != expected.Code {
		t.Errorf("gone error = %#v, want %#v", gone, expected)
	}
}

func assertBadRequestError(t *testing.T, err error, expected errorStatusExpected) {
	t.Helper()
	var badRequest *loonfs.BadRequestError
	if !errors.As(err, &badRequest) {
		t.Fatalf("expected BadRequestError, found %T: %v", err, err)
	}
	if badRequest.StatusCode != expected.Status || badRequest.Body == nil || badRequest.Body.Code != expected.Code {
		t.Errorf("bad request error = %#v, want %#v", badRequest, expected)
	}
}

func equalInt64s(left, right []int64) bool {
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
	page, err := h.client.Files.List(
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
	page, err = h.client.Files.List(
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

func listedNames(t *testing.T, entries []*loonfs.PathEntry) []string {
	t.Helper()
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := identityOf(entry).displayName
		if name == "" {
			t.Fatal("listed entry has no display_name")
		}
		names = append(names, name)
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
	mkdir := proxyCreateCommit(t, proxyServer.Client(), namespaceAliasBaseURL, createDirectoryCommit(
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
	proxiedBegin := proxyCreateUpload(t, proxyServer.Client(), namespaceAliasBaseURL, &loonfs.BeginUploadRequest{
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
	directBegin := proxyCreateUpload(t, proxyServer.Client(), namespaceAliasBaseURL, &loonfs.BeginUploadRequest{
		DirectPut: &loonfs.BeginUploadDirectPut{SizeBytes: &sizeBytes},
	})
	if directBegin.DirectPut == nil {
		t.Fatalf("proxy begin upload mode = %q, want direct_put", directBegin.Mode)
	}
	directUploadID := string(directBegin.DirectPut.UploadID)
	directPut := directBegin.DirectPut
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
		namespaceAliasBaseURL+"/filesystem/entries?"+listingQuery.Encode(),
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
		proxyServer.URL+"/v0/namespace-aliases/"+url.PathEscape(request.UnknownNamespaceAlias)+"/filesystem/entries",
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

func proxyCreateUpload(
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
) *loonfs.UploadSession {
	t.Helper()
	return proxyJSONRequest[loonfs.UploadSession](
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
	return proxyCreateCommit(t, httpClient, namespaceAliasBaseURL, &loonfs.CommitRequest{
		NamespaceID:   namespaceID,
		Actor:         actor,
		CommitID:      loonfs.CommitID(commitID),
		ContentTokens: contentTokens,
		Operations: []*loonfs.FilesystemOperation{
			{
				PutFile: &loonfs.FilesystemOperationPutFile{
					Behavior:   &noReplace,
					ContentRef: contentRef,
					Path:       loonfs.AbsolutePath(path),
				},
			},
		},
	})
}

func proxyCreateCommit(
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
	committed, err := h.client.Commits.Create(context.Background(), commit)
	if err != nil {
		t.Fatalf("commit change: %v", err)
	}
	if int64(committed.CommittedSeq) != expected.CommittedSeq {
		t.Errorf("committed_seq = %d, want %d", committed.CommittedSeq, expected.CommittedSeq)
	}
	feed, err := h.client.Changes.List(
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

func requireCompletedStatus(
	t *testing.T,
	response *loonfs.UploadSession,
) *loonfs.UploadSessionStatusCompleted {
	t.Helper()
	if response == nil {
		t.Fatal("upload session response is nil")
	}
	if response.Completed == nil || response.Completed.ContentRef == nil {
		t.Fatalf("upload status = %q, want completed", response.Status)
	}
	return response.Completed
}

func requireAbortedStatus(
	t *testing.T,
	response *loonfs.UploadSession,
) *loonfs.UploadSessionStatusAborted {
	t.Helper()
	if response == nil {
		t.Fatal("upload session response is nil")
	}
	if response.Aborted == nil {
		t.Fatalf("upload status = %q, want aborted", response.Status)
	}
	return response.Aborted
}

func requireFileProjection(t *testing.T, entry *loonfs.PathEntry) *loonfs.PathEntryFile {
	t.Helper()
	if entry == nil {
		t.Fatal("path entry is nil")
	}
	if entry.File == nil {
		t.Fatalf("path entry kind = %q, want file", entry.InodeKind)
	}
	return entry.File
}

// pathEntryIdentity is the common subset of the generated path-entry variants.
type pathEntryIdentity struct {
	path              string
	inodeID           string
	displayName       string
	bindingGeneration string
}

func identityOf(entry *loonfs.PathEntry) pathEntryIdentity {
	if entry == nil {
		return pathEntryIdentity{}
	}
	if entry.Dir != nil {
		return pathEntryIdentity{
			path:              string(entry.Dir.Path),
			inodeID:           string(entry.Dir.InodeID),
			displayName:       optionalString(entry.Dir.DisplayName),
			bindingGeneration: optionalString(entry.Dir.BindingGeneration),
		}
	}
	if entry.File != nil {
		return pathEntryIdentity{
			path:              string(entry.File.Path),
			inodeID:           string(entry.File.InodeID),
			displayName:       optionalString(entry.File.DisplayName),
			bindingGeneration: optionalString(entry.File.BindingGeneration),
		}
	}
	return pathEntryIdentity{}
}

// optionalString returns an empty string for an absent value, which is what
// the root entry reports for its display name and its binding generation.
func optionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func commitCompletedFile(
	t *testing.T,
	sdk *server.Client,
	namespaceID string,
	path string,
	commitID string,
	actor *loonfs.ActorRef,
	contentRef *loonfs.ContentRef,
	contentToken *loonfs.ContentToken,
) *loonfs.CommitResponse {
	t.Helper()
	noReplace := loonfs.DestinationBehaviorNoReplace
	return applyCommit(t, sdk, &loonfs.CommitRequest{
		NamespaceID:   namespaceID,
		Actor:         actor,
		CommitID:      loonfs.CommitID(commitID),
		ContentTokens: contentTokens(contentToken),
		Operations: []*loonfs.FilesystemOperation{
			{
				PutFile: &loonfs.FilesystemOperationPutFile{
					Behavior:   &noReplace,
					ContentRef: contentRef,
					Path:       loonfs.AbsolutePath(path),
				},
			},
		},
	})
}

func applyCommit(t *testing.T, sdk *server.Client, request *loonfs.CommitRequest) *loonfs.CommitResponse {
	t.Helper()
	response, err := sdk.Commits.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("apply commit %q: %v", request.CommitID, err)
	}
	return response
}

func statPath(t *testing.T, sdk *server.Client, namespaceID, path string) *loonfs.PathEntry {
	t.Helper()
	entry, err := sdk.Files.Retrieve(context.Background(), &loonfs.GetPathEntryRequest{
		NamespaceID: namespaceID,
		Path:        path,
	})
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return entry
}

func getFile(t *testing.T, sdk *server.Client, namespaceID, path string) *transfers.GetFileResult {
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
	sdk *server.Client,
	namespaceID string,
	path string,
) []*loonfs.PathEntry {
	t.Helper()
	page, err := sdk.Files.List(context.Background(), &loonfs.ListPathEntriesRequest{
		NamespaceID: namespaceID,
		Path:        path,
	})
	if err != nil {
		t.Fatalf("list %s: %v", path, err)
	}
	return page.Results
}

func listingContainsPath(entries []*loonfs.PathEntry, path string) bool {
	for _, entry := range entries {
		if identityOf(entry).path == path {
			return true
		}
	}
	return false
}

func listChanges(t *testing.T, sdk *server.Client, namespaceID string) *loonfs.ListChangesResponse {
	t.Helper()
	changes, err := sdk.Changes.List(context.Background(), &loonfs.ListChangesRequest{
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

func createNamespace(t *testing.T, sdk *server.Client, namespaceID string) {
	t.Helper()
	_, err := sdk.Namespaces.Create(
		context.Background(),
		&loonfs.CreateNamespaceRequest{NamespaceID: loonfs.NamespaceID(namespaceID)},
	)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
}

func applyCreateDirectory(
	t *testing.T,
	sdk *server.Client,
	namespaceID string,
	commitID string,
	actor *loonfs.ActorRef,
	path string,
) {
	t.Helper()
	_, err := sdk.Commits.Create(
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
				CreateDirectory: &loonfs.FilesystemOperationCreateDirectory{
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
