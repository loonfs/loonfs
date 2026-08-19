package conformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/client"
	"github.com/loonfs/loonfs-sdk-go/option"
)

const fixtureVersion = 1

const transferSkip = "file transfer cases are not implemented in the Go harness yet"

type conformanceCase struct {
	Version  int             `json:"version"`
	Name     string          `json:"name"`
	Intent   string          `json:"intent"`
	Family   string          `json:"family"`
	Request  json.RawMessage `json:"request"`
	Expected json.RawMessage `json:"expected"`
}

var expectedCases = []struct {
	name   string
	family string
}{
	{name: "changes", family: "changes"},
	{name: "commit_replay", family: "commit_replay"},
	{name: "download", family: "download"},
	{name: "end_to_end", family: "end_to_end"},
	{name: "error_contract", family: "error_contract"},
	{name: "pagination", family: "pagination"},
	{name: "upload_abort", family: "upload_abort"},
	{name: "upload_direct_put", family: "upload_direct_put"},
	{name: "upload_multipart", family: "upload_multipart"},
}

type harness struct {
	baseURL         string
	token           string
	client          *client.Client
	unauthenticated *client.Client
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
		baseURL: baseURL,
		token:   token,
		client: client.NewClient(
			option.WithBaseURL(baseURL),
			option.WithToken(token),
		),
		unauthenticated: client.NewClient(option.WithBaseURL(baseURL)),
	}

	for _, testCase := range cases {
		t.Run(testCase.Name, func(t *testing.T) {
			switch testCase.Family {
			case "error_contract":
				runErrorContract(t, h, testCase)
			case "commit_replay":
				runCommitReplay(t, h, testCase)
			case "pagination":
				runPagination(t, h, testCase)
			case "changes":
				runChanges(t, h, testCase)
			case "upload_direct_put", "upload_multipart", "upload_abort", "download", "end_to_end":
				t.Skip(transferSkip)
			default:
				t.Fatalf("unknown case family %q", testCase.Family)
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
		return nil, fmt.Errorf("fixture version 1 requires %d cases, found %d", len(expectedCases), len(cases))
	}
	for index, expected := range expectedCases {
		if cases[index].Name != expected.name || cases[index].Family != expected.family {
			return nil, fmt.Errorf(
				"expected %q with family %q, found %q with family %q",
				expected.name,
				expected.family,
				cases[index].Name,
				cases[index].Family,
			)
		}
	}
	return cases, nil
}

func validateCase(path string, testCase *conformanceCase) error {
	if testCase.Version != fixtureVersion {
		return fmt.Errorf("invalid fixture %s: version must be %d, found %d", path, fixtureVersion, testCase.Version)
	}
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
	NamespaceID     string          `json:"namespace_id"`
	MalformedBody   json.RawMessage `json:"malformed_body"`
	InvalidAfterSeq string          `json:"invalid_after_seq"`
}

type errorContractExpected struct {
	Unauthenticated errorStatusExpected `json:"unauthenticated"`
	MalformedBody   errorOutcome        `json:"malformed_body"`
	InvalidQuery    errorOutcome        `json:"invalid_query"`
}

type errorStatusExpected struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
}

type errorOutcome struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Param  string `json:"param"`
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

	malformedRequest, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		fmt.Sprintf("%s/v0/namespaces/%s/commits", h.baseURL, request.NamespaceID),
		bytes.NewReader(request.MalformedBody),
	)
	if err != nil {
		t.Fatalf("build malformed-body request: %v", err)
	}
	malformedRequest.Header.Set("Authorization", "Bearer "+h.token)
	malformedRequest.Header.Set("Content-Type", "application/json")
	malformed := sendRawRequest(t, malformedRequest)
	assertRawError(t, malformed, expected.MalformedBody)

	invalidRequest, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		fmt.Sprintf(
			"%s/v0/namespaces/%s/changes?after_seq=%s",
			h.baseURL,
			request.NamespaceID,
			request.InvalidAfterSeq,
		),
		nil,
	)
	if err != nil {
		t.Fatalf("build invalid-query request: %v", err)
	}
	invalidRequest.Header.Set("Authorization", "Bearer "+h.token)
	invalidQuery := sendRawRequest(t, invalidRequest)
	assertRawError(t, invalidQuery, expected.InvalidQuery)
}

func sendRawRequest(t *testing.T, request *http.Request) *http.Response {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("send raw request: %v", err)
	}
	return response
}

func assertRawError(t *testing.T, response *http.Response, expected errorOutcome) {
	t.Helper()
	defer response.Body.Close()
	if response.StatusCode != expected.Status {
		t.Errorf("status = %d, want %d", response.StatusCode, expected.Status)
	}
	var apiError loonfs.APIError
	if err := json.NewDecoder(response.Body).Decode(&apiError); err != nil {
		t.Fatalf("decode API error envelope: %v", err)
	}
	if apiError.Code != expected.Code {
		t.Errorf("code = %q, want %q", apiError.Code, expected.Code)
	}
	if apiError.Param == nil || *apiError.Param != expected.Param {
		t.Errorf("param = %v, want %q", apiError.Param, expected.Param)
	}
	if apiError.RequestID == nil {
		t.Error("error has no request_id")
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
	var cursor *string
	pageCount := 0
	var savedCursor *string
	resumeOffset := -1
	for {
		page, err := h.client.Filesystem.ListPathEntries(
			context.Background(),
			&loonfs.ListPathEntriesRequest{
				NamespaceID: request.NamespaceID,
				Path:        request.Directory,
				Limit:       &request.PageSize,
				Cursor:      cursor,
			},
		)
		if err != nil {
			t.Fatalf("list pagination page: %v", err)
		}
		pageCount++
		if int64(page.HeadSeq) != expected.HeadSeq {
			t.Errorf("page %d head_seq = %d, want %d", pageCount, page.HeadSeq, expected.HeadSeq)
		}
		observed = append(observed, listedNames(t, page.Entries)...)
		cursor = page.NextCursor
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
	cursor = savedCursor
	for {
		page, err := h.client.Filesystem.ListPathEntries(
			context.Background(),
			&loonfs.ListPathEntriesRequest{
				NamespaceID: request.NamespaceID,
				Path:        request.Directory,
				Limit:       &request.PageSize,
				Cursor:      cursor,
			},
		)
		if err != nil {
			t.Fatalf("resume pagination page: %v", err)
		}
		resumed = append(resumed, listedNames(t, page.Entries)...)
		cursor = page.NextCursor
		if cursor == nil {
			break
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

type changesRequest struct {
	NamespaceID string          `json:"namespace_id"`
	Path        string          `json:"path"`
	CommitID    string          `json:"commit_id"`
	Actor       loonfs.ActorRef `json:"actor"`
	AfterSeq    int64           `json:"after_seq"`
}

type changesExpected struct {
	CommittedSeq int64  `json:"committed_seq"`
	ChangeCount  int    `json:"change_count"`
	EventKind    string `json:"event_kind"`
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
	if expected.EventKind != "directory_created" {
		t.Errorf("expected event kind = %q, want %q", expected.EventKind, "directory_created")
	}
	if len(change.Events) != 1 || change.Events[0] == nil || change.Events[0].DirectoryCreated == nil {
		t.Errorf("change events = %#v, want one directory_created event", change.Events)
	}
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
