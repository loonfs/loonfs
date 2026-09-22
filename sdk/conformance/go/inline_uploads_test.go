package conformance_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/files"
	"github.com/loonfs/loonfs-sdk-go/option"
	"github.com/loonfs/loonfs-sdk-go/server"
)

type inlineCase struct {
	Name    string          `json:"name"`
	Bytes   int             `json:"bytes"`
	Size    *int64          `json:"size"`
	Limit   json.RawMessage `json:"limit"`
	Feature *bool           `json:"feature"`
	Inline  bool            `json:"inline"`
	Error   bool            `json:"error"`
	Fault   bool            `json:"fault"`
}

func TestInlinePreparationAndRetry(t *testing.T) {
	data, err := os.ReadFile("../fixtures/inline_uploads.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []inlineCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range cases {
		t.Run(fixture.Name, func(t *testing.T) {
			content := make([]byte, fixture.Bytes)
			for i := range content {
				content[i] = byte(i)
			}
			source := &uploadSource{reader: strings.NewReader(string(content)), fault: fixture.Fault}
			limit := int64(65536)
			limits := map[string]int64{}
			if string(fixture.Limit) != "null" {
				if len(fixture.Limit) != 0 {
					if err := json.Unmarshal(fixture.Limit, &limit); err != nil {
						t.Fatal(err)
					}
				}
				limits["commit.max_inline_content_bytes_per_operation"] = limit
			}
			feature := fixture.Feature == nil || *fixture.Feature
			session := map[string]any{"namespace_id": "demo", "upload_id": "upl_test", "mode": "service_proxied"}
			claim := map[string]any{"kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_test", "size_bytes": len(content), "checksum": map[string]any{"algorithm": "sha256", "value": strings.Repeat("0", 64)}}
			var mu sync.Mutex
			var paths []string
			var commits []map[string]any
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				path := r.URL.Path
				paths = append(paths, path)
				w.Header().Set("Content-Type", "application/json")
				var value any
				switch {
				case strings.HasSuffix(path, "/capabilities"):
					if len(paths) != 1 {
						t.Error("publication reselected transport")
					}
					value = map[string]any{"protocol_version": "v0", "api_groups": []string{"filesystem/v0"}, "features": map[string]bool{"filesystem.commits.inline_content": feature}, "limits": limits}
				case strings.HasSuffix(path, "/uploads"):
					if fixture.Inline || fixture.Error {
						t.Error("unexpected staged upload")
					}
					if feature && string(fixture.Limit) != "null" {
						bound := limit
						if bound > 65536 {
							bound = 65536
						}
						if source.position.Load() != bound+1 {
							t.Error("lookahead did not stop at the bound")
						}
					}
					session["status"] = "open"
					session["expires_at_ms"] = 2000000000000
					value = session
				case strings.HasSuffix(path, "/content"):
					body, err := io.ReadAll(r.Body)
					if err != nil || !bytes.Equal(body, content) {
						t.Error("fallback lost prefix bytes", err)
					}
					value = session
				case strings.HasSuffix(path, "/complete"):
					session["status"] = "completed"
					session["completed_at_ms"] = 0
					session["content_ref"] = claim
					session["content_token"] = map[string]any{"content_ref": claim, "token": "retained"}
					value = session
				case strings.HasSuffix(path, "/commits"):
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						return
					}
					commits = append(commits, body)
					op := body["operations"].([]any)[0].(map[string]any)
					if body["commit_id"] != "stable" || body["message"] != "original" || r.Header.Get("Loonfs-Actor") != "writer" || op["path"] != "/file" || op["behavior"] != "replace" || op["expected_inode_id"] != "ino_1" || op["expected_revision_no"] != float64(1) {
						t.Error("publication options changed")
					}
					if fixture.Inline {
						encoded, ok := op["inline_content"].(string)
						decoded, err := base64.StdEncoding.DecodeString(encoded)
						if !ok || err != nil || !bytes.Equal(decoded, content) || op["content_ref"] != nil {
							t.Error("wrong inline bytes")
						}
						if tokens, ok := body["content_tokens"].([]any); ok && len(tokens) != 0 {
							t.Error("unexpected inline token")
						}
					} else if op["inline_content"] != nil || op["content_ref"] == nil {
						t.Error("staged content representation changed")
					}
					if len(commits) == 1 {
						w.WriteHeader(503)
						value = map[string]any{"code": "deadline_exceeded", "message": "reply lost"}
					} else {
						if !reflect.DeepEqual(commits[0], body) {
							t.Error("retry request changed")
						}
						value = map[string]any{"namespace_id": "demo", "commit_id": "stable", "committed_seq": 1, "committed_at_ms": 0, "committed_by": "writer", "events": []any{}}
					}
				default:
					t.Error("unexpected request", path)
					w.WriteHeader(500)
				}
				json.NewEncoder(w).Encode(value)
			}))
			defer endpoint.Close()
			client := server.NewClient(option.WithBaseURL(endpoint.URL), option.WithToken("secret"))
			prepared, err := client.Files.PrepareStream(context.Background(), "demo", source, fixture.Size)
			if fixture.Error {
				if err == nil {
					t.Fatal("expected source validation failure")
				}
				mu.Lock()
				defer mu.Unlock()
				if !reflect.DeepEqual(paths, []string{"/v0/capabilities"}) {
					t.Fatal("source failure started a mutation", paths)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			_, inline := prepared.(*files.InlinePreparedContent)
			if inline != fixture.Inline {
				t.Fatalf("unexpected prepared kind %T", prepared)
			}
			message, inode, revision := "original", "ino_1", int64(1)
			input := files.PreparedUploadInput{NamespaceID: "demo", Path: "/file", Prepared: prepared, CommitID: "stable", Message: &message, Behavior: loonfs.DestinationBehaviorReplace, ExpectedInodeID: &inode, ExpectedRevisionNo: &revision}
			_, err = client.Files.UploadPrepared(context.Background(), input, option.WithMaxAttempts(1), option.WithHTTPHeader(http.Header{"Loonfs-Actor": []string{"writer"}}))
			if err == nil {
				t.Fatal("expected lost-response error")
			}
			result, err := client.Files.UploadPrepared(context.Background(), input, option.WithMaxAttempts(1), option.WithHTTPHeader(http.Header{"Loonfs-Actor": []string{"writer"}}))
			if err != nil || result.CommittedSeq != 1 {
				t.Fatal("replay failed", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(commits) != 2 {
				t.Fatal("unexpected retries", len(commits))
			}
		})
	}
}

func TestInlinePeekCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/capabilities" {
			t.Error("mutation started during cancelled peek")
		}
		fmt.Fprint(w, `{"protocol_version":"v0","api_groups":[],"features":{"filesystem.commits.inline_content":true},"limits":{"commit.max_inline_content_bytes_per_operation":65536}}`)
	}))
	defer endpoint.Close()
	client := server.NewClient(option.WithBaseURL(endpoint.URL))
	source := &cancellingInlineSource{cancel: cancel}
	_, err := client.Files.PrepareStream(ctx, "demo", source, nil)
	if err == nil {
		t.Fatal("expected cancellation")
	}
}

type cancellingInlineSource struct{ cancel context.CancelFunc }

func (s *cancellingInlineSource) Read(p []byte) (int, error) { s.cancel(); p[0] = 1; return 1, nil }
