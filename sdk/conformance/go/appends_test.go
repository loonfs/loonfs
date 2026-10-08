package conformance_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/loonfs/loonfs-sdk-go/files"
	"github.com/loonfs/loonfs-sdk-go/option"
	"github.com/loonfs/loonfs-sdk-go/server"
)

type appendCase struct {
	Name  string `json:"name"`
	Bytes int    `json:"bytes"`
	Sent  bool   `json:"sent"`
}

func TestAppendSendsOneCommit(t *testing.T) {
	data, err := os.ReadFile("../fixtures/appends.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []appendCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range cases {
		t.Run(fixture.Name, func(t *testing.T) {
			content := make([]byte, fixture.Bytes)
			for i := range content {
				content[i] = byte(i)
			}
			var mu sync.Mutex
			var requests []string
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				requests = append(requests, r.Method+" "+r.URL.Path)
				if r.Method != http.MethodPost || r.URL.Path != "/v0/namespaces/demo/commits" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
					return
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				operations, _ := body["operations"].([]any)
				if len(operations) != 1 {
					t.Errorf("operations = %v", body["operations"])
					http.Error(w, "one operation expected", http.StatusBadRequest)
					return
				}
				op := operations[0].(map[string]any)
				if body["commit_id"] != "stable" || body["message"] != "original" || r.Header.Get("Loonfs-Actor") != "writer" {
					t.Error("commit options changed")
				}
				if op["kind"] != "append_file" || op["path"] != "/file" || op["expected_inode_id"] != "ino_1" || op["expected_revision_no"] != float64(1) {
					t.Errorf("operation = %v", op)
				}
				if _, ok := op["behavior"]; ok {
					t.Error("an append has no behavior")
				}
				if tokens, ok := body["content_tokens"].([]any); ok && len(tokens) != 0 {
					t.Error("an append carries no content token")
				}
				encoded, _ := op["inline_content"].(string)
				if decoded, err := base64.StdEncoding.DecodeString(encoded); err != nil || !bytes.Equal(decoded, content) {
					t.Error("wrong appended bytes", err)
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{"namespace_id": "demo", "commit_id": "stable", "committed_seq": 2, "committed_at_ms": 0, "committed_by": "writer", "events": []any{}})
			}))
			defer endpoint.Close()
			client := server.NewClient(option.WithBaseURL(endpoint.URL), option.WithToken("secret"))
			message, inode, revision := "original", "ino_1", int64(1)
			committed, err := client.Files.Append(context.Background(), files.AppendInput{
				NamespaceID: "demo", Path: "/file", Content: content, CommitID: "stable",
				Message: &message, ExpectedInodeID: &inode, ExpectedRevisionNo: &revision,
			}, option.WithMaxAttempts(1), option.WithHTTPHeader(http.Header{"Loonfs-Actor": []string{"writer"}}))
			mu.Lock()
			defer mu.Unlock()
			if !fixture.Sent {
				if err == nil {
					t.Fatal("expected the helper to refuse the content")
				}
				if len(requests) != 0 {
					t.Fatalf("a refused append sent %v", requests)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if committed.CommittedSeq != 2 || len(requests) != 1 {
				t.Fatalf("committed_seq = %d after %v", committed.CommittedSeq, requests)
			}
		})
	}
}
