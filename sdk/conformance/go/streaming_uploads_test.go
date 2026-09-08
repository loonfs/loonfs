package conformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/option"
	"github.com/loonfs/loonfs-sdk-go/server"
)

type uploadCase struct {
	streamCase
	Mode  string `json:"mode"`
	Size  *int64 `json:"size"`
	Fault string `json:"fault"`
}
type uploadSource struct {
	reader   *strings.Reader
	fault    bool
	position atomic.Int64
	eof      atomic.Bool
}

func (s *uploadSource) Read(p []byte) (int, error) {
	if len(p) > 65536 {
		return 0, fmt.Errorf("unbounded source read: %d", len(p))
	}
	n, err := s.reader.Read(p)
	s.position.Add(int64(n))
	if err == io.EOF {
		s.eof.Store(true)
		if s.fault {
			err = errors.New("source failed after its last byte")
		}
	}
	return n, err
}

func TestStreamingUploads(t *testing.T) {
	data, err := os.ReadFile("../fixtures/streaming_uploads.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []uploadCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range cases {
		t.Run(fixture.Name, func(t *testing.T) {
			source := &uploadSource{reader: strings.NewReader(fixture.Content), fault: fixture.Fault == "source_error"}
			var payload, abort, complete atomic.Int64
			var bodies bytes.Buffer // server handlers complete before their response is consumed
			claim := map[string]any{"kind": "blob", "owner_namespace_id": "demo", "content_id": "cnt_00000000000000000000000000000001", "size_bytes": fixture.SizeBytes, "checksum": map[string]any{"algorithm": fixture.Algorithm, "value": fixture.Checksum}}
			var host *httptest.Server
			host = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				path := r.URL.Path
				if path == "/object" {
					if r.Header.Get("Authorization") != "" || r.Header.Get("X-Private") != "" {
						t.Error("API credentials leaked")
					}
					if fixture.Mode == "direct_put" && r.ContentLength != *fixture.Size {
						t.Errorf("content length=%d", r.ContentLength)
					}
				} else if r.Header.Get("Authorization") != "Bearer private-token" {
					t.Error("missing API credentials")
				}
				w.Header().Set("Content-Type", "application/json")
				session := map[string]any{"namespace_id": "demo", "upload_id": "upl_test", "mode": fixture.Mode}
				access := map[string]any{"kind": "presigned_url", "method": "PUT", "url": host.URL + "/object", "expires_at_ms": 2000000000000}
				var value any
				switch {
				case strings.HasSuffix(path, "/capabilities"):
					limits := map[string]int{}
					if fixture.Mode == "direct_put" {
						limits["upload.max_content_bytes"] = 0
					}
					value = map[string]any{"protocol_version": "v0", "api_groups": []string{"filesystem/v0"}, "features": map[string]bool{"filesystem.uploads.direct_put": fixture.Mode == "direct_put", "filesystem.uploads.direct_multipart": fixture.Mode == "direct_multipart"}, "limits": limits}
				case strings.HasSuffix(path, "/uploads"):
					var body struct {
						Mode string `json:"mode"`
					}
					json.NewDecoder(r.Body).Decode(&body)
					if body.Mode != fixture.Mode {
						t.Errorf("mode %s", body.Mode)
					}
					session["checksum_algorithm"] = fixture.Algorithm
					session["part_size_bytes"] = 4
					session["access"] = access
					value = session
				case strings.HasSuffix(path, "/parts"):
					var body struct {
						Parts []struct {
							PartNumber int `json:"part_number"`
						} `json:"parts"`
					}
					json.NewDecoder(r.Body).Decode(&body)
					if len(body.Parts) != 1 {
						t.Error("must sign one part at a time")
						http.Error(w, "bad parts", 400)
						return
					}
					if source.position.Load() > payload.Load()*4+4 {
						t.Error("read the next part before sending this one")
					}
					value = map[string]any{"namespace_id": "demo", "upload_id": "upl_test", "parts": []any{map[string]any{"part_number": body.Parts[0].PartNumber, "access": access}}}
				case path == "/object" || strings.HasSuffix(path, "/content"):
					payload.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, "body interrupted", 400)
						return
					}
					bodies.Write(body)
					if fixture.Fault == "timeout" {
						<-r.Context().Done()
						return
					}
					if fixture.Fault == "payload_error" {
						http.Error(w, "payload failed", 503)
						return
					}
					w.Header().Set("ETag", "test-etag")
					session["content_ref"] = claim
					value = session
				case strings.HasSuffix(path, "/abort"):
					abort.Add(1)
					session["status"] = "aborted"
					session["aborted_at_ms"] = 0
					value = session
				case strings.HasSuffix(path, "/complete"):
					complete.Add(1)
					if !source.eof.Load() {
						t.Error("completed before source EOF")
					}
					var body struct {
						Content *loonfs.UploadContentClaim `json:"content"`
					}
					json.NewDecoder(r.Body).Decode(&body)
					if fixture.Mode != "service_proxied" && (body.Content == nil || body.Content.Checksum.Value != fixture.Checksum || body.Content.SizeBytes != int64(fixture.SizeBytes)) {
						t.Error("wrong completion checksum/size")
					}
					if fixture.Fault == "completion_error" {
						http.Error(w, "completion unavailable", 503)
						return
					}
					session["status"] = "completed"
					session["completed_at_ms"] = 0
					session["content_ref"] = claim
					session["content_token"] = map[string]any{"token": "retained-token", "content_ref": claim}
					value = session
				default:
					t.Errorf("unexpected %s", path)
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(value)
			}))
			defer host.Close()
			client := server.NewClient(option.WithBaseURL(host.URL), option.WithToken("private-token"), option.WithHTTPHeader(http.Header{"X-Private": {"secret"}}), option.WithHTTPClient(host.Client()), option.WithMaxAttempts(4))
			ctx := context.Background()
			if fixture.Fault == "timeout" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}
			prepared, err := client.Files.PrepareFileStream(ctx, "demo", source, fixture.Size)
			if fixture.Error {
				if err == nil {
					t.Fatal("invalid upload succeeded")
				}
				expectedAbort, expectedComplete := int64(1), int64(0)
				if fixture.Fault == "completion_error" {
					expectedAbort, expectedComplete = 0, 1
				}
				if abort.Load() != expectedAbort || complete.Load() != expectedComplete {
					t.Fatalf("abort=%d complete=%d err=%v", abort.Load(), complete.Load(), err)
				}
				if fixture.Fault == "payload_error" && payload.Load() != 1 {
					t.Error("payload retried")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if prepared.ContentToken.Token != "retained-token" || bodies.String() != fixture.Content {
					t.Error("wrong prepared result or bytes")
				}
				if complete.Load() != 1 || abort.Load() != 0 {
					t.Error("unexpected session lifecycle")
				}
			}
		})
	}
}
