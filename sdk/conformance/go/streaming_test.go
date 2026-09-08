package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/files"
	"github.com/loonfs/loonfs-sdk-go/option"
	"github.com/loonfs/loonfs-sdk-go/server"
)

type streamCase struct {
	Name           string `json:"name"`
	Content        string `json:"content"`
	Algorithm      string `json:"algorithm"`
	Checksum       string `json:"checksum"`
	SizeBytes      int    `json:"size_bytes"`
	Error          bool   `json:"error"`
	TransportError bool   `json:"transport_error"`
}

func streamingCases(t *testing.T) []streamCase {
	t.Helper()
	data, err := os.ReadFile("../fixtures/streaming_downloads.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []streamCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

func streamTestServer(t *testing.T, fixture streamCase, direct bool, content func(http.ResponseWriter, *http.Request)) *httptest.Server {
	t.Helper()
	var host *httptest.Server
	claim := map[string]any{"kind": "blob", "owner_namespace_id": "demo", "content_id": "cnt_00000000000000000000000000000001", "size_bytes": fixture.SizeBytes, "checksum": map[string]any{"algorithm": fixture.Algorithm, "value": fixture.Checksum}}
	host = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/object" {
			if r.Header.Get("Authorization") != "" || r.Header.Get("X-Private") != "" {
				t.Error("API credentials leaked to object store")
			}
			content(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("API authorization missing")
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/capabilities"):
			json.NewEncoder(w).Encode(map[string]any{"protocol_version": "v0", "api_groups": []string{"filesystem/v0"}, "features": map[string]bool{"filesystem.downloads.direct_get": direct}})
		case strings.HasSuffix(r.URL.Path, "/downloads"):
			json.NewEncoder(w).Encode(map[string]any{"namespace_id": "demo", "path": "/file", "revision_no": 1, "content_ref": claim, "access": map[string]any{"kind": "presigned_url", "url": host.URL + "/object", "method": "GET", "expires_at_ms": 2000000000000}})
		case strings.HasSuffix(r.URL.Path, "/entry"):
			json.NewEncoder(w).Encode(map[string]any{"inode_kind": "file", "namespace_id": "demo", "path": "/file", "revision_no": 1, "content_ref": claim})
		case strings.HasSuffix(r.URL.Path, "/content"):
			if r.URL.Query().Get("revision_no") != "1" {
				t.Error("proxy read is not pinned to its revision")
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			content(w, r)
		default:
			t.Errorf("unexpected request %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	return host
}

func TestStreamingDownloadConformance(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, fixture := range streamingCases(t) {
			t.Run(fixture.Name+map[bool]string{false: "_proxy", true: "_direct"}[direct], func(t *testing.T) {
				host := streamTestServer(t, fixture, direct, func(w http.ResponseWriter, r *http.Request) {
					if fixture.TransportError {
						w.Header().Set("Content-Length", "10")
					}
					io.WriteString(w, fixture.Content)
				})
				defer host.Close()
				client := server.NewClient(option.WithBaseURL(host.URL), option.WithToken("private-token"), option.WithHTTPHeader(http.Header{"X-Private": {"secret"}}), option.WithHTTPClient(host.Client()))
				opened, err := client.Files.DownloadStream(context.Background(), files.DownloadInput{NamespaceID: loonfs.NamespaceID("demo"), Path: loonfs.AbsolutePath("/file")})
				if err != nil {
					t.Fatal(err)
				}
				defer opened.Content.Close()
				data, err := io.ReadAll(opened.Content)
				if fixture.Error {
					if err == nil {
						t.Fatal("invalid body passed verification")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if string(data) != fixture.Content {
					t.Fatalf("got %q", data)
				}
			})
		}
	}
}

func TestStreamingDownloadsStartBeforeEOFAndCancelPendingReads(t *testing.T) {
	fixture := streamingCases(t)[0]
	for _, direct := range []bool{false, true} {
		host := streamTestServer(t, fixture, direct, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "1")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		})
		client := server.NewClient(option.WithBaseURL(host.URL), option.WithToken("private-token"))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		opened, err := client.Files.DownloadStream(ctx, files.DownloadInput{NamespaceID: "demo", Path: "/file"})
		if err != nil {
			cancel()
			host.Close()
			t.Fatal(err)
		}
		first := make([]byte, 1)
		if _, err := io.ReadFull(opened.Content, first); err != nil {
			t.Fatal(err)
		}
		cancel()
		if _, err := opened.Content.Read(first); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
		opened.Content.Close()
		host.Close()
	}
}
