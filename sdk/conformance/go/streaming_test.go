package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/files"
	"github.com/loonfs/loonfs-sdk-go/option"
	"github.com/loonfs/loonfs-sdk-go/server"
)

type streamCase struct {
	Name               string   `json:"name"`
	Content            string   `json:"content"`
	Algorithm          string   `json:"algorithm"`
	Checksum           string   `json:"checksum"`
	SizeBytes          int      `json:"size_bytes"`
	Range              *string  `json:"range"`
	TransportError     bool     `json:"transport_error"`
	ErrorMessage       string   `json:"error_message"`
	DirectErrorMessage string   `json:"direct_error_message"`
	ObjectRequests     []string `json:"object_requests"`
	Ranges             []struct {
		StartOffset int64  `json:"start_offset"`
		Length      int64  `json:"length"`
		Range       string `json:"range"`
		Content     string `json:"content"`
	} `json:"ranges"`
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

func streamTestClient(t *testing.T, fixture streamCase, direct bool, content func(http.ResponseWriter, *http.Request), requests chan<- string) *http.Client {
	t.Helper()
	claim := map[string]any{"kind": "blob_v1", "owner_namespace_id": "demo", "content_id": "con_00000000000000000000000000000001", "size_bytes": fixture.SizeBytes, "checksum": map[string]any{"algorithm": fixture.Algorithm, "value": fixture.Checksum}}
	var signed []string
	if fixture.Range != nil {
		signed = []string{*fixture.Range}
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests != nil {
			requests <- r.URL.Path
		}
		if strings.HasPrefix(r.URL.Path, "/object/") {
			index, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/object/"))
			if err != nil {
				t.Error(err)
				return
			}
			part := fixture.Ranges[index]
			if r.Header.Get("Authorization") != "" || r.Header.Get("X-Private") != "" {
				t.Error("API credentials leaked to object store")
			}
			if r.Header.Get("Range") != part.Range {
				t.Errorf("range = %q, want %q", r.Header.Get("Range"), part.Range)
			}
			io.WriteString(w, part.Content)
			return
		}
		if r.URL.Path == "/object" {
			if r.Header.Get("Authorization") != "" || r.Header.Get("X-Private") != "" {
				t.Error("API credentials leaked to object store")
			}
			if fixture.SizeBytes == 0 {
				t.Error("a grant of zero bytes needs no request")
			}
			if ranges := r.Header.Values("Range"); !reflect.DeepEqual(ranges, signed) {
				t.Errorf("object request range = %q, want the grant's %q", ranges, signed)
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
			access := map[string]any{"kind": "presigned_url", "url": "http://objects.test/object", "method": "GET", "expires_at_ms": 2000000000000}
			if fixture.Range != nil {
				access["headers"] = map[string]string{"range": *fixture.Range}
			}
			ranges := []any{map[string]any{"start_offset": 0, "length": fixture.SizeBytes, "access": access}}
			if fixture.Ranges != nil {
				ranges = make([]any, 0, len(fixture.Ranges))
				for index, part := range fixture.Ranges {
					ranges = append(ranges, map[string]any{"start_offset": part.StartOffset, "length": part.Length,
						"access": map[string]any{"kind": "presigned_url", "method": "GET", "url": "http://objects.test/object/" + strconv.Itoa(index), "headers": map[string]string{"range": part.Range}, "expires_at_ms": 2000000000000}})
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"namespace_id": "demo", "path": "/file", "revision_no": 1, "content_ref": claim, "ranges": ranges})
		case strings.HasSuffix(r.URL.Path, "/entry"):
			json.NewEncoder(w).Encode(map[string]any{"inode_kind": "file", "namespace_id": "demo", "path": "/file", "revision_no": 1, "content_ref": claim})
		case strings.HasSuffix(r.URL.Path, "/content"):
			if r.URL.Query().Get("revision_no") != "1" {
				t.Error("proxy read is not pinned to its revision")
			}
			if r.Header.Get("Range") != "" {
				t.Error("proxy read sent a range")
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			content(w, r)
		default:
			t.Errorf("unexpected request %s", r.URL)
			http.NotFound(w, r)
		}
	})
	return &http.Client{Transport: transferTransport(func(request *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		response := recorder.Result()
		if fixture.TransportError && (request.URL.Path == "/object" || strings.HasSuffix(request.URL.Path, "/content")) {
			response.Body = io.NopCloser(io.MultiReader(response.Body, failingReader{}))
		}
		return response, nil
	})}
}

type transferTransport func(*http.Request) (*http.Response, error)

func (transport transferTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestStreamingDownloadConformance(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, fixture := range streamingCases(t) {
			t.Run(fixture.Name+map[bool]string{false: "_proxy", true: "_direct"}[direct], func(t *testing.T) {
				requests := make(chan string, 32)
				httpClient := streamTestClient(t, fixture, direct, func(w http.ResponseWriter, r *http.Request) {
					if fixture.TransportError {
						w.Header().Set("Content-Length", "10")
					}
					io.WriteString(w, fixture.Content)
				}, requests)
				client := server.NewClient(option.WithBaseURL("http://api.test"), option.WithToken("private-token"), option.WithHTTPHeader(http.Header{"X-Private": {"secret"}}), option.WithHTTPClient(httpClient))
				opened, err := client.Files.DownloadStream(context.Background(), files.DownloadInput{NamespaceID: loonfs.NamespaceID("demo"), Path: loonfs.AbsolutePath("/file")})
				var data []byte
				if err == nil {
					defer opened.Content.Close()
					data, err = io.ReadAll(opened.Content)
				}
				expectedError := fixture.ErrorMessage
				if direct {
					expectedError = fixture.DirectErrorMessage
				}
				if fixture.TransportError {
					if !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("transport error = %v", err)
					}
				} else if expectedError != "" {
					if err == nil || err.Error() != expectedError {
						t.Fatalf("error = %v, want %s", err, expectedError)
					}
				} else if err != nil || string(data) != fixture.Content {
					t.Fatalf("content = %q, error = %v", data, err)
				}
				paths := []string{}
				for len(requests) > 0 {
					path := <-requests
					if direct {
						if strings.HasPrefix(path, "/object") {
							paths = append(paths, path)
						}
					} else {
						paths = append(paths, path[strings.LastIndex(path, "/")+1:])
					}
				}
				expectedPaths := fixture.ObjectRequests
				if !direct {
					expectedPaths = []string{"capabilities", "entry", "content"}
				}
				if !reflect.DeepEqual(paths, expectedPaths) {
					t.Fatalf("requests = %v, want %v", paths, expectedPaths)
				}

			})
		}
	}
}

func TestStreamingDownloadsStartBeforeEOFAndCancelPendingReads(t *testing.T) {
	fixture := streamingCases(t)[0]
	for _, direct := range []bool{false, true} {
		httpClient := streamTestClient(t, fixture, direct, func(http.ResponseWriter, *http.Request) {}, nil)
		metadata := httpClient.Transport
		httpClient.Transport = transferTransport(func(request *http.Request) (*http.Response, error) {
			if request.URL.Path != "/object" && !strings.HasSuffix(request.URL.Path, "/content") {
				return metadata.RoundTrip(request)
			}
			reader, writer := io.Pipe()
			go func() {
				writer.Write([]byte("1"))
				<-request.Context().Done()
				writer.CloseWithError(request.Context().Err())
			}()
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: reader}, nil
		})
		client := server.NewClient(option.WithBaseURL("http://api.test"), option.WithToken("private-token"), option.WithHTTPClient(httpClient))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		opened, err := client.Files.DownloadStream(ctx, files.DownloadInput{NamespaceID: "demo", Path: "/file"})
		if err != nil {
			cancel()
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
	}
}
