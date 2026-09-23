package conformance_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	loonfs "github.com/loonfs/loonfs-sdk-go"
	"github.com/loonfs/loonfs-sdk-go/core"
	"github.com/loonfs/loonfs-sdk-go/option"
	"github.com/loonfs/loonfs-sdk-go/server"
)

func TestRetryControlsReturnTheFinalResponseWithoutBackoff(t *testing.T) {
	for _, mode := range []string{"client-disabled", "request-disabled", "one-attempt", "disabled-client-with-request-attempts"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.Header().Set("X-Request-Id", "req_retry_control")
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `{"code":"temporarily_unavailable","message":"try later"}`)
			}))
			defer local.Close()
			clientOptions := []option.RequestOption{option.WithBaseURL(local.URL)}
			var requestOptions []option.RequestOption
			switch mode {
			case "client-disabled":
				clientOptions = append(clientOptions, option.WithoutRetries())
			case "request-disabled":
				requestOptions = append(requestOptions, option.WithoutRetries())
			case "one-attempt":
				requestOptions = append(requestOptions, option.WithMaxAttempts(1))
			case "disabled-client-with-request-attempts":
				clientOptions = append(clientOptions, option.WithoutRetries())
				requestOptions = append(requestOptions, option.WithMaxAttempts(3))
			}
			client := server.NewClient(clientOptions...)
			started := time.Now()
			_, err := client.Files.Retrieve(context.Background(), &loonfs.GetPathEntryRequest{
				NamespaceID: "demo", Path: "/missing",
			}, requestOptions...)
			if elapsed := time.Since(started); elapsed >= time.Second {
				t.Errorf("final response waited for backoff: %s", elapsed)
			}
			if calls.Load() != 1 {
				t.Errorf("made %d requests, want 1", calls.Load())
			}
			var apiError *core.APIError
			if !errors.As(err, &apiError) || apiError.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("expected original HTTP error, got %v", err)
			}
			if apiError.Header.Get("Retry-After") != "1" || apiError.Header.Get("X-Request-Id") != "req_retry_control" {
				t.Errorf("lost response headers: %v", apiError.Header)
			}
			var unavailable *loonfs.ServiceUnavailableError
			if !errors.As(err, &unavailable) {
				t.Fatalf("expected decoded service error, got %v", err)
			}
			body, ok := unavailable.Body.(map[string]interface{})
			if !ok || body["message"] != "try later" {
				t.Errorf("lost decoded error body: %v", err)
			}
		})
	}
}

func TestRetryBackoffRespectsContext(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", deadline), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			}
			defer cancel()
			var calls atomic.Int32
			local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusServiceUnavailable)
				if !deadline {
					time.AfterFunc(30*time.Millisecond, cancel)
				}
			}))
			defer local.Close()
			client := server.NewClient(option.WithBaseURL(local.URL))
			started := time.Now()
			_, err := client.Files.Retrieve(ctx, &loonfs.GetPathEntryRequest{NamespaceID: "demo", Path: "/missing"})
			if elapsed := time.Since(started); elapsed >= time.Second {
				t.Errorf("context waited for backoff: %s", elapsed)
			}
			if !errors.Is(err, ctx.Err()) || ctx.Err() == nil {
				t.Errorf("expected context error %v, got %v", ctx.Err(), err)
			}
			if calls.Load() != 1 {
				t.Errorf("made %d requests, want 1", calls.Load())
			}
		})
	}
}
