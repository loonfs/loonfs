package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/loonfs/loonfs-sdk-go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The server marks a retryable answer with Retry-After. Every other answer is
// final, whatever its status code.
func TestRetrierFollowsRetryAfter(t *testing.T) {
	tests := []struct {
		description string
		statusCodes []int
		retryAfter  string
		wantStatus  int
		wantCalls   int32
	}{
		{
			description: "a 503 with Retry-After is retried until it succeeds",
			statusCodes: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusOK},
			retryAfter:  "1",
			wantStatus:  http.StatusOK,
			wantCalls:   3,
		},
		{
			description: "attempts stop at the cap",
			statusCodes: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusOK},
			retryAfter:  "1",
			wantStatus:  http.StatusServiceUnavailable,
			wantCalls:   3,
		},
		{
			description: "a 503 without Retry-After is final",
			statusCodes: []int{http.StatusServiceUnavailable, http.StatusOK},
			wantStatus:  http.StatusServiceUnavailable,
			wantCalls:   1,
		},
		{
			description: "a 429 without Retry-After is final",
			statusCodes: []int{http.StatusTooManyRequests, http.StatusOK},
			wantStatus:  http.StatusTooManyRequests,
			wantCalls:   1,
		},
		{
			description: "a 500 without Retry-After is final",
			statusCodes: []int{http.StatusInternalServerError, http.StatusOK},
			wantStatus:  http.StatusInternalServerError,
			wantCalls:   1,
		},
	}

	for _, tc := range tests {
		test := tc
		t.Run(test.description, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				index := int(calls.Add(1)) - 1
				if index >= len(test.statusCodes) {
					index = len(test.statusCodes) - 1
				}
				status := test.statusCodes[index]
				if status != http.StatusOK && test.retryAfter != "" {
					w.Header().Set("Retry-After", test.retryAfter)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				require.NoError(t, json.NewEncoder(w).Encode(&InternalTestResponse{Id: "1"}))
			}))
			defer server.Close()

			caller := NewCaller(&CallerParams{Client: server.Client()})
			var response *InternalTestResponse
			_, err := caller.Call(
				context.Background(),
				&CallParams{
					URL:                server.URL,
					Method:             http.MethodGet,
					Request:            &InternalTestRequest{},
					Response:           &response,
					MaxAttempts:        3,
					ResponseIsOptional: true,
				},
			)

			assert.Equal(t, test.wantCalls, calls.Load())
			if test.wantStatus == http.StatusOK {
				require.NoError(t, err)
				assert.Equal(t, &InternalTestResponse{Id: "1"}, response)
				return
			}
			require.IsType(t, &core.APIError{}, err)
			assert.Equal(t, test.wantStatus, err.(*core.APIError).StatusCode)
		})
	}
}
