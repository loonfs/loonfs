package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestAuthorizeRefusesOrStampsCommitBodies(t *testing.T) {
	type forwardedRequest struct {
		body             []byte
		path             string
		contentType      string
		contentLength    int64
		transferEncoding []string
	}
	forwarded := make(chan forwardedRequest, 3)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read forwarded body: %v", err)
		}
		forwarded <- forwardedRequest{body, r.URL.Path, r.Header.Get("Content-Type"), r.ContentLength, r.TransferEncoding}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer upstream.Close()
	refusal := &Refusal{Status: http.StatusForbidden, ContentType: "application/json", Body: []byte(`{"code":"unauthorized","message":"refused"}`)}
	proxy, err := NewHandler(Config{
		ServerBaseURL:    upstream.URL,
		Token:            "server-token",
		NamespaceAliases: map[string]string{"team-files": "namespace-id"},
		Authorize: func(r *http.Request, c RouteContext) (Authorization, error) {
			expected := RouteContext{http.MethodPost, "/v0/namespace-aliases/{namespace_alias}/commits", "team-files", "namespace-id"}
			if c != expected {
				t.Errorf("route context = %#v, want %#v", c, expected)
			}
			if r.Header.Get("X-Refuse") != "" {
				return Authorization{}, refusal
			}
			return Authorization{ActorID: "proxy-actor-雪"}, nil
		},
	})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	for _, testCase := range []struct {
		name   string
		body   string
		refuse bool
		status int
		result string
	}{
		{"refusal", `"nope"`, true, refusal.Status, string(refusal.Body)},
		{"stamped", `{"actor_id":"browser-actor","expected_head_seq":9007199254740993}`, false, http.StatusAccepted, ""},
		{"non_object", `"nope"`, false, http.StatusBadRequest, `{"code":"invalid_request","message":"commit body must be a JSON object"}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v0/namespace-aliases/team-files/commits", strings.NewReader(testCase.body))
			request.Header.Set("Content-Type", "text/plain")
			request.Header.Set("Content-Length", strconv.Itoa(len(testCase.body)))
			request.Header.Set("Transfer-Encoding", "chunked")
			request.TransferEncoding = []string{"chunked"}
			if testCase.refuse {
				request.Header.Set("X-Refuse", "1")
			}
			response := httptest.NewRecorder()
			proxy.ServeHTTP(response, request)
			if response.Code != testCase.status || response.Body.String() != testCase.result {
				t.Errorf("response = (%d, %s), want (%d, %s)", response.Code, response.Body, testCase.status, testCase.result)
			}
			if testCase.status != http.StatusAccepted && response.Header().Get("Content-Type") != "application/json" {
				t.Errorf("content type = %q, want application/json", response.Header().Get("Content-Type"))
			}
		})
	}
	if len(forwarded) != 1 {
		t.Fatalf("forwarded request count = %d, want 1", len(forwarded))
	}
	actual := <-forwarded
	if actual.path != "/v0/namespaces/namespace-id/commits" || actual.contentType != "application/json" ||
		actual.contentLength != int64(len(actual.body)) || len(actual.transferEncoding) != 0 {
		t.Errorf("forwarded request = %#v", actual)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(actual.body, &body); err != nil {
		t.Fatalf("decode forwarded body: %v", err)
	}
	if string(body["actor_id"]) != `"proxy-actor-雪"` || string(body["expected_head_seq"]) != "9007199254740993" || len(body) != 2 {
		t.Errorf("forwarded body = %s", actual.body)
	}
}
