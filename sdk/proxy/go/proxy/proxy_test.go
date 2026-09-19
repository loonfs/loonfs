package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthorizeRefusesOrSetsActorHeader(t *testing.T) {
	type forwardedRequest struct {
		body        []byte
		path        string
		contentType string
		actor       string
		subject     string
		scope       string
		principals  string
	}
	forwarded := make(chan forwardedRequest, 3)
	upstream := recordingTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read forwarded body: %v", err)
		}
		forwarded <- forwardedRequest{body, r.URL.Path, r.Header.Get("Content-Type"), r.Header.Get("Loonfs-Actor"), r.Header.Get("Loonfs-Subject"), r.Header.Get("Loonfs-Principal-Scope"), r.Header.Get("Loonfs-Principals")}
		w.WriteHeader(http.StatusAccepted)
	})}
	refusal := &Refusal{Status: http.StatusForbidden, ContentType: "application/json", Body: []byte(`{"code":"unauthorized","message":"refused"}`)}
	proxy, err := NewHandler(Config{
		ServerBaseURL:    "http://upstream.invalid",
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
			return Authorization{ActorID: "proxy-actor", SubjectID: "proxy-subject", PrincipalScope: "org_proxy", Principals: []string{"prn_ada", "prn_team"}}, nil
		},
	})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	proxy.(*handler).transport = upstream
	for _, testCase := range []struct {
		name   string
		body   string
		refuse bool
		status int
		result string
	}{
		{"refusal", `"nope"`, true, refusal.Status, string(refusal.Body)},
		{"stamped", `{"expected_head_seq":9007199254740993}`, false, http.StatusAccepted, ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v0/namespace-aliases/team-files/commits", strings.NewReader(testCase.body))
			request.Header.Set("Content-Type", "text/plain")
			request.Header.Set("Loonfs-Actor", "browser-actor")
			request.Header.Set("Loonfs-Subject", "browser-subject")
			request.Header.Set("Loonfs-Principal-Scope", "org_browser")
			request.Header.Set("Loonfs-Principals", "prn_browser")
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
	if actual.path != "/v0/namespaces/namespace-id/commits" || actual.contentType != "text/plain" || actual.actor != "proxy-actor" || actual.subject != "proxy-subject" || actual.scope != "org_proxy" || actual.principals != "prn_ada,prn_team" {
		t.Errorf("forwarded request = %#v", actual)
	}
	if string(actual.body) != `{"expected_head_seq":9007199254740993}` {
		t.Errorf("forwarded body = %s", actual.body)
	}
}

func TestAuthorizeRejectsPartialSubjectContextBeforeForwarding(t *testing.T) {
	forwarded := 0
	upstream := recordingTransport{http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forwarded++
		w.WriteHeader(http.StatusAccepted)
	})}
	tests := []struct {
		name          string
		authorization Authorization
	}{
		{"principals without a scope", Authorization{Principals: []string{"prn_team"}}},
		{"scope without principals", Authorization{PrincipalScope: "org_demo"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proxy, err := NewHandler(Config{
				ServerBaseURL: "http://upstream.invalid",
				Token:         "server-token",
				Authorize: func(_ *http.Request, _ RouteContext) (Authorization, error) {
					return test.authorization, nil
				},
			})
			if err != nil {
				t.Fatalf("create proxy: %v", err)
			}
			proxy.(*handler).transport = upstream
			response := httptest.NewRecorder()
			proxy.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v0/capabilities", nil))
			if response.Code != http.StatusInternalServerError {
				t.Errorf("response status = %d, want %d", response.Code, http.StatusInternalServerError)
			}
		})
	}
	if forwarded != 0 {
		t.Errorf("forwarded request count = %d, want 0", forwarded)
	}
}

func TestActorHeaderIsRemovedWithoutAuthorize(t *testing.T) {
	upstream := recordingTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if actor := r.Header.Get("Loonfs-Actor"); actor != "" {
			t.Errorf("forwarded actor = %q", actor)
		}
		w.WriteHeader(http.StatusAccepted)
	})}
	proxy, err := NewHandler(Config{ServerBaseURL: "http://upstream.invalid", Token: "server-token"})
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	proxy.(*handler).transport = upstream
	request := httptest.NewRequest(http.MethodGet, "/v0/capabilities", nil)
	request.Header.Set("Loonfs-Actor", "browser-actor")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Errorf("response status = %d", response.Code)
	}
}

type recordingTransport struct {
	http.Handler
}

func (transport recordingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response := httptest.NewRecorder()
	transport.ServeHTTP(response, request)
	return response.Result(), nil
}
