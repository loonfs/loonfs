// Package proxy forwards LoonFS browser requests to a LoonFS server.
package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Config defines a proxy handler.
type Config struct {
	ServerBaseURL string
	Token         string
	Mounts        map[string]string
}

type handler struct {
	mounts    map[string]string
	target    *url.URL
	token     string
	transport http.RoundTripper
}

type route struct {
	method  string
	pattern string
}

// These routes must match docs/specs/openapi-proxy.json.
var routes = []route{
	{method: http.MethodGet, pattern: "/v0/capabilities"},
	{method: http.MethodGet, pattern: "/v0/mounts/{mount}/changes"},
	{method: http.MethodPost, pattern: "/v0/mounts/{mount}/commits"},
	{method: http.MethodGet, pattern: "/v0/mounts/{mount}/filesystem/content"},
	{method: http.MethodPost, pattern: "/v0/mounts/{mount}/filesystem/downloads"},
	{method: http.MethodGet, pattern: "/v0/mounts/{mount}/filesystem/list"},
	{method: http.MethodGet, pattern: "/v0/mounts/{mount}/filesystem/revisions"},
	{method: http.MethodGet, pattern: "/v0/mounts/{mount}/filesystem/stat"},
	{method: http.MethodGet, pattern: "/v0/mounts/{mount}/filesystem/trash"},
	{method: http.MethodGet, pattern: "/v0/mounts/{mount}/query/grep"},
	{method: http.MethodPost, pattern: "/v0/mounts/{mount}/uploads"},
	{method: http.MethodGet, pattern: "/v0/mounts/{mount}/uploads/{upload_id}"},
	{method: http.MethodPost, pattern: "/v0/mounts/{mount}/uploads/{upload_id}/abort"},
	{method: http.MethodPost, pattern: "/v0/mounts/{mount}/uploads/{upload_id}/complete"},
	{method: http.MethodPut, pattern: "/v0/mounts/{mount}/uploads/{upload_id}/content"},
	{method: http.MethodPost, pattern: "/v0/mounts/{mount}/uploads/{upload_id}/parts"},
}

var hopByHopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// NewHandler validates the config and returns a proxy handler.
func NewHandler(config Config) (http.Handler, error) {
	target, err := validateTarget(config.ServerBaseURL)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(config.Token) == "" {
		return nil, fmt.Errorf("proxy: token is required")
	}

	mounts := make(map[string]string, len(config.Mounts))
	for mount, namespaceID := range config.Mounts {
		if mount == "" || strings.Contains(mount, "/") {
			return nil, fmt.Errorf("proxy: mount %q must be one non-empty path segment", mount)
		}
		if !validNamespaceID(namespaceID) {
			return nil, fmt.Errorf("proxy: namespace id %q for mount %q is invalid", namespaceID, mount)
		}
		mounts[mount] = namespaceID
	}
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("proxy: default HTTP transport has unexpected type %T", http.DefaultTransport)
	}
	transport := defaultTransport.Clone()
	transport.DisableCompression = true

	return &handler{
		mounts:    mounts,
		target:    target,
		token:     config.Token,
		transport: transport,
	}, nil
}

func validateTarget(serverBaseURL string) (*url.URL, error) {
	if strings.TrimSpace(serverBaseURL) == "" {
		return nil, fmt.Errorf("proxy: server base URL is required")
	}
	target, err := url.Parse(serverBaseURL)
	if err != nil {
		return nil, fmt.Errorf("proxy: parse server base URL: %w", err)
	}
	if (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		return nil, fmt.Errorf("proxy: server base URL must be an absolute HTTP or HTTPS URL")
	}
	if target.User != nil || target.RawQuery != "" || target.ForceQuery || target.Fragment != "" {
		return nil, fmt.Errorf("proxy: server base URL must not contain user info, a query, or a fragment")
	}
	return target, nil
}

func validNamespaceID(namespaceID string) bool {
	if len(namespaceID) == 0 || len(namespaceID) > 128 {
		return false
	}
	for index := 0; index < len(namespaceID); index++ {
		character := namespaceID[index]
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			continue
		}
		if index > 0 && (character == '.' || character == '_' || character == '-') {
			continue
		}
		return false
	}
	return true
}

func (h *handler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	rewrittenPath, ok := h.rewritePath(request.Method, request.URL.Path)
	if !ok {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	outgoing := request.Clone(request.Context())
	outgoing.URL.Scheme = h.target.Scheme
	outgoing.URL.Host = h.target.Host
	outgoing.URL.Path = joinPath(h.target.Path, rewrittenPath)
	outgoing.URL.RawPath = ""
	outgoing.URL.RawQuery = request.URL.RawQuery
	outgoing.URL.ForceQuery = request.URL.ForceQuery
	outgoing.URL.Fragment = ""
	outgoing.RequestURI = ""
	outgoing.Host = h.target.Host
	outgoing.Close = false
	outgoing.Header = request.Header.Clone()
	removeHopByHopHeaders(outgoing.Header)
	// Remove the browser-facing host and application cookies before forwarding.
	outgoing.Header.Del("Host")
	outgoing.Header.Del("Cookie")
	outgoing.Header.Set("Authorization", "Bearer "+h.token)
	if outgoing.Header.Get("User-Agent") == "" {
		outgoing.Header.Set("User-Agent", "")
	}

	response, err := h.transport.RoundTrip(outgoing)
	if err != nil {
		responseWriter.WriteHeader(http.StatusBadGateway)
		return
	}
	defer response.Body.Close()

	responseHeaders := response.Header.Clone()
	removeHopByHopHeaders(responseHeaders)
	// Do not forward LoonFS cookies to the application.
	responseHeaders.Del("Set-Cookie")
	copyHeaders(responseWriter.Header(), responseHeaders)
	responseWriter.WriteHeader(response.StatusCode)
	_, _ = io.Copy(responseWriter, response.Body)
}

func (h *handler) rewritePath(method, path string) (string, bool) {
	for _, candidate := range routes {
		if candidate.method != method {
			continue
		}
		mount, ok := matchPattern(candidate.pattern, path)
		if !ok {
			continue
		}
		if mount == "" {
			return path, true
		}
		namespaceID, ok := h.mounts[mount]
		if !ok {
			return "", false
		}
		prefix := "/v0/mounts/" + mount
		return "/v0/namespaces/" + namespaceID + strings.TrimPrefix(path, prefix), true
	}
	return "", false
}

func matchPattern(pattern, path string) (string, bool) {
	patternSegments := strings.Split(pattern, "/")
	pathSegments := strings.Split(path, "/")
	if len(patternSegments) != len(pathSegments) {
		return "", false
	}

	var mount string
	for index, patternSegment := range patternSegments {
		pathSegment := pathSegments[index]
		switch patternSegment {
		case "{mount}":
			if pathSegment == "" {
				return "", false
			}
			mount = pathSegment
		case "{upload_id}":
			if pathSegment == "" {
				return "", false
			}
		default:
			if pathSegment != patternSegment {
				return "", false
			}
		}
	}
	return mount, true
}

func joinPath(basePath, requestPath string) string {
	if basePath == "" || basePath == "/" {
		return requestPath
	}
	return strings.TrimSuffix(basePath, "/") + requestPath
}

func removeHopByHopHeaders(headers http.Header) {
	for _, connection := range headers.Values("Connection") {
		for _, name := range strings.Split(connection, ",") {
			headers.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range hopByHopHeaders {
		headers.Del(name)
	}
}

func copyHeaders(destination, source http.Header) {
	for name, values := range source {
		for _, value := range values {
			destination.Add(name, value)
		}
	}
}
