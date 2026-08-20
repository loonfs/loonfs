export interface ProxyConfig {
    serverBaseUrl: string;
    token: string;
    mounts: Record<string, string>;
}

interface ProxyRoute {
    operation: string;
    method: "GET" | "POST" | "PUT";
    pattern: RegExp;
    mountScoped: boolean;
}

interface MatchedRoute {
    route: ProxyRoute;
    match: RegExpMatchArray;
}

const HOP_BY_HOP_HEADERS = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
] as const;

// These routes must match docs/specs/openapi-proxy.json.
interface ProxyRouteTemplate {
    operation: string;
    method: "GET" | "POST" | "PUT";
    template: string;
}

const PROXY_ROUTE_TABLE: readonly ProxyRouteTemplate[] = [
    { operation: "capabilities", method: "GET", template: "/v0/capabilities" },
    { operation: "list_changes", method: "GET", template: "/v0/mounts/{mount}/changes" },
    { operation: "apply_commit", method: "POST", template: "/v0/mounts/{mount}/commits" },
    { operation: "get_file_bytes", method: "GET", template: "/v0/mounts/{mount}/filesystem/content" },
    { operation: "begin_download", method: "POST", template: "/v0/mounts/{mount}/filesystem/downloads" },
    { operation: "list_path_entries", method: "GET", template: "/v0/mounts/{mount}/filesystem/list" },
    { operation: "list_file_revisions", method: "GET", template: "/v0/mounts/{mount}/filesystem/revisions" },
    { operation: "stat_path", method: "GET", template: "/v0/mounts/{mount}/filesystem/stat" },
    { operation: "list_trash", method: "GET", template: "/v0/mounts/{mount}/filesystem/trash" },
    { operation: "grep", method: "GET", template: "/v0/mounts/{mount}/query/grep" },
    { operation: "begin_upload", method: "POST", template: "/v0/mounts/{mount}/uploads" },
    { operation: "get_upload_status", method: "GET", template: "/v0/mounts/{mount}/uploads/{upload_id}" },
    { operation: "abort_upload", method: "POST", template: "/v0/mounts/{mount}/uploads/{upload_id}/abort" },
    { operation: "complete_upload", method: "POST", template: "/v0/mounts/{mount}/uploads/{upload_id}/complete" },
    { operation: "upload_content", method: "PUT", template: "/v0/mounts/{mount}/uploads/{upload_id}/content" },
    { operation: "sign_upload_parts", method: "POST", template: "/v0/mounts/{mount}/uploads/{upload_id}/parts" },
];

function patternFor(template: string): RegExp {
    const source = template
        .split("/")
        .map((segment) => {
            if (segment === "{mount}") {
                return "(?<mount>[^/]+)";
            }
            if (segment.startsWith("{") && segment.endsWith("}")) {
                return "[^/]+";
            }
            return segment.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        })
        .join("/");
    return new RegExp(`^${source}$`);
}

const PROXY_ROUTES: readonly ProxyRoute[] = PROXY_ROUTE_TABLE.map((entry) => ({
    operation: entry.operation,
    method: entry.method,
    pattern: patternFor(entry.template),
    mountScoped: entry.template.includes("{mount}"),
}));

/** Creates a fetch-compatible handler for LoonFS browser requests. */
export function createProxyHandler(config: ProxyConfig): (request: Request) => Promise<Response> {
    const serverBaseUrl = new URL(config.serverBaseUrl);
    serverBaseUrl.search = "";
    serverBaseUrl.hash = "";
    const serverBasePath = serverBaseUrl.pathname.replace(/\/+$/, "");
    const token = config.token;
    const mounts = new Map(Object.entries(config.mounts));

    return async (request: Request): Promise<Response> => {
        const requestUrl = new URL(request.url);
        const matched = matchRoute(request.method, requestUrl.pathname);
        if (matched === undefined) {
            return notFound();
        }

        const upstreamPath = rewritePath(requestUrl.pathname, matched, mounts);
        if (upstreamPath === undefined) {
            return notFound();
        }

        const headers = forwardedHeaders(request.headers, REQUEST_STRIPPED_HEADERS);
        headers.set("authorization", `Bearer ${token}`);
        const init: RequestInit & { duplex?: "half" } = {
            method: request.method,
            headers,
            redirect: "manual",
            signal: request.signal,
        };
        if (request.body !== null) {
            init.body = request.body;
            init.duplex = "half";
        }

        const upstreamUrl = new URL(serverBaseUrl);
        upstreamUrl.pathname = `${serverBasePath}${upstreamPath}`;
        upstreamUrl.search = requestUrl.search;
        const upstream = await fetch(upstreamUrl, init);
        return new Response(upstream.body, {
            status: upstream.status,
            statusText: upstream.statusText,
            headers: forwardedHeaders(upstream.headers, RESPONSE_STRIPPED_HEADERS),
        });
    };
}

function matchRoute(method: string, pathname: string): MatchedRoute | undefined {
    for (const route of PROXY_ROUTES) {
        if (route.method !== method) {
            continue;
        }
        const match = pathname.match(route.pattern);
        if (match !== null) {
            return { route, match };
        }
    }
    return undefined;
}

function rewritePath(
    pathname: string,
    matched: MatchedRoute,
    mounts: ReadonlyMap<string, string>,
): string | undefined {
    if (!matched.route.mountScoped) {
        return pathname;
    }

    const encodedMount = matched.match.groups?.mount;
    if (encodedMount === undefined) {
        return undefined;
    }
    let mount: string;
    try {
        mount = decodeURIComponent(encodedMount);
    } catch {
        return undefined;
    }
    const namespaceId = mounts.get(mount);
    if (namespaceId === undefined) {
        return undefined;
    }

    const mountPrefix = `/v0/mounts/${encodedMount}`;
    const suffix = pathname.slice(mountPrefix.length);
    return `/v0/namespaces/${encodeURIComponent(namespaceId)}${suffix}`;
}

function forwardedHeaders(source: Headers, extra: readonly string[]): Headers {
    const headers = new Headers(source);
    const connectionHeaders = headers
        .get("connection")
        ?.split(",")
        .map((name) => name.trim())
        .filter((name) => name !== "") ?? [];
    for (const name of [...HOP_BY_HOP_HEADERS, ...connectionHeaders, ...extra]) {
        headers.delete(name);
    }
    return headers;
}

// Do not forward application cookies to LoonFS.
const REQUEST_STRIPPED_HEADERS = ["cookie"] as const;
// Fetch decompresses responses, so remove the old encoding and length headers.
// Do not forward LoonFS cookies to the application.
const RESPONSE_STRIPPED_HEADERS = ["content-encoding", "content-length", "set-cookie"] as const;

function notFound(): Response {
    return new Response(null, { status: 404 });
}
