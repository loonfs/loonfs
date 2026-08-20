export interface ProxyConfig {
    serverBaseUrl: string;
    token: string;
    namespaceAliases: Record<string, string>;
}

interface ProxyRoute {
    method: "GET" | "POST" | "PUT";
    pattern: RegExp;
    namespaceAliasScoped: boolean;
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
    method: "GET" | "POST" | "PUT";
    template: string;
}

const PROXY_ROUTE_TABLE: readonly ProxyRouteTemplate[] = [
    { method: "GET", template: "/v0/capabilities" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/changes" },
    { method: "POST", template: "/v0/namespace-aliases/{namespace_alias}/commits" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/content" },
    { method: "POST", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/downloads" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/list" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/revisions" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/stat" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/trash" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/query/grep" },
    { method: "POST", template: "/v0/namespace-aliases/{namespace_alias}/uploads" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/uploads/{upload_id}" },
    { method: "POST", template: "/v0/namespace-aliases/{namespace_alias}/uploads/{upload_id}/abort" },
    { method: "POST", template: "/v0/namespace-aliases/{namespace_alias}/uploads/{upload_id}/complete" },
    { method: "PUT", template: "/v0/namespace-aliases/{namespace_alias}/uploads/{upload_id}/content" },
    { method: "POST", template: "/v0/namespace-aliases/{namespace_alias}/uploads/{upload_id}/parts" },
];

function patternFor(template: string): RegExp {
    const source = template
        .split("/")
        .map((segment) => {
            if (segment === "{namespace_alias}") {
                return "(?<namespaceAlias>[^/]+)";
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
    method: entry.method,
    pattern: patternFor(entry.template),
    namespaceAliasScoped: entry.template.includes("{namespace_alias}"),
}));

/** Creates a fetch-compatible handler for LoonFS browser requests. */
export function createProxyHandler(config: ProxyConfig): (request: Request) => Promise<Response> {
    const serverBaseUrl = new URL(config.serverBaseUrl);
    serverBaseUrl.search = "";
    serverBaseUrl.hash = "";
    const serverBasePath = serverBaseUrl.pathname.replace(/\/+$/, "");
    const token = config.token;
    const namespaceAliases = new Map(Object.entries(config.namespaceAliases));

    return async (request: Request): Promise<Response> => {
        const requestUrl = new URL(request.url);
        const matched = matchRoute(request.method, requestUrl.pathname);
        if (matched === undefined) {
            return notFound();
        }

        const upstreamPath = rewritePath(requestUrl.pathname, matched, namespaceAliases);
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
    namespaceAliases: ReadonlyMap<string, string>,
): string | undefined {
    if (!matched.route.namespaceAliasScoped) {
        return pathname;
    }

    const encodedNamespaceAlias = matched.match.groups?.namespaceAlias;
    if (encodedNamespaceAlias === undefined) {
        return undefined;
    }
    let namespaceAlias: string;
    try {
        namespaceAlias = decodeURIComponent(encodedNamespaceAlias);
    } catch {
        return undefined;
    }
    const namespaceId = namespaceAliases.get(namespaceAlias);
    if (namespaceId === undefined) {
        return undefined;
    }

    const namespaceAliasPrefix = `/v0/namespace-aliases/${encodedNamespaceAlias}`;
    const suffix = pathname.slice(namespaceAliasPrefix.length);
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
