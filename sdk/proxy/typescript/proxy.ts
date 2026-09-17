export interface ProxyRouteContext {
    method: string;
    template: string;
    namespaceAlias?: string;
    namespaceId?: string;
}

export interface ProxyAuthorization {
    /** Sent upstream in `Loonfs-Actor`. */
    actorId?: string;
    subjectId?: string;
    principals?: string[];
}

export interface ProxyConfig {
    serverBaseUrl: string;
    token: string;
    namespaceAliases: Record<string, string>;
    /** Runs before every forwarded request. Return a Response to refuse it. */
    authorize?: (
        request: Request,
        context: ProxyRouteContext,
    ) => ProxyAuthorization | Response | Promise<ProxyAuthorization | Response>;
}

interface ProxyRoute extends ProxyRouteTemplate {
    pattern: RegExp;
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
    method: "GET" | "POST" | "PUT" | "DELETE";
    template: string;
}

const PROXY_ROUTE_TABLE: readonly ProxyRouteTemplate[] = [
    { method: "GET", template: "/v0/capabilities" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/changes" },
    { method: "POST", template: "/v0/namespace-aliases/{namespace_alias}/commits" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/content" },
    { method: "POST", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/downloads" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/entries" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/entry" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/revisions" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/filesystem/trash" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/grep" },
    { method: "GET", template: "/v0/namespace-aliases/{namespace_alias}/snapshots" },
    { method: "POST", template: "/v0/namespace-aliases/{namespace_alias}/snapshots" },
    { method: "POST", template: "/v0/namespace-aliases/{namespace_alias}/snapshots/{snapshot_id}/extend" },
    { method: "DELETE", template: "/v0/namespace-aliases/{namespace_alias}/snapshots/{snapshot_id}" },
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
    ...entry,
    pattern: patternFor(entry.template),
}));

/** Creates a fetch-compatible handler for LoonFS browser requests. */
export function createProxyHandler(config: ProxyConfig): (request: Request) => Promise<Response> {
    const serverBaseUrl = new URL(config.serverBaseUrl);
    const serverBasePath = serverBaseUrl.pathname.replace(/\/+$/, "");
    const token = config.token;
    const namespaceAliases = new Map(Object.entries(config.namespaceAliases));
    const authorize = config.authorize;

    return async (request: Request): Promise<Response> => {
        const requestUrl = new URL(request.url);
        const resolved = resolveRoute(request.method, requestUrl.pathname, namespaceAliases);
        if (resolved === undefined) {
            return notFound();
        }

        const authorization = await authorize?.(request, resolved.context);
        if (authorization instanceof Response) {
            return authorization;
        }

        const headers = forwardedHeaders(request.headers, REQUEST_STRIPPED_HEADERS);
        headers.set("authorization", `Bearer ${token}`);
        if (authorization?.actorId !== undefined) {
            headers.set("Loonfs-Actor", authorization.actorId);
        }
        if (authorization?.subjectId !== undefined) {
            headers.set("Loonfs-Subject", authorization.subjectId);
        }
        if (authorization?.principals !== undefined) {
            headers.set("Loonfs-Principals", authorization.principals.join(","));
        }
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
        upstreamUrl.pathname = `${serverBasePath}${resolved.path}`;
        upstreamUrl.search = requestUrl.search;
        const upstream = await fetch(upstreamUrl, init);
        return new Response(upstream.body, {
            status: upstream.status,
            statusText: upstream.statusText,
            headers: forwardedHeaders(upstream.headers, RESPONSE_STRIPPED_HEADERS),
        });
    };
}

function resolveRoute(
    method: string,
    pathname: string,
    namespaceAliases: ReadonlyMap<string, string>,
): { path: string; context: ProxyRouteContext } | undefined {
    for (const route of PROXY_ROUTES) {
        if (route.method !== method) {
            continue;
        }
        const match = pathname.match(route.pattern);
        if (match === null) {
            continue;
        }
        const context: ProxyRouteContext = { method, template: route.template };
        const encodedNamespaceAlias = match.groups?.namespaceAlias;
        if (encodedNamespaceAlias === undefined) {
            return { path: pathname, context };
        }
        try {
            context.namespaceAlias = decodeURIComponent(encodedNamespaceAlias);
        } catch {
            return undefined;
        }
        context.namespaceId = namespaceAliases.get(context.namespaceAlias);
        if (context.namespaceId === undefined) {
            return undefined;
        }

        const namespaceAliasPrefix = `/v0/namespace-aliases/${encodedNamespaceAlias}`;
        const suffix = pathname.slice(namespaceAliasPrefix.length);
        return {
            path: `/v0/namespaces/${encodeURIComponent(context.namespaceId)}${suffix}`,
            context,
        };
    }
    return undefined;
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
const REQUEST_STRIPPED_HEADERS = [
    "cookie",
    "loonfs-actor",
    "loonfs-subject",
    "loonfs-principals",
] as const;
// Fetch decompresses responses, so remove the old encoding and length headers.
// Do not forward LoonFS cookies to the application.
const RESPONSE_STRIPPED_HEADERS = ["content-encoding", "content-length", "set-cookie"] as const;

function notFound(): Response {
    return new Response(null, { status: 404 });
}
