export interface ProxyRouteContext {
    method: string;
    template: string;
    /** The decoded namespace ID from the path. Absent for `/v0/capabilities`. */
    namespaceId?: string;
}

export interface ProxyAuthorization {
    /** Sent upstream in `Loonfs-Actor`. */
    actorId?: string;
    subjectId?: string;
    principalScope?: string;
    principals?: string[];
}

export interface ProxyConfig {
    serverBaseUrl: string;
    token: string;
    /**
     * Runs before every forwarded request. Return a Response to refuse it.
     * Required; returning an empty authorization forwards as the token holder.
     * The proxy forwards any namespace ID in the path, so check
     * `context.namespaceId` here.
     */
    authorize: (
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

// These routes must match the `routes` list in
// crates/loonfs-conformance/cases/proxy.json.
interface ProxyRouteTemplate {
    method: "GET" | "POST" | "PUT" | "DELETE";
    template: string;
}

const PROXY_ROUTE_TABLE: readonly ProxyRouteTemplate[] = [
    { method: "GET", template: "/v0/capabilities" },
    { method: "GET", template: "/v0/namespaces/{namespace_id}/changes" },
    { method: "POST", template: "/v0/namespaces/{namespace_id}/commits" },
    { method: "GET", template: "/v0/namespaces/{namespace_id}/filesystem/content" },
    { method: "POST", template: "/v0/namespaces/{namespace_id}/filesystem/downloads" },
    { method: "GET", template: "/v0/namespaces/{namespace_id}/filesystem/entries" },
    { method: "GET", template: "/v0/namespaces/{namespace_id}/filesystem/entry" },
    { method: "GET", template: "/v0/namespaces/{namespace_id}/filesystem/revisions" },
    { method: "GET", template: "/v0/namespaces/{namespace_id}/filesystem/trash" },
    { method: "GET", template: "/v0/namespaces/{namespace_id}/grep" },
    { method: "GET", template: "/v0/namespaces/{namespace_id}/snapshots" },
    { method: "POST", template: "/v0/namespaces/{namespace_id}/snapshots" },
    { method: "POST", template: "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend" },
    { method: "GET", template: "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}" },
    { method: "DELETE", template: "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}" },
    { method: "POST", template: "/v0/namespaces/{namespace_id}/uploads" },
    { method: "GET", template: "/v0/namespaces/{namespace_id}/uploads/{upload_id}" },
    { method: "POST", template: "/v0/namespaces/{namespace_id}/uploads/{upload_id}/abort" },
    { method: "POST", template: "/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete" },
    { method: "PUT", template: "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content" },
    { method: "POST", template: "/v0/namespaces/{namespace_id}/uploads/{upload_id}/parts" },
];

function patternFor(template: string): RegExp {
    const source = template
        .split("/")
        .map((segment) => {
            if (segment === "{namespace_id}") {
                return "(?<namespaceId>[^/]+)";
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
    const authorize = config.authorize;
    if (typeof authorize !== "function") {
        throw new TypeError("authorize must be a function");
    }

    return async (request: Request): Promise<Response> => {
        const requestUrl = new URL(request.url);
        const context = resolveRoute(request.method, requestUrl.pathname);
        if (context === undefined) {
            return notFound();
        }

        const authorization = await authorize(request, context);
        if (authorization instanceof Response) {
            return authorization;
        }
        if ((authorization.principalScope === undefined) !== (authorization.principals === undefined)) {
            return new Response("proxy authorization must set principalScope and principals together", {
                status: 500,
            });
        }

        const headers = forwardedHeaders(request.headers, REQUEST_STRIPPED_HEADERS);
        headers.set("authorization", `Bearer ${token}`);
        if (authorization.actorId !== undefined) {
            headers.set("Loonfs-Actor", authorization.actorId);
        }
        if (authorization.subjectId !== undefined) {
            headers.set("Loonfs-Subject", authorization.subjectId);
        }
        if (authorization.principalScope !== undefined) {
            headers.set("Loonfs-Principal-Scope", authorization.principalScope);
        }
        if (authorization.principals !== undefined) {
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
        upstreamUrl.pathname = `${serverBasePath}${requestUrl.pathname}`;
        upstreamUrl.search = requestUrl.search;
        const upstream = await fetch(upstreamUrl, init);
        return new Response(upstream.body, {
            status: upstream.status,
            statusText: upstream.statusText,
            headers: forwardedHeaders(upstream.headers, RESPONSE_STRIPPED_HEADERS),
        });
    };
}

function resolveRoute(method: string, pathname: string): ProxyRouteContext | undefined {
    for (const route of PROXY_ROUTES) {
        if (route.method !== method) {
            continue;
        }
        const match = pathname.match(route.pattern);
        if (match === null) {
            continue;
        }
        const context: ProxyRouteContext = { method, template: route.template };
        const encodedNamespaceId = match.groups?.namespaceId;
        if (encodedNamespaceId !== undefined) {
            try {
                context.namespaceId = decodeURIComponent(encodedNamespaceId);
            } catch {
                return undefined;
            }
        }
        return context;
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
    "host",
    "cookie",
    "loonfs-actor",
    "loonfs-subject",
    "loonfs-principal-scope",
    "loonfs-principals",
] as const;
// Fetch decompresses responses, so remove the old encoding and length headers.
// Do not forward LoonFS cookies to the application.
const RESPONSE_STRIPPED_HEADERS = ["content-encoding", "content-length", "set-cookie"] as const;

function notFound(): Response {
    return new Response(null, { status: 404 });
}
