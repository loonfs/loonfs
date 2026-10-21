"""ASGI handler for forwarding LoonFS browser requests."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict
from urllib.parse import quote

import httpx

__all__ = ["LoonFSProxy", "ProxyRouteContext", "ProxyAuthorization", "ProxyRefusal"]


@dataclass(frozen=True)
class ProxyRouteContext:
    method: str
    template: str
    namespace_id: str | None


@dataclass(frozen=True)
class ProxyAuthorization:
    actor_id: str | None = None
    subject_id: str | None = None
    principal_scope: str | None = None
    principals: list[str] | None = None


@dataclass(frozen=True)
class ProxyRefusal:
    status: int
    body: bytes = b""
    content_type: str | None = None


# Python 3.8 requires typing aliases for runtime-evaluated generics.
_Receive = Callable[[], Awaitable[Dict[str, Any]]]
_Send = Callable[[Dict[str, Any]], Awaitable[None]]

_HOP_BY_HOP_HEADERS = frozenset(
    {
        b"connection",
        b"keep-alive",
        b"proxy-authenticate",
        b"proxy-authorization",
        b"proxy-connection",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
    }
)

# These routes must match the `routes` list in
# crates/loonfs-conformance/cases/proxy.json.
_ROUTE_TEMPLATES: tuple[tuple[str, str], ...] = (
    ("GET", "/v0/capabilities"),
    ("GET", "/v0/namespaces/{namespace_id}/changes"),
    ("POST", "/v0/namespaces/{namespace_id}/commits"),
    ("GET", "/v0/namespaces/{namespace_id}/filesystem/content"),
    ("POST", "/v0/namespaces/{namespace_id}/filesystem/downloads"),
    ("GET", "/v0/namespaces/{namespace_id}/filesystem/entries"),
    ("GET", "/v0/namespaces/{namespace_id}/filesystem/entry"),
    ("GET", "/v0/namespaces/{namespace_id}/filesystem/revisions"),
    ("GET", "/v0/namespaces/{namespace_id}/filesystem/trash"),
    ("GET", "/v0/namespaces/{namespace_id}/grep"),
    ("GET", "/v0/namespaces/{namespace_id}/snapshots"),
    ("POST", "/v0/namespaces/{namespace_id}/snapshots"),
    ("POST", "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend"),
    ("GET", "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}"),
    ("DELETE", "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}"),
    ("POST", "/v0/namespaces/{namespace_id}/uploads"),
    ("GET", "/v0/namespaces/{namespace_id}/uploads/{upload_id}"),
    ("POST", "/v0/namespaces/{namespace_id}/uploads/{upload_id}/abort"),
    ("POST", "/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete"),
    ("PUT", "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content"),
    ("POST", "/v0/namespaces/{namespace_id}/uploads/{upload_id}/parts"),
)


def _pattern_for(template: str) -> re.Pattern[str]:
    parts = []
    for segment in template.split("/"):
        if segment == "{namespace_id}":
            parts.append(r"(?P<namespace_id>[^/]+)")
        elif segment.startswith("{") and segment.endswith("}"):
            parts.append(r"[^/]+")
        else:
            parts.append(re.escape(segment))
    return re.compile("/".join(parts))


_COMPILED_ROUTES = tuple(
    (method, template, _pattern_for(template)) for method, template in _ROUTE_TEMPLATES
)


def _connection_headers(headers: list[tuple[bytes, bytes]]) -> set[bytes]:
    names: set[bytes] = set()
    for name, value in headers:
        if name.lower() != b"connection":
            continue
        names.update(part.strip().lower() for part in value.split(b",") if part.strip())
    return names


# Remove the browser-facing host and application cookies before forwarding.
_REQUEST_EXCLUDED_HEADERS = frozenset(
    {
        b"host",
        b"cookie",
        b"authorization",
        b"loonfs-actor",
        b"loonfs-subject",
        b"loonfs-principal-scope",
        b"loonfs-principals",
    }
)
# Do not forward LoonFS cookies to the application.
_RESPONSE_EXCLUDED_HEADERS = frozenset({b"set-cookie"})


def _forwarded_headers(
    headers: list[tuple[bytes, bytes]],
    extra_excluded: frozenset[bytes],
) -> list[tuple[bytes, bytes]]:
    excluded = _HOP_BY_HOP_HEADERS | _connection_headers(headers) | extra_excluded
    return [
        (name, value)
        for name, value in headers
        if name.lower() not in excluded
    ]


async def _request_body(receive: _Receive) -> AsyncIterator[bytes]:
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return
        body = message.get("body", b"")
        if body:
            yield body
        if not message.get("more_body", False):
            return


class LoonFSProxy:
    """Forward allowed browser routes to a LoonFS server."""

    def __init__(
        self,
        server_base_url: str,
        token: str,
        *,
        authorize: Callable[
            [dict[str, Any], ProxyRouteContext], Awaitable[ProxyAuthorization | ProxyRefusal]
        ],
    ) -> None:
        """The authorize hook is required and runs before every forwarded request.

        Return a ProxyRefusal to refuse it.
        Returning an empty ProxyAuthorization forwards as the token holder.
        The proxy forwards any namespace ID in the path, so check
        `context.namespace_id` here.
        """
        if authorize is None:
            raise TypeError("authorize is required")
        self._server_base_url = server_base_url.rstrip("/")
        self._authorization = f"Bearer {token}".encode("latin-1")
        self._authorize = authorize
        self._client = httpx.AsyncClient(timeout=None, follow_redirects=False)

    async def __call__(self, scope: dict[str, Any], receive: _Receive, send: _Send) -> None:
        if scope["type"] == "lifespan":
            await self._lifespan(receive, send)
            return
        if scope["type"] != "http":
            return

        context = self._resolve_route(scope["method"], scope["path"])
        if context is None:
            await self._not_found(send)
            return
        authorization = await self._authorize(scope, context)
        if isinstance(authorization, ProxyRefusal):
            await self._refuse(send, authorization)
            return
        if (authorization.principal_scope is None) != (authorization.principals is None):
            await self._refuse(
                send,
                ProxyRefusal(
                    500,
                    b"proxy authorization must set principal_scope and principals together",
                    "text/plain",
                ),
            )
            return

        # The ASGI path is decoded. Quoting it again keeps a decoded `?` or `#`
        # inside its segment, so LoonFS receives the route that the hook approved.
        target = httpx.URL(f"{self._server_base_url}{quote(scope['path'])}").copy_with(
            query=scope.get("query_string", b"")
        )
        headers = _forwarded_headers(scope.get("headers", []), _REQUEST_EXCLUDED_HEADERS)
        headers.append((b"authorization", self._authorization))
        if authorization.actor_id is not None:
            headers.append((b"loonfs-actor", authorization.actor_id.encode("ascii")))
        if authorization.subject_id is not None:
            headers.append((b"loonfs-subject", authorization.subject_id.encode("ascii")))
        if authorization.principal_scope is not None:
            headers.append(
                (b"loonfs-principal-scope", authorization.principal_scope.encode("ascii"))
            )
        if authorization.principals is not None:
            headers.append(
                (b"loonfs-principals", ",".join(authorization.principals).encode("ascii"))
            )
        request = self._client.build_request(
            scope["method"],
            target,
            headers=headers,
            content=_request_body(receive),
        )
        response = await self._client.send(request, stream=True)
        try:
            await send(
                {
                    "type": "http.response.start",
                    "status": response.status_code,
                    "headers": _forwarded_headers(
                        response.headers.raw, _RESPONSE_EXCLUDED_HEADERS
                    ),
                }
            )
            async for chunk in response.aiter_raw():
                await send(
                    {
                        "type": "http.response.body",
                        "body": chunk,
                        "more_body": True,
                    }
                )
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        finally:
            await response.aclose()

    @staticmethod
    def _resolve_route(method: str, path: str) -> ProxyRouteContext | None:
        # httpx removes `.` and `..` segments from the forwarded URL, which would
        # change the route after the hook approved it.
        if any(segment in {".", ".."} for segment in path.split("/")):
            return None
        for route_method, template, pattern in _COMPILED_ROUTES:
            if method != route_method:
                continue
            match = pattern.fullmatch(path)
            if match is None:
                continue
            return ProxyRouteContext(method, template, match.groupdict().get("namespace_id"))
        return None

    async def _lifespan(self, receive: _Receive, send: _Send) -> None:
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await self._client.aclose()
                await send({"type": "lifespan.shutdown.complete"})
                return

    @staticmethod
    async def _refuse(send: _Send, refusal: ProxyRefusal) -> None:
        headers = [(b"content-length", str(len(refusal.body)).encode("ascii"))]
        if refusal.content_type is not None:
            headers.append((b"content-type", refusal.content_type.encode("latin-1")))
        await send(
            {"type": "http.response.start", "status": refusal.status, "headers": headers}
        )
        await send({"type": "http.response.body", "body": refusal.body})

    @staticmethod
    async def _not_found(send: _Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 404,
                "headers": [(b"content-length", b"0")],
            }
        )
        await send({"type": "http.response.body", "body": b""})
