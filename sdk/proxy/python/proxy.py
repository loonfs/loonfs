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
    namespace_alias: str | None
    namespace_id: str | None


@dataclass(frozen=True)
class ProxyAuthorization:
    actor_id: str | None = None
    subject_id: str | None = None
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

# These routes must match docs/specs/openapi-proxy.json.
_ROUTE_TEMPLATES: tuple[tuple[str, str], ...] = (
    ("GET", "/v0/capabilities"),
    ("GET", "/v0/namespace-aliases/{namespace_alias}/changes"),
    ("POST", "/v0/namespace-aliases/{namespace_alias}/commits"),
    ("GET", "/v0/namespace-aliases/{namespace_alias}/filesystem/content"),
    ("POST", "/v0/namespace-aliases/{namespace_alias}/filesystem/downloads"),
    ("GET", "/v0/namespace-aliases/{namespace_alias}/filesystem/entries"),
    ("GET", "/v0/namespace-aliases/{namespace_alias}/filesystem/entry"),
    ("GET", "/v0/namespace-aliases/{namespace_alias}/filesystem/revisions"),
    ("GET", "/v0/namespace-aliases/{namespace_alias}/filesystem/trash"),
    ("GET", "/v0/namespace-aliases/{namespace_alias}/grep"),
    ("GET", "/v0/namespace-aliases/{namespace_alias}/snapshots"),
    ("POST", "/v0/namespace-aliases/{namespace_alias}/snapshots"),
    ("POST", "/v0/namespace-aliases/{namespace_alias}/snapshots/{snapshot_id}/extend"),
    ("DELETE", "/v0/namespace-aliases/{namespace_alias}/snapshots/{snapshot_id}"),
    ("POST", "/v0/namespace-aliases/{namespace_alias}/uploads"),
    ("GET", "/v0/namespace-aliases/{namespace_alias}/uploads/{upload_id}"),
    ("POST", "/v0/namespace-aliases/{namespace_alias}/uploads/{upload_id}/abort"),
    ("POST", "/v0/namespace-aliases/{namespace_alias}/uploads/{upload_id}/complete"),
    ("PUT", "/v0/namespace-aliases/{namespace_alias}/uploads/{upload_id}/content"),
    ("POST", "/v0/namespace-aliases/{namespace_alias}/uploads/{upload_id}/parts"),
)


def _pattern_for(template: str) -> re.Pattern[str]:
    parts = []
    for segment in template.split("/"):
        if segment == "{namespace_alias}":
            parts.append(r"(?P<namespace_alias>[^/]+)")
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
    """Forward allowed browser routes to their configured namespaces."""

    def __init__(
        self,
        server_base_url: str,
        token: str,
        namespace_aliases: dict[str, str],
        *,
        authorize: Callable[
            [dict[str, Any], ProxyRouteContext], Awaitable[ProxyAuthorization | ProxyRefusal]
        ] | None = None,
    ) -> None:
        self._server_base_url = server_base_url.rstrip("/")
        self._authorization = f"Bearer {token}".encode("latin-1")
        self._namespace_aliases = dict(namespace_aliases)
        self._authorize = authorize
        self._client = httpx.AsyncClient(timeout=None, follow_redirects=False)

    async def __call__(self, scope: dict[str, Any], receive: _Receive, send: _Send) -> None:
        if scope["type"] == "lifespan":
            await self._lifespan(receive, send)
            return
        if scope["type"] != "http":
            return

        resolved = self._resolve_route(scope["method"], scope["path"])
        if resolved is None:
            await self._not_found(send)
            return
        rewritten_path, context = resolved
        authorization = ProxyAuthorization()
        if self._authorize is not None:
            authorization = await self._authorize(scope, context)
            if isinstance(authorization, ProxyRefusal):
                await self._refuse(send, authorization)
                return

        target = httpx.URL(f"{self._server_base_url}{rewritten_path}").copy_with(
            query=scope.get("query_string", b"")
        )
        headers = _forwarded_headers(scope.get("headers", []), _REQUEST_EXCLUDED_HEADERS)
        headers.append((b"authorization", self._authorization))
        if authorization.actor_id is not None:
            headers.append((b"loonfs-actor", authorization.actor_id.encode("ascii")))
        if authorization.subject_id is not None:
            headers.append((b"loonfs-subject", authorization.subject_id.encode("ascii")))
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

    def _resolve_route(self, method: str, path: str) -> tuple[str, ProxyRouteContext] | None:
        for route_method, template, pattern in _COMPILED_ROUTES:
            if method != route_method:
                continue
            match = pattern.fullmatch(path)
            if match is None:
                continue
            namespace_alias = match.groupdict().get("namespace_alias")
            if namespace_alias is None:
                return path, ProxyRouteContext(method, template, None, None)
            namespace_id = self._namespace_aliases.get(namespace_alias)
            if namespace_id is None:
                return None
            namespace_alias_prefix = f"/v0/namespace-aliases/{namespace_alias}"
            rewritten_path = f"/v0/namespaces/{quote(namespace_id, safe='')}{path[len(namespace_alias_prefix):]}"
            return rewritten_path, ProxyRouteContext(
                method, template, namespace_alias, namespace_id
            )
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
