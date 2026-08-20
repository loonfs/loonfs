"""Framework-free ASGI proxy for mount-scoped LoonFS requests."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any
from urllib.parse import quote

import httpx

__all__ = ["LoonFSProxy", "ROUTES"]


_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]

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

# Source: docs/specs/openapi-proxy.json, whose operation set comes from
# PROXY_OPERATIONS in openapi_postprocess.rs.
ROUTES: tuple[tuple[str, str, str], ...] = (
    ("capabilities", "GET", "/v0/capabilities"),
    ("list_changes", "GET", "/v0/mounts/{mount}/changes"),
    ("apply_commit", "POST", "/v0/mounts/{mount}/commits"),
    ("get_file_bytes", "GET", "/v0/mounts/{mount}/filesystem/content"),
    ("begin_download", "POST", "/v0/mounts/{mount}/filesystem/downloads"),
    ("list_path_entries", "GET", "/v0/mounts/{mount}/filesystem/list"),
    ("list_file_revisions", "GET", "/v0/mounts/{mount}/filesystem/revisions"),
    ("stat_path", "GET", "/v0/mounts/{mount}/filesystem/stat"),
    ("list_trash", "GET", "/v0/mounts/{mount}/filesystem/trash"),
    ("grep", "GET", "/v0/mounts/{mount}/query/grep"),
    ("begin_upload", "POST", "/v0/mounts/{mount}/uploads"),
    ("get_upload_status", "GET", "/v0/mounts/{mount}/uploads/{upload_id}"),
    ("abort_upload", "POST", "/v0/mounts/{mount}/uploads/{upload_id}/abort"),
    ("complete_upload", "POST", "/v0/mounts/{mount}/uploads/{upload_id}/complete"),
    ("upload_content", "PUT", "/v0/mounts/{mount}/uploads/{upload_id}/content"),
    ("sign_upload_parts", "POST", "/v0/mounts/{mount}/uploads/{upload_id}/parts"),
)


def _pattern_for(template: str) -> re.Pattern[str]:
    parts = []
    for segment in template.split("/"):
        if segment == "{mount}":
            parts.append(r"(?P<mount>[^/]+)")
        elif segment.startswith("{") and segment.endswith("}"):
            parts.append(r"[^/]+")
        else:
            parts.append(re.escape(segment))
    return re.compile("/".join(parts))


_COMPILED_ROUTES = tuple(
    (operation, method, _pattern_for(template))
    for operation, method, template in ROUTES
)


def _connection_headers(headers: list[tuple[bytes, bytes]]) -> set[bytes]:
    names: set[bytes] = set()
    for name, value in headers:
        if name.lower() != b"connection":
            continue
        names.update(part.strip().lower() for part in value.split(b",") if part.strip())
    return names


# The host belongs to the configured server, and application cookies never
# cross into LoonFS requests.
_REQUEST_EXCLUDED_HEADERS = frozenset({b"host", b"cookie"})
# Session cookies never cross back to the application.
_RESPONSE_EXCLUDED_HEADERS = frozenset({b"set-cookie"})


def _forwarded_headers(
    headers: list[tuple[bytes, bytes]],
    extra_excluded: frozenset[bytes],
    authorization: bytes | None = None,
) -> list[tuple[bytes, bytes]]:
    excluded = _HOP_BY_HOP_HEADERS | _connection_headers(headers) | extra_excluded
    forwarded = [
        (name, value)
        for name, value in headers
        if name.lower() not in excluded
        and (authorization is None or name.lower() != b"authorization")
    ]
    if authorization is not None:
        forwarded.append((b"authorization", authorization))
    return forwarded


async def _request_body(receive: _Receive) -> AsyncIterator[bytes]:
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return
        if message["type"] != "http.request":
            continue
        body = message.get("body", b"")
        if body:
            yield body
        if not message.get("more_body", False):
            return


class LoonFSProxy:
    """Map approved mount routes to namespace routes and forward them."""

    def __init__(
        self,
        server_base_url: str,
        token: str,
        mounts: dict[str, str],
    ) -> None:
        self._server_base_url = server_base_url.rstrip("/")
        self._authorization = f"Bearer {token}".encode("latin-1")
        self._mounts = dict(mounts)
        self._client = httpx.AsyncClient(timeout=None, follow_redirects=False)

    async def __call__(self, scope: dict[str, Any], receive: _Receive, send: _Send) -> None:
        if scope["type"] == "lifespan":
            await self._lifespan(receive, send)
            return
        if scope["type"] != "http":
            return

        rewritten_path = self._rewritten_path(scope["method"], scope["path"])
        if rewritten_path is None:
            await self._not_found(send)
            return

        target = httpx.URL(f"{self._server_base_url}{rewritten_path}").copy_with(
            query=scope.get("query_string", b"")
        )
        headers = _forwarded_headers(
            scope.get("headers", []), _REQUEST_EXCLUDED_HEADERS, self._authorization
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

    def _rewritten_path(self, method: str, path: str) -> str | None:
        for _operation, route_method, pattern in _COMPILED_ROUTES:
            if method != route_method:
                continue
            match = pattern.fullmatch(path)
            if match is None:
                continue
            mount = match.groupdict().get("mount")
            if mount is None:
                return path
            namespace_id = self._mounts.get(mount)
            if namespace_id is None:
                return None
            mount_prefix = f"/v0/mounts/{mount}"
            return f"/v0/namespaces/{quote(namespace_id, safe='')}{path[len(mount_prefix):]}"
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
    async def _not_found(send: _Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 404,
                "headers": [(b"content-length", b"0")],
            }
        )
        await send({"type": "http.response.body", "body": b""})
