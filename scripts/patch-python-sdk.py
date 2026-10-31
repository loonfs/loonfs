#!/usr/bin/env python3
"""Removes the streaming and deprecated options from the generated Python SDK, applies the LoonFS retry and error rules, skips change events of kinds that the SDK doesn't define, keeps path entries of inode kinds that the SDK doesn't define, and moves its entry point to `loonfs.server`."""

import json
import pathlib
import re


def remove_exact(source, snippet, expected_count, label):
    actual_count = source.count(snippet)
    assert actual_count == expected_count, (
        f"expected {expected_count} {label}, found {actual_count}"
    )
    return source.replace(snippet, "")


def replace_once(source, old, new):
    actual_count = source.count(old)
    assert actual_count == 1, f"expected exactly one match for {old!r}, found {actual_count}"
    return source.replace(old, new)


path = pathlib.Path("generated/python/core/pydantic_utilities.py")
source = path.read_text()
import_block = 'if TYPE_CHECKING:\n    from .http_sse._models import ServerSentEvent\n\n'
source = replace_once(source, import_block, "")
start = source.index("def parse_sse_obj(")
end = source.index("_type_adapter_cache")
path.write_text(source[:start] + source[end:])

serialization_path = pathlib.Path("generated/python/core/serialization.py")
source = serialization_path.read_text()
source = replace_once(
    source,
    "    # Models / TypedDicts: a field alias here means we must dealias; otherwise recurse into fields.\n",
    "    # Class variables are not response fields (including Pydantic's model_config).\n"
    "    if typing_extensions.get_origin(clean_type) is typing.ClassVar:\n"
    "        return False\n\n"
    "    # Models / TypedDicts: a field alias here means we must dealias; otherwise recurse into fields.\n",
)
serialization_path.write_text(source)

request_options_path = pathlib.Path("generated/python/core/request_options.py")
source = request_options_path.read_text()
source = replace_once(
    source,
    "        - timeout_in_seconds: int. Deprecated alias for `timeout`; both are in seconds. Prefer `timeout`.\n\n",
    "",
)
source = replace_once(source, "    timeout_in_seconds: NotRequired[int]\n", "")
source = remove_exact(
    source,
    "    stream_reconnection_enabled: NotRequired[bool]\n",
    1,
    "request stream_reconnection_enabled option",
)
source = remove_exact(
    source,
    "    max_stream_reconnection_attempts: NotRequired[int]\n",
    1,
    "request max_stream_reconnection_attempts option",
)
request_options_path.write_text(source)

http_client_path = pathlib.Path("generated/python/core/http_client.py")
source = http_client_path.read_text()
source = remove_exact(
    source,
    '            else request_options.get("timeout_in_seconds")\n'
    '            if request_options is not None and request_options.get("timeout_in_seconds") is not None\n',
    4,
    "http client timeout_in_seconds fallbacks",
)
source = replace_once(
    source,
    "def _should_retry(response: httpx.Response) -> bool:\n"
    "    return response.status_code >= 500 or response.status_code in [429, 408, 409]\n",
    "def _should_retry(response: httpx.Response) -> bool:\n"
    '    return "retry-after" in response.headers\n',
)
http_client_path.write_text(source)

api_error_path = pathlib.Path("generated/python/core/api_error.py")
source = api_error_path.read_text()
source = replace_once(
    source,
    "    def __str__(self) -> str:\n"
    '        return f"headers: {self.headers}, status_code: {self.status_code}, body: {self.body}"\n',
    '''    def __str__(self) -> str:
        if isinstance(self.body, dict):
            body = self.body
        else:
            body = {
                field: getattr(self.body, field, None)
                for field in ("code", "message", "param", "request_id", "details")
            }
        if body.get("code") is None or body.get("message") is None:
            return f"headers: {self.headers}, status_code: {self.status_code}, body: {self.body}"

        result = f"{body['code']}: {body['message']}"
        param = body.get("param")
        if param is not None:
            result += f" (param {param})"
        request_id = body.get("request_id")
        if request_id is not None:
            result += f" [request {request_id}]"
        details = body.get("details")
        if details is not None:
            if isinstance(details, dict):
                details = {key: value for key, value in details.items() if value is not None}
            else:
                details = details.model_dump(exclude_none=True)
            if details:
                result += f"\\ndetails: {details}"
        return result
''',
)
api_error_path.write_text(source)

# A client must skip a change event whose kind it doesn't know, so that a newer
# server can add kinds. The generated union rejects an unknown kind, which fails
# the whole response, so the commit model drops those events before validation.
openapi = json.loads(pathlib.Path("../docs/specs/openapi.json").read_text())
event_kinds = "".join(
    f'    "{kind}",\n'
    for kind in sorted(openapi["components"]["schemas"]["FilesystemChange"]["discriminator"]["mapping"])
)
commit_path = pathlib.Path("generated/python/types/commit.py")
source = commit_path.read_text()
source = replace_once(
    source,
    "from ..core.pydantic_utilities import IS_PYDANTIC_V2, UniversalBaseModel\n",
    "from ..core.pydantic_utilities import IS_PYDANTIC_V2, UniversalBaseModel, universal_field_validator\n",
)
source = replace_once(
    source,
    "from .namespace_id import NamespaceId\n",
    "from .namespace_id import NamespaceId\n"
    "\n"
    f"_EVENT_KINDS = frozenset({{\n{event_kinds}}})\n",
)
source = replace_once(
    source,
    "    if IS_PYDANTIC_V2:\n",
    "    @universal_field_validator(\"events\", pre=True)\n"
    "    def _skip_unknown_event_kinds(cls, events: typing.Any) -> typing.Any:\n"
    "        # A newer server can return an event kind that this SDK doesn't define, and the\n"
    "        # API requires a client to skip it. An event without a kind still fails validation.\n"
    "        if not isinstance(events, list):\n"
    "            return events\n"
    "        return [\n"
    "            event\n"
    "            for event in events\n"
    "            if not isinstance(event, dict)\n"
    "            or not isinstance(event.get(\"kind\"), str)\n"
    "            or event[\"kind\"] in _EVENT_KINDS\n"
    "        ]\n"
    "\n"
    "    if IS_PYDANTIC_V2:\n",
)
commit_path.write_text(source)

# A client must keep a path entry whose inode kind it doesn't know, and read the fields
# that every entry includes. The generated union rejects an unknown kind, which fails the
# whole stat or listing, so the patch wraps that union in one with a fallback member for
# the other kinds. The fallback member copies the directory member's fields, apart from
# the fields that only a directory entry includes.
schemas = openapi["components"]["schemas"]
entry_fields = {
    kind: set(schemas[reference.rsplit("/", 1)[1]]["properties"])
    for kind, reference in schemas["PathEntry"]["discriminator"]["mapping"].items()
}
directory_fields = entry_fields["dir"] - set.intersection(*entry_fields.values())
inode_kinds = "".join(f'    "{kind}",\n' for kind in sorted(entry_fields))
path_entry_path = pathlib.Path("generated/python/types/path_entry.py")
source = path_entry_path.read_text()
source = replace_once(
    source,
    "from ..core.pydantic_utilities import IS_PYDANTIC_V2, UniversalBaseModel\n",
    "from ..core.pydantic_utilities import IS_PYDANTIC_V2, UniversalBaseModel, universal_field_validator\n",
)
source = replace_once(
    source,
    "from .revision_no import RevisionNo\n",
    "from .revision_no import RevisionNo\n"
    "\n"
    f"_INODE_KINDS = frozenset({{\n{inode_kinds}}})\n",
)
directory_member = source[
    source.index("class PathEntry_Dir(UniversalBaseModel):\n") : source.index("class PathEntry_File(UniversalBaseModel):\n")
]
unknown_member, count = re.subn(
    r'    """\n.*?    """\n',
    '    """\n'
    "    An entry of an inode kind that this version of the SDK doesn't define, such as a kind that a\n"
    "    newer server added. It has the fields that every entry includes.\n"
    '    """\n',
    directory_member,
    count=1,
    flags=re.DOTALL,
)
assert count == 1, "expected a docstring on the directory member"
for field in sorted(directory_fields):
    unknown_member, count = re.subn(rf"    {field}: [^\n]*\n", "", unknown_member)
    assert count == 1, f"expected one `{field}` field on the directory member"
unknown_member = replace_once(unknown_member, "class PathEntry_Dir(", "class PathEntry_Unknown(")
unknown_member = replace_once(unknown_member, '    inode_kind: typing.Literal["dir"] = "dir"\n', "    inode_kind: str\n")
unknown_member = replace_once(
    unknown_member,
    "    if IS_PYDANTIC_V2:\n",
    "    @universal_field_validator(\"inode_kind\")\n"
    "    def _reject_defined_inode_kinds(cls, inode_kind: str) -> str:\n"
    "        # A directory or file entry decodes only as its own member, so an entry of a defined\n"
    "        # kind that's missing a field fails validation instead of decoding as this member.\n"
    "        if inode_kind in _INODE_KINDS:\n"
    "            raise ValueError(f\"an entry of kind {inode_kind!r} doesn't match that kind's fields\")\n"
    "        return inode_kind\n"
    "\n"
    "    if IS_PYDANTIC_V2:\n",
)
source = replace_once(
    source,
    "PathEntry = typing_extensions.Annotated[\n"
    '    typing.Union[PathEntry_Dir, PathEntry_File], pydantic.Field(discriminator="inode_kind")\n'
    "]\n",
    unknown_member + "PathEntry = typing.Union[\n"
    "    typing_extensions.Annotated[\n"
    '        typing.Union[PathEntry_Dir, PathEntry_File], pydantic.Field(discriminator="inode_kind")\n'
    "    ],\n"
    "    PathEntry_Unknown,\n"
    "]\n",
)
path_entry_path.write_text(source)
# The package root becomes `loonfs.server` below, so both modules export the fallback member
# beside the other members.
for exports_path, replacements in (
    (
        pathlib.Path("generated/python/types/__init__.py"),
        (
            (
                "    from .path_entry import PathEntry, PathEntry_Dir, PathEntry_File\n",
                "    from .path_entry import PathEntry, PathEntry_Dir, PathEntry_File, PathEntry_Unknown\n",
            ),
            ('    "PathEntry_File": ".path_entry",\n', '    "PathEntry_File": ".path_entry",\n    "PathEntry_Unknown": ".path_entry",\n'),
            ('    "PathEntry_File",\n', '    "PathEntry_File",\n    "PathEntry_Unknown",\n'),
        ),
    ),
    (
        pathlib.Path("generated/python/__init__.py"),
        (
            ("        PathEntry_File,\n", "        PathEntry_File,\n        PathEntry_Unknown,\n"),
            ('    "PathEntry_File": ".types",\n', '    "PathEntry_File": ".types",\n    "PathEntry_Unknown": ".types",\n'),
            ('    "PathEntry_File",\n', '    "PathEntry_File",\n    "PathEntry_Unknown",\n'),
        ),
    ),
):
    source = exports_path.read_text()
    for old, new in replacements:
        source = replace_once(source, old, new)
    exports_path.write_text(source)

client_path = pathlib.Path("generated/python/client.py")
source = client_path.read_text()
source = remove_exact(
    source,
    "    stream_reconnection_enabled : typing.Optional[bool]\n"
    "        Whether to automatically reconnect on stream disconnection for resumable streaming endpoints. Defaults to True. Per-request `stream_reconnection_enabled` in `request_options` takes precedence over this value.\n\n",
    2,
    "client stream_reconnection_enabled documentation blocks",
)
source = remove_exact(
    source,
    "    max_stream_reconnection_attempts : typing.Optional[int]\n"
    "        The maximum number of reconnection attempts for resumable streaming endpoints. Defaults to no limit. Per-request `max_stream_reconnection_attempts` in `request_options` takes precedence over this value.\n\n",
    2,
    "client max_stream_reconnection_attempts documentation blocks",
)
source = remove_exact(
    source,
    "        stream_reconnection_enabled: typing.Optional[bool] = None,\n",
    2,
    "client stream_reconnection_enabled parameters",
)
source = remove_exact(
    source,
    "        max_stream_reconnection_attempts: typing.Optional[int] = None,\n",
    2,
    "client max_stream_reconnection_attempts parameters",
)
source = remove_exact(
    source,
    "            stream_reconnection_enabled=stream_reconnection_enabled,\n",
    2,
    "client stream_reconnection_enabled pass-throughs",
)
source = remove_exact(
    source,
    "            max_stream_reconnection_attempts=max_stream_reconnection_attempts,\n",
    2,
    "client max_stream_reconnection_attempts pass-throughs",
)
client_path.write_text(source)

wrapper_path = pathlib.Path("generated/python/core/client_wrapper.py")
source = wrapper_path.read_text()
source = replace_once(
    source,
    "from .logging import LogConfig, Logger\n",
    "from .logging import LogConfig, Logger\n"
    "from importlib.metadata import PackageNotFoundError, version\n"
    "\n"
    "\n"
    "def _sdk_version() -> str:\n"
    "    try:\n"
    "        return version(\"loonfs\")\n"
    "    except PackageNotFoundError:\n"
    "        return \"unknown\"\n",
)
# Fern writes the version of the last published package plus one patch into
# the User-Agent, so the literal is stale in every release; the installed
# distribution's own version is always right.
user_agent_matches = re.findall(r'"User-Agent": "loonfs/[^"]+"', source)
assert len(user_agent_matches) == 1, f"expected one User-Agent literal, found {user_agent_matches}"
source = source.replace(user_agent_matches[0], '"User-Agent": f"loonfs/{_sdk_version()}"')
source = remove_exact(
    source,
    "        stream_reconnection_enabled: typing.Optional[bool] = None,\n",
    3,
    "client wrapper stream_reconnection_enabled parameters",
)
source = remove_exact(
    source,
    "        max_stream_reconnection_attempts: typing.Optional[int] = None,\n",
    3,
    "client wrapper max_stream_reconnection_attempts parameters",
)
source = remove_exact(
    source,
    "        self._stream_reconnection_enabled = stream_reconnection_enabled\n",
    1,
    "client wrapper stream_reconnection_enabled attribute",
)
source = remove_exact(
    source,
    "        self._max_stream_reconnection_attempts = max_stream_reconnection_attempts\n",
    1,
    "client wrapper max_stream_reconnection_attempts attribute",
)
source = remove_exact(
    source,
    "    def get_stream_reconnection_enabled(self) -> bool:\n"
    "        return self._stream_reconnection_enabled if self._stream_reconnection_enabled is not None else True\n\n",
    1,
    "client wrapper stream_reconnection_enabled getter",
)
source = remove_exact(
    source,
    "    def get_max_stream_reconnection_attempts(self) -> typing.Optional[int]:\n"
    "        return self._max_stream_reconnection_attempts\n\n",
    1,
    "client wrapper max_stream_reconnection_attempts getter",
)
source = remove_exact(
    source,
    "            stream_reconnection_enabled=stream_reconnection_enabled,\n",
    2,
    "client wrapper stream_reconnection_enabled pass-throughs",
)
source = remove_exact(
    source,
    "            max_stream_reconnection_attempts=max_stream_reconnection_attempts,\n",
    2,
    "client wrapper max_stream_reconnection_attempts pass-throughs",
)
wrapper_path.write_text(source)

package_root = pathlib.Path("generated/python")
server_module = package_root / "server.py"
(package_root / "__init__.py").replace(server_module)
(package_root / "__init__.py").write_text(
    '"""Explicit server and proxy entry points for the LoonFS SDK."""\n'
)

source = server_module.read_text()
assert '"ApiError"' not in source, "generated server.py unexpectedly exports ApiError"
generated_import = "    from .client import AsyncLoonFS, LoonFS\n"
assert source.count(generated_import) == 1, "generated server.py client import not found"
source = source.replace(
    generated_import,
    "    from .core.api_error import ApiError\n"
    "    from .transfers import AsyncDownloadStream, AsyncLoonFS, DownloadResult, DownloadStream, InlinePreparedContent, PreparedContent, PreparedFile, LoonFS\n",
)
for client_name in ("LoonFS", "AsyncLoonFS"):
    generated_mapping = f'    "{client_name}": ".client",\n'
    assert source.count(generated_mapping) == 1, f"generated server.py {client_name} mapping not found exactly once"
    source = source.replace(generated_mapping, f'    "{client_name}": ".transfers",\n')
for anchor, insertion, label in (
    ('    "AsyncLoonFS": ".transfers",\n', '    "ApiError": ".core.api_error",\n    "AsyncDownloadStream": ".transfers",\n', "async transfer and ApiError mappings"),
    ('    "FileRevision": ".types",\n', '    "DownloadResult": ".transfers",\n    "DownloadStream": ".transfers",\n', "DownloadResult mapping"),
    ('    "FileRevision": ".types",\n', '    "PreparedContent": ".transfers",\n    "InlinePreparedContent": ".transfers",\n    "PreparedFile": ".transfers",\n', "PreparedContent mapping"),
):
    assert source.count(anchor) == 1, f"generated server.py anchor for {label} not found exactly once"
    source = source.replace(anchor, insertion + anchor)
for anchor, insertion, label in (
    ('    "AsyncLoonFS",\n', '    "ApiError",\n    "AsyncDownloadStream",\n', "async transfer and ApiError __all__ entries"),
    ('    "FileRevision",\n', '    "DownloadResult",\n    "DownloadStream",\n', "DownloadResult __all__ entry"),
    ('    "FileRevision",\n', '    "PreparedContent",\n    "InlinePreparedContent",\n    "PreparedFile",\n', "PreparedContent __all__ entry"),
):
    assert source.count(anchor) == 1, f"generated server.py anchor for {label} not found exactly once"
    source = source.replace(anchor, insertion + anchor)
server_module.write_text(source)

for example_path in [*package_root.rglob("*.py"), package_root / "reference.md"]:
    source = example_path.read_text()
    example_path.write_text(
        source.replace("from loonfs import", "from loonfs.server import")
    )
