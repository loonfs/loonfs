# 070 — Namespace API

## Overview

This spec defines the operations a client can perform against a loon server. Each operation is an HTTP endpoint. The CLI surfaces these endpoints as `loon namespace <command>`.

The API is stateless and request-response: the client holds no sync state, no local database, and no persistent connection. Every command is a self-contained round-trip.

---

## Conventions

### Paths

All file paths within a namespace are absolute, rooted at `/`. The root directory `/` always exists and cannot be deleted.

A trailing `/` on a path in `put` signals that the path is a directory and the uploaded file should be placed inside it with its local filename.

### Errors

Every error response is JSON:

```json
{
  "error": "not_found",
  "message": "No file or directory at /logs/missing.txt"
}
```

Standard error codes:

| Code | HTTP Status | Meaning |
|------|-------------|---------|
| `not_found` | 404 | Namespace or path does not exist |
| `already_exists` | 409 | Namespace or file already exists |
| `not_empty` | 409 | Directory is not empty (non-recursive delete) |
| `invalid_path` | 400 | Malformed or relative path |
| `invalid_name` | 400 | Namespace name is invalid |
| `precondition_failed` | 412 | Concurrent modification detected |

### JSON output

Commands that produce structured output accept `--json` to emit machine-readable JSON instead of human-formatted text.

---

## Namespace management

### create

Create a named namespace.

**HTTP**

```
POST /v1/namespaces
```

```json
{
  "name": "my-namespace"
}
```

**Response** — `201 Created`

```json
{
  "name": "my-namespace",
  "created_at": "2026-04-02T12:00:00Z"
}
```

**Errors**: `already_exists` if the name is taken, `invalid_name` if the name is empty or contains invalid characters.

**CLI**

```
loon namespace create [OPTIONS] NAME
```

| Option | Description |
|--------|-------------|
| `-e, --env TEXT` | Environment to target |

---

### list

List all namespaces in an environment.

**HTTP**

```
GET /v1/namespaces
```

**Response** — `200 OK`

```json
{
  "namespaces": [
    { "name": "analytics", "created_at": "2026-03-01T08:00:00Z" },
    { "name": "logs", "created_at": "2026-03-15T14:30:00Z" }
  ]
}
```

**CLI**

```
loon namespace list [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--json / --no-json` | Machine-readable output (default: no-json) |
| `-e, --env TEXT` | Environment to target |

---

### delete

Delete a namespace and all of its data. This is irreversible.

**HTTP**

```
DELETE /v1/namespaces/{name}
```

**Response** — `204 No Content`

**Errors**: `not_found` unless `allow_missing` is set.

**CLI**

```
loon namespace delete [OPTIONS] NAME
```

| Option | Description |
|--------|-------------|
| `--allow-missing` | Don't error if the namespace doesn't exist |
| `-y, --yes` | Skip confirmation prompt |
| `-e, --env TEXT` | Environment to target |

---

### rename

Rename a namespace.

**HTTP**

```
PATCH /v1/namespaces/{name}
```

```json
{
  "name": "new-name"
}
```

**Response** — `200 OK`

```json
{
  "name": "new-name"
}
```

**Errors**: `not_found` if the old name doesn't exist, `already_exists` if the new name is taken, `invalid_name` if the new name is invalid.

**CLI**

```
loon namespace rename [OPTIONS] OLD_NAME NEW_NAME
```

| Option | Description |
|--------|-------------|
| `-y, --yes` | Skip confirmation prompt |
| `-e, --env TEXT` | Environment to target |

---

## File operations

### ls

List files and directories at a path within a namespace.

**HTTP**

```
GET /v1/namespaces/{name}/ls?path=/some/dir
```

**Response** — `200 OK`

```json
{
  "path": "/some/dir",
  "entries": [
    {
      "name": "report.csv",
      "path": "/some/dir/report.csv",
      "kind": "file",
      "size_bytes": 4096,
      "content_digest": "sha256:abc123..."
    },
    {
      "name": "subdir",
      "path": "/some/dir/subdir",
      "kind": "dir"
    }
  ]
}
```

If `path` points to a file, the response contains that single file as the only entry. If `path` is omitted, it defaults to `/`.

**Errors**: `not_found` if the path doesn't exist.

**CLI**

```
loon namespace ls [OPTIONS] NAMESPACE_NAME [PATH]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `NAMESPACE_NAME` | required | Namespace to browse |
| `PATH` | `/` | Directory or file to list |

| Option | Description |
|--------|-------------|
| `--json / --no-json` | Machine-readable output (default: no-json) |
| `-e, --env TEXT` | Environment to target |

---

### get

Download a file or directory from a namespace to a local path.

If a directory is specified as `REMOTE_PATH`, its contents are downloaded recursively, including all subdirectories.

Use `-` as `LOCAL_DESTINATION` to write file contents to standard output.

**HTTP**

```
GET /v1/namespaces/{name}/files/{path}
```

For files, the response body is the raw file bytes with `Content-Type: application/octet-stream`.

For directories, the response is a tar stream (`Content-Type: application/x-tar`) containing the directory tree.

**Response headers:**

```
Content-Type: application/octet-stream
Content-Length: 4096
X-Loon-Content-Digest: sha256:abc123...
```

**Errors**: `not_found` if the path doesn't exist.

**CLI**

```
loon namespace get [OPTIONS] NAMESPACE_NAME REMOTE_PATH [LOCAL_DESTINATION]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `NAMESPACE_NAME` | required | Namespace to download from |
| `REMOTE_PATH` | required | File or directory to download |
| `LOCAL_DESTINATION` | `.` | Local path to write to, or `-` for stdout |

| Option | Description |
|--------|-------------|
| `--force / --no-force` | Overwrite existing local files (default: no-force) |
| `-e, --env TEXT` | Environment to target |

---

### put

Upload a local file or directory to a namespace.

Remote parent directories are created as needed. If `REMOTE_PATH` ends with `/`, the file is uploaded under that directory using its local filename.

**HTTP**

```
PUT /v1/namespaces/{name}/files/{path}
```

Request body is the raw file bytes with `Content-Type: application/octet-stream`.

For directory uploads, the request body is a tar stream (`Content-Type: application/x-tar`).

**Response** — `201 Created` (new file) or `200 OK` (overwrite with `--force`)

```json
{
  "path": "/data/report.csv",
  "kind": "file",
  "size_bytes": 4096,
  "content_digest": "sha256:abc123..."
}
```

**Errors**: `already_exists` if the path exists and `--force` is not set. `not_found` if the namespace doesn't exist.

**CLI**

```
loon namespace put [OPTIONS] NAMESPACE_NAME LOCAL_PATH [REMOTE_PATH]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `NAMESPACE_NAME` | required | Namespace to upload to |
| `LOCAL_PATH` | required | Local file or directory |
| `REMOTE_PATH` | `/` | Destination path in namespace |

| Option | Description |
|--------|-------------|
| `-f, --force` | Overwrite existing files |
| `-e, --env TEXT` | Environment to target |

---

### rm

Delete a file or directory from a namespace.

Deleting a non-empty directory requires `--recursive`. Deleting the root `/` is not allowed.

**HTTP**

```
DELETE /v1/namespaces/{name}/files/{path}?recursive={true|false}
```

**Response** — `204 No Content`

**Errors**: `not_found` if the path doesn't exist. `not_empty` if the path is a non-empty directory and `recursive` is false.

**CLI**

```
loon namespace rm [OPTIONS] NAMESPACE_NAME REMOTE_PATH
```

| Argument | Description |
|----------|-------------|
| `NAMESPACE_NAME` | required |
| `REMOTE_PATH` | required |

| Option | Description |
|--------|-------------|
| `-r, --recursive` | Delete directory recursively |
| `-e, --env TEXT` | Environment to target |

---

### cp

Copy a file or directory within a namespace.

Copies source to destination. If multiple source paths are given, the last path is treated as the destination directory. Parent directories of the destination are created as needed.

**HTTP**

```
POST /v1/namespaces/{name}/cp
```

```json
{
  "source": "/data/report.csv",
  "destination": "/archive/report.csv"
}
```

For multi-source copies:

```json
{
  "sources": ["/data/a.csv", "/data/b.csv"],
  "destination_dir": "/archive/"
}
```

**Response** — `201 Created`

```json
{
  "copied": [
    { "source": "/data/report.csv", "destination": "/archive/report.csv" }
  ]
}
```

**Errors**: `not_found` if a source path doesn't exist. `already_exists` if the destination already exists. `invalid_path` if the destination is invalid.

**CLI**

```
loon namespace cp [OPTIONS] NAMESPACE_NAME PATHS...
```

| Argument | Description |
|----------|-------------|
| `NAMESPACE_NAME` | required |
| `PATHS...` | Source path(s) followed by destination path |

| Option | Description |
|--------|-------------|
| `-r, --recursive` | Copy directories recursively |
| `-e, --env TEXT` | Environment to target |
