# 070 — Namespace API

## Overview

This spec defines the HTTP API for operating on loon namespaces and their contents. The API is stateless and request-response: every call is a self-contained round-trip with no client-side sync state or persistent connections.

---

## Conventions

### Paths

All file paths within a namespace are absolute, rooted at `/`. The root directory `/` always exists and cannot be deleted.

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

---

## Namespace management

### Create namespace

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

---

### List namespaces

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

---

### Delete namespace

Delete a namespace and all of its data. This is irreversible.

```
DELETE /v1/namespaces/{name}
```

**Response** — `204 No Content`

**Errors**: `not_found` if the namespace doesn't exist. Pass `?allow_missing=true` to suppress.

---

### Rename namespace

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

---

## File operations

### List path

List files and directories at a path within a namespace.

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

---

### Download file

Download file contents from a namespace.

```
GET /v1/namespaces/{name}/files/{path}
```

For files, the response body is the raw file bytes:

```
Content-Type: application/octet-stream
Content-Length: 4096
X-Loon-Content-Digest: sha256:abc123...
```

For directories, the response is a tar stream containing the directory tree:

```
Content-Type: application/x-tar
```

**Errors**: `not_found` if the path doesn't exist.

---

### Upload file

Upload a file to a namespace. Parent directories are created as needed.

```
PUT /v1/namespaces/{name}/files/{path}
```

Request body is the raw file bytes with `Content-Type: application/octet-stream`.

For directory uploads, the request body is a tar stream with `Content-Type: application/x-tar`.

**Response** — `201 Created` (new file) or `200 OK` (overwrite)

```json
{
  "path": "/data/report.csv",
  "kind": "file",
  "size_bytes": 4096,
  "content_digest": "sha256:abc123..."
}
```

By default, uploading to an existing path fails with `already_exists`. Pass `?overwrite=true` to replace.

**Errors**: `already_exists` if the path exists and `overwrite` is not set. `not_found` if the namespace doesn't exist.

---

### Delete file

Delete a file or directory from a namespace. Deleting the root `/` is not allowed.

```
DELETE /v1/namespaces/{name}/files/{path}
```

Pass `?recursive=true` to delete a non-empty directory.

**Response** — `204 No Content`

**Errors**: `not_found` if the path doesn't exist. `not_empty` if the path is a non-empty directory and `recursive` is not set.

---

### Copy

Copy a file or directory within a namespace. Parent directories of the destination are created as needed.

```
POST /v1/namespaces/{name}/cp
```

Single-source copy:

```json
{
  "source": "/data/report.csv",
  "destination": "/archive/report.csv"
}
```

Multi-source copy (destination must be a directory):

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
