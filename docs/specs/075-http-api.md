# 075 — HTTP API

## Overview

This spec defines the HTTP API that backs the `loon namespace` CLI (spec 070). All endpoints are versioned under `/v1/`.

The API splits into three groups:

1. **Content endpoints** — immutable, content-addressed block and manifest storage. The caller splits files into 16 MiB blocks, uploads them individually, then constructs a manifest. These operations are idempotent.
2. **Metadata endpoints** — atomic namespace tree mutations. The caller references previously uploaded content by manifest digest. The server validates all content exists, then commits the change atomically (WAL write + CAS head update).
3. **Namespace management** — create, list, delete, rename namespaces.

Content uploads are staging — they don't change the namespace tree. The metadata commit is the atomicity boundary. If the caller crashes mid-upload, some orphaned blocks may exist in the store. They are harmless (immutable, content-addressed) and can be garbage-collected.

---

## Conventions

### Paths

File paths within a namespace are absolute, rooted at `/`. In URL path segments, the leading `/` of the namespace path is implicit — the namespace path `/data/report.csv` appears as `.../files/data/report.csv`.

### Content addressing

Block digests use the format `sha256:{hex}`. The block size is fixed at 16 MiB; the final block of a file may be shorter. Manifest digests are `sha256:{hex}` of the canonical manifest JSON bytes.

### Errors

Every error response is a JSON body:

```json
{
  "error": "not_found",
  "message": "No file or directory at /logs/missing.txt"
}
```

| Code | HTTP Status | Meaning |
|------|-------------|---------|
| `not_found` | 404 | Namespace, path, block, or manifest does not exist |
| `already_exists` | 409 | Namespace, file, or directory already exists |
| `not_empty` | 409 | Directory is not empty (non-recursive delete) |
| `invalid_path` | 400 | Malformed or relative path |
| `invalid_name` | 400 | Namespace name is empty or invalid |
| `invalid_digest` | 400 | Digest does not match uploaded content |
| `content_missing` | 400 | Manifest references blocks that don't exist, or file mutation references a manifest that doesn't exist |
| `is_directory` | 400 | Operation requires a file but path is a directory |
| `payload_too_large` | 413 | Block exceeds 16 MiB |

### Authentication

Authentication is out of scope for this spec.

---

## Content endpoints

These endpoints store immutable, content-addressed data. They use create-if-absent semantics — uploading a block or manifest that already exists is a no-op.

### Upload block

```
PUT /v1/namespaces/{name}/blobs/{digest}
Content-Type: application/octet-stream
```

The request body is the raw block bytes (max 16 MiB). The server verifies the SHA-256 digest of the body matches `{digest}`.

**Response**:
- `201 Created` — block stored
- `200 OK` — block already existed

**Errors**: `invalid_digest` if the body doesn't match the digest. `payload_too_large` if the body exceeds 16 MiB.

---

### Check block existence

```
HEAD /v1/namespaces/{name}/blobs/{digest}
```

**Response**:
- `200 OK` — block exists (includes `Content-Length`)
- `404 Not Found` — block does not exist

Use this to skip uploading blocks that are already present. For a file update where only one block changed, the caller HEAD-checks all blocks and uploads only the missing one.

---

### Download block

```
GET /v1/namespaces/{name}/blobs/{digest}
```

**Response** — `200 OK`

```
Content-Type: application/octet-stream
Content-Length: 16777216
```

**Errors**: `not_found` if the block doesn't exist.

---

### Upload manifest

```
PUT /v1/namespaces/{name}/manifests/{manifest_digest}
Content-Type: application/json
```

The request body is a `ContentManifestEnvelope` JSON object:

```json
{
  "kind": "namespace_content_manifest",
  "format_version": 1,
  "payload_checksum_sha256": "...",
  "payload": {
    "namespace_id": "ns-a1b2c3",
    "file_size_bytes": 33554432,
    "file_digest_sha256": "sha256:...",
    "block_size_bytes": 16777216,
    "blocks": [
      { "content_digest_sha256": "sha256:aaa...", "plaintext_size_bytes": 16777216 },
      { "content_digest_sha256": "sha256:bbb...", "plaintext_size_bytes": 16777216 }
    ]
  }
}
```

The server verifies:
1. The SHA-256 of the JSON body matches `{manifest_digest}`.
2. The `payload_checksum_sha256` matches the payload.

The server does **not** verify that referenced blocks exist at upload time — that check happens at metadata commit time.

**Response**:
- `201 Created` — manifest stored
- `200 OK` — manifest already existed

**Errors**: `invalid_digest` if the body doesn't match the manifest digest.

---

### Download manifest

```
GET /v1/namespaces/{name}/manifests/{manifest_digest}
```

**Response** — `200 OK`

```
Content-Type: application/json
```

Body is the `ContentManifestEnvelope` JSON.

**Errors**: `not_found` if the manifest doesn't exist.

---

## Metadata endpoints

These endpoints mutate the namespace tree. Each mutation internally acquires the namespace lease, validates preconditions against the authoritative head, writes an immutable WAL commit, and CAS-updates `head.json`. The caller sees a single atomic request/response.

### List path

**CLI**: `loon namespace ls`

```
GET /v1/namespaces/{name}/ls?path={path}
```

| Query param | Default | Description |
|-------------|---------|-------------|
| `path` | `/` | Absolute path to list |

**Response** — `200 OK`

```json
{
  "namespace_id": "ns-a1b2c3",
  "path": "/data",
  "entries": [
    {
      "name": "report.csv",
      "path": "/data/report.csv",
      "kind": "file",
      "inode_id": 42,
      "revision_no": 3,
      "size_bytes": 4096,
      "content_digest": "sha256:...",
      "content_manifest_digest": "sha256:..."
    },
    {
      "name": "archive",
      "path": "/data/archive",
      "kind": "dir",
      "inode_id": 15
    }
  ]
}
```

If `path` is a file, returns that single file as the only entry. Entries are sorted alphabetically by name. File entries include `content_manifest_digest` so the caller can fetch the manifest for block-level access.

**Errors**: `not_found` if the path doesn't exist.

---

### Download file

**CLI**: `loon namespace get`

```
GET /v1/namespaces/{name}/files/{path}
```

Convenience endpoint that reassembles a file from its manifest and blocks.

**Response** — `200 OK`

```
Content-Type: application/octet-stream
Content-Length: 33554432
X-Loon-Inode-Id: 42
X-Loon-Revision-No: 3
X-Loon-Content-Digest: sha256:...
X-Loon-Content-Manifest-Digest: sha256:...
```

The body is the full reassembled file bytes. The `X-Loon-Content-Manifest-Digest` header lets the caller switch to block-level access for future reads.

For directories, the response is a tar archive:

```
Content-Type: application/x-tar
```

**Errors**: `not_found` if the path doesn't exist.

---

### Create file

**CLI**: `loon namespace put` (new file)

```
POST /v1/namespaces/{name}/files/{path}
Content-Type: application/json
```

```json
{
  "content_manifest_digest": "sha256:..."
}
```

**Response** — `201 Created`

```json
{
  "path": "/data/report.csv",
  "kind": "file",
  "inode_id": 42,
  "revision_no": 1,
  "size_bytes": 4096,
  "content_manifest_digest": "sha256:..."
}
```

**Server behavior**:
1. Validates that the manifest exists and all referenced blocks are durable.
2. Resolves the parent path. If intermediate directories are missing, auto-creates them via `create_dir` commits.
3. Commits `create_file(parent_inode, display_name, content_manifest_digest)`.

**Errors**: `already_exists` if the path already exists. `content_missing` if the manifest or any of its blocks don't exist.

---

### Replace file

**CLI**: `loon namespace put --force` (existing file)

```
PUT /v1/namespaces/{name}/files/{path}
Content-Type: application/json
```

```json
{
  "content_manifest_digest": "sha256:..."
}
```

**Response** — `200 OK`

```json
{
  "path": "/data/report.csv",
  "kind": "file",
  "inode_id": 42,
  "revision_no": 4,
  "size_bytes": 4096,
  "content_manifest_digest": "sha256:..."
}
```

**Server behavior**:
1. Validates that the manifest exists and all referenced blocks are durable.
2. Resolves the path to an existing file inode and reads its current `revision_no`.
3. Commits `replace_file(inode_id, base_revision_no, content_manifest_digest)`.

**Errors**: `not_found` if the path doesn't exist. `is_directory` if the path is a directory. `content_missing` if the manifest or any of its blocks don't exist.

---

### Create directory

**CLI**: `loon namespace put` (implicit parent creation) or direct use

```
POST /v1/namespaces/{name}/dirs/{path}
```

**Response** — `201 Created`

```json
{
  "path": "/data/archive",
  "kind": "dir",
  "inode_id": 55
}
```

**Server behavior**: Resolves the parent path, auto-creating intermediate directories as needed. Commits `create_dir(parent_inode, display_name)`.

**Errors**: `already_exists` if the path already exists.

---

### Delete file or directory

**CLI**: `loon namespace rm`

```
DELETE /v1/namespaces/{name}/files/{path}
```

| Query param | Default | Description |
|-------------|---------|-------------|
| `recursive` | `false` | If `true`, delete a non-empty directory and all descendants |

**Response** — `204 No Content`

**Server behavior**:
- **File**: Commits `delete_file(inode_id)`.
- **Empty directory**: Commits `delete_subtree(root_inode)`.
- **Non-empty directory** with `recursive=true`: Commits `delete_subtree(root_inode)` — a single WAL entry with a subtree tombstone that hides the directory and all descendants.

**Errors**: `not_found` if the path doesn't exist. `not_empty` if the path is a non-empty directory and `recursive` is not set. `invalid_path` if the path is `/`.

---

### Copy

**CLI**: `loon namespace cp`

```
POST /v1/namespaces/{name}/cp
Content-Type: application/json
```

```json
{
  "source": "/data/report.csv",
  "destination": "/archive/report.csv"
}
```

| Query param | Default | Description |
|-------------|---------|-------------|
| `recursive` | `false` | If `true`, copy directories recursively |

**Response** — `201 Created`

```json
{
  "copied": [
    {
      "source": "/data/report.csv",
      "destination": "/archive/report.csv",
      "inode_id": 99,
      "revision_no": 1
    }
  ]
}
```

**Server behavior**:
- **File copy**: Resolves source to its `content_manifest_digest`, then commits `create_file` at the destination pointing at the same manifest. No bytes are copied — content is immutable and content-addressed.
- **Directory copy** with `recursive=true`: Creates destination directory tree and copies each file as above.
- Missing parent directories of the destination are auto-created.

**Errors**: `not_found` if the source doesn't exist. `already_exists` if the destination exists. `is_directory` if copying a directory without `recursive=true`.

---

## Namespace management

These endpoints operate on a namespace registry that maps human-readable names to namespace IDs.

### Create namespace

**CLI**: `loon namespace create`

```
POST /v1/namespaces
Content-Type: application/json
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
  "namespace_id": "ns-a1b2c3",
  "created_at": "2026-04-02T12:00:00Z"
}
```

**Server behavior**: Allocates a `namespace_id`, bootstraps the namespace (seeds root inode 1 as DIR, publishes initial `head.json`), and registers the name-to-ID mapping.

**Errors**: `already_exists` if the name is taken. `invalid_name` if the name is empty or invalid.

---

### List namespaces

**CLI**: `loon namespace list`

```
GET /v1/namespaces
```

**Response** — `200 OK`

```json
{
  "namespaces": [
    { "name": "analytics", "namespace_id": "ns-x1", "created_at": "2026-03-01T08:00:00Z" },
    { "name": "logs", "namespace_id": "ns-x2", "created_at": "2026-03-15T14:30:00Z" }
  ]
}
```

---

### Delete namespace

**CLI**: `loon namespace delete`

```
DELETE /v1/namespaces/{name}
```

| Query param | Default | Description |
|-------------|---------|-------------|
| `allow_missing` | `false` | Return `204` even if the namespace doesn't exist |

**Response** — `204 No Content`

**Server behavior**: Removes all objects under the namespace prefix and deletes the registry entry.

**Errors**: `not_found` if the namespace doesn't exist and `allow_missing` is not set.

---

### Rename namespace

**CLI**: `loon namespace rename`

```
PATCH /v1/namespaces/{name}
Content-Type: application/json
```

```json
{
  "name": "new-name"
}
```

**Response** — `200 OK`

```json
{
  "name": "new-name",
  "namespace_id": "ns-a1b2c3"
}
```

**Server behavior**: Updates the registry mapping. The underlying `namespace_id` and all stored objects remain unchanged.

**Errors**: `not_found` if the old name doesn't exist. `already_exists` if the new name is taken. `invalid_name` if the new name is invalid.

---

## CLI-to-API mapping

How each CLI command maps to API calls:

| CLI command | API calls |
|------------|-----------|
| `loon namespace create NAME` | `POST /v1/namespaces` |
| `loon namespace list` | `GET /v1/namespaces` |
| `loon namespace delete NAME` | `DELETE /v1/namespaces/{name}` |
| `loon namespace rename OLD NEW` | `PATCH /v1/namespaces/{name}` |
| `loon namespace ls NS [PATH]` | `GET /v1/namespaces/{name}/ls` |
| `loon namespace get NS PATH` | `GET /v1/namespaces/{name}/files/{path}` |
| `loon namespace put NS LOCAL REMOTE` | `HEAD` blocks → `PUT` missing blocks → `PUT` manifest → `POST /files/{path}` (or `PUT` if `--force`) |
| `loon namespace rm NS PATH` | `DELETE /v1/namespaces/{name}/files/{path}` |
| `loon namespace cp NS SRC DST` | `POST /v1/namespaces/{name}/cp` |

The `put` command is the only multi-step operation: the CLI splits the local file into blocks, checks which blocks already exist (HEAD), uploads missing blocks, constructs and uploads the manifest, then commits the metadata mutation.
