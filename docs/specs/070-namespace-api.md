# 070 — Namespace CLI

## Overview

This spec defines the `loon namespace` CLI commands for managing namespaces and their contents.

All file paths within a namespace are absolute, rooted at `/`. The root directory `/` always exists and cannot be deleted.

---

## Namespace management

### create

Create a named, persistent namespace.

```
loon namespace create [OPTIONS] NAME
```

| Argument | Description |
|----------|-------------|
| `NAME` | Name for the new namespace (required) |

| Option | Description |
|--------|-------------|
| `-e, --env TEXT` | Environment to target |

Errors if a namespace with the same name already exists.

---

### list

List all namespaces in an environment.

```
loon namespace list [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--json / --no-json` | Machine-readable JSON output (default: no-json) |
| `-e, --env TEXT` | Environment to target |

---

### delete

Delete a namespace and all of its data. This is irreversible.

```
loon namespace delete [OPTIONS] NAME
```

| Argument | Description |
|----------|-------------|
| `NAME` | Name of the namespace to delete (required, case sensitive) |

| Option | Description |
|--------|-------------|
| `--allow-missing` | Don't error if the namespace doesn't exist |
| `-y, --yes` | Skip confirmation prompt |
| `-e, --env TEXT` | Environment to target |

---

### rename

Rename a namespace.

```
loon namespace rename [OPTIONS] OLD_NAME NEW_NAME
```

| Argument | Description |
|----------|-------------|
| `OLD_NAME` | Current namespace name (required) |
| `NEW_NAME` | New namespace name (required) |

| Option | Description |
|--------|-------------|
| `-y, --yes` | Skip confirmation prompt |
| `-e, --env TEXT` | Environment to target |

Errors if `OLD_NAME` doesn't exist or `NEW_NAME` is already taken.

---

## File operations

### ls

List files and directories in a namespace.

```
loon namespace ls [OPTIONS] NAMESPACE_NAME [PATH]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `NAMESPACE_NAME` | required | Namespace to browse |
| `PATH` | `/` | Directory or file to list |

| Option | Description |
|--------|-------------|
| `--json / --no-json` | Machine-readable JSON output (default: no-json) |
| `-e, --env TEXT` | Environment to target |

If `PATH` is a file, displays that file's metadata. If `PATH` is a directory, lists its contents.

---

### get

Download files from a namespace.

```
loon namespace get [OPTIONS] NAMESPACE_NAME REMOTE_PATH [LOCAL_DESTINATION]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `NAMESPACE_NAME` | required | Namespace to download from |
| `REMOTE_PATH` | required | File or directory to download |
| `LOCAL_DESTINATION` | `.` | Local path to write to |

| Option | Description |
|--------|-------------|
| `--force / --no-force` | Overwrite existing local files (default: no-force) |
| `-e, --env TEXT` | Environment to target |

If `REMOTE_PATH` is a directory, its contents are downloaded recursively including all subdirectories. Use `-` as `LOCAL_DESTINATION` to write file contents to stdout.

---

### put

Upload a file or directory to a namespace.

```
loon namespace put [OPTIONS] NAMESPACE_NAME LOCAL_PATH [REMOTE_PATH]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `NAMESPACE_NAME` | required | Namespace to upload to |
| `LOCAL_PATH` | required | Local file or directory to upload |
| `REMOTE_PATH` | `/` | Destination path in namespace |

| Option | Description |
|--------|-------------|
| `-f, --force` | Overwrite existing remote files |
| `-e, --env TEXT` | Environment to target |

Remote parent directories are created as needed. If `REMOTE_PATH` ends with `/`, it is treated as a directory and the file is uploaded under it using its local filename.

---

### rm

Delete a file or directory from a namespace.

```
loon namespace rm [OPTIONS] NAMESPACE_NAME REMOTE_PATH
```

| Argument | Description |
|----------|-------------|
| `NAMESPACE_NAME` | Namespace to delete from (required) |
| `REMOTE_PATH` | File or directory to delete (required) |

| Option | Description |
|--------|-------------|
| `-r, --recursive` | Delete directory recursively |
| `-e, --env TEXT` | Environment to target |

Deleting a non-empty directory without `--recursive` is an error. Deleting the root `/` is not allowed.

---

### cp

Copy files within a namespace.

```
loon namespace cp [OPTIONS] NAMESPACE_NAME PATHS...
```

| Argument | Description |
|----------|-------------|
| `NAMESPACE_NAME` | Namespace to copy within (required) |
| `PATHS...` | Source path(s) followed by destination path (required) |

| Option | Description |
|--------|-------------|
| `-r, --recursive` | Copy directories recursively |
| `-e, --env TEXT` | Environment to target |

Copies source to destination. If multiple source paths are given, the last path is treated as the destination directory. Parent directories of the destination are created as needed.
