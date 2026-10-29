# Security Policy

## Deploying securely

If you serve a LoonFS server beyond localhost, serve it over TLS. Without TLS, anyone who can read the connection can read the bearer token, and the presigned object-store URLs that upload responses include. [Self-hosting](crates/loonfs-server/docs/self-hosting.md) describes the `[transport]` configuration, and the one exception, for deployments where a proxy terminates TLS instead.

## Supported Versions

LoonFS supports these versions:

| Version        | Supported          |
| -------------- | ------------------ |
| latest release | :white_check_mark: |

LoonFS is pre-1.0. Security fixes are merged to `main`, and they ship in the next release.

## Reporting a Vulnerability

Please don't report vulnerabilities in public GitHub issues. Instead, submit a private report through GitHub:

https://github.com/loonfs/loonfs/security/advisories/new
