# Security Policy

## Deploying securely

Serving a LoonFS server beyond localhost means serving it over TLS, because
the bearer token and the presigned object-store URLs in upload responses are
both readable by anyone who can read the connection. See
[Running a server in production](README.md#running-a-server-in-production)
for the `[tls]` configuration and the one escape hatch, for deployments where
a proxy terminates TLS instead.

## Supported Versions

| Version        | Supported          |
| -------------- | ------------------ |
| latest release | :white_check_mark: |

LoonFS is pre-1.0; security fixes land on `main` and ship in the next release.

## Reporting a Vulnerability

Please do not report vulnerabilities through public GitHub issues. Instead,
submit a private report through GitHub:

https://github.com/loonfs/loonfs/security/advisories/new
