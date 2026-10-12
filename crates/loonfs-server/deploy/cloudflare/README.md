# Deploy LoonFS to Cloudflare

This directory deploys one LoonFS server to a Cloudflare Container. R2 stores the data, so restarting the Container doesn't delete it. To run the server on your own host or on Kubernetes instead, see [Self-hosting LoonFS](../../docs/self-hosting.md).

Before you start, you need Workers Paid, R2, Node.js 22 or newer, and a running Docker engine. Create an R2 bucket, and an R2 API token with Object Read & Write access.

Run the commands below from this directory.

## 1. Install and sign in

Install the dependencies and sign in to Cloudflare:

```bash
npm ci
npx wrangler login
```

## 2. Set configuration

Choose your Cloudflare account, your R2 bucket, and a unique key prefix:

```bash
export CLOUDFLARE_ACCOUNT_ID="your-account-id"
export LOONFS_R2_BUCKET="your-bucket-name"
export LOONFS_R2_KEY_PREFIX="your-key-prefix"
```

These values are ordinary configuration, not secrets.

## 3. Set secrets

Generate two LoonFS secrets and enter your R2 credentials:

```bash
export LOONFS_AUTH_TOKEN="$(openssl rand -hex 32)"
export LOONFS_CONTENT_TOKEN_SECRET="$(openssl rand -hex 32)"
export AWS_ACCESS_KEY_ID="your-r2-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-r2-secret-access-key"

printf '%s' "$LOONFS_AUTH_TOKEN" | npx wrangler secret put LOONFS_AUTH_TOKEN
printf '%s' "$LOONFS_CONTENT_TOKEN_SECRET" | npx wrangler secret put LOONFS_CONTENT_TOKEN_SECRET
printf '%s' "$AWS_ACCESS_KEY_ID" | npx wrangler secret put AWS_ACCESS_KEY_ID
printf '%s' "$AWS_SECRET_ACCESS_KEY" | npx wrangler secret put AWS_SECRET_ACCESS_KEY
```

Save `LOONFS_AUTH_TOKEN`, because you'll need it to connect to the server.

## 4. Deploy

Run the type check, and then deploy:

```bash
npm run typecheck
npx wrangler deploy \
  --var "CLOUDFLARE_ACCOUNT_ID:$CLOUDFLARE_ACCOUNT_ID" \
  --var "LOONFS_R2_BUCKET:$LOONFS_R2_BUCKET" \
  --var "LOONFS_R2_KEY_PREFIX:$LOONFS_R2_KEY_PREFIX"
```

Wrangler prints the server URL. For later deployments, you can run `npm run deploy`.

## 5. Test

Install the `loonfs` CLI, and then run:

```bash
export LOONFS_SERVER_URL="https://loonfs-cloudflare.<your-subdomain>.workers.dev"
./smoke-test.sh
```

The smoke test checks the server, R2 access, and a file upload and download.

## Important details

Keep these details in mind when you run the server on Cloudflare:

- Keep `max_instances` set to `1`, because LoonFS expects one active server writer.
- The first request can take a few minutes while the Container starts.
- The Container stops after ten idle minutes, and it starts again when a request arrives.
- `/health` and `/readiness` are public. `/metrics` and `/v0/*` require the auth token.
