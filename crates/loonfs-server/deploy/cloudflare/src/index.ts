import { Container, getContainer } from "@cloudflare/containers";

interface Env {
  LOONFS_CONTAINER: DurableObjectNamespace<LoonFSContainer>;
  CLOUDFLARE_ACCOUNT_ID: string;
  LOONFS_R2_BUCKET: string;
  LOONFS_R2_KEY_PREFIX: string;
  LOONFS_AUTH_TOKEN: string;
  LOONFS_CONTENT_TOKEN_SECRET: string;
  AWS_ACCESS_KEY_ID: string;
  AWS_SECRET_ACCESS_KEY: string;
}

type StringBinding = Exclude<keyof Env, "LOONFS_CONTAINER">;

function required(env: Env, name: StringBinding): string {
  const value = env[name].trim();
  if (value.length === 0) {
    throw new Error(`Cloudflare binding ${name} must not be empty`);
  }
  return value;
}

function serverConfig(env: Env): string {
  const accountId = required(env, "CLOUDFLARE_ACCOUNT_ID");
  if (!/^[a-f0-9]{32}$/i.test(accountId)) {
    throw new Error("CLOUDFLARE_ACCOUNT_ID must be a 32-character hexadecimal account ID");
  }
  const bucket = required(env, "LOONFS_R2_BUCKET");
  const keyPrefix = required(env, "LOONFS_R2_KEY_PREFIX");

  return `bind = "0.0.0.0:9400"
writer_id = "cloudflare-primary"
allow_remote_without_tls = true
max_upload_bytes = 67108864
max_download_bytes = 67108864
max_concurrent_uploads = 2
max_concurrent_downloads = 2
max_concurrent_maintenance = 1

[runtime_cache]
max_cached_namespaces = 8
max_cached_wal_tail_projection_rows = 100000
max_cached_wal_tail_projection_decoded_bytes = 67108864
metadata_segment_cache_max_decoded_bytes = 67108864

[store]
kind = "cloudflare-r2"
bucket = ${JSON.stringify(bucket)}
account_id = "${accountId}"
endpoint_url = "https://${accountId}.r2.cloudflarestorage.com"
key_prefix = ${JSON.stringify(keyPrefix)}

[store.credentials]
kind = "ambient"
`;
}

export class LoonFSContainer extends Container<Env> {
  override defaultPort = 9400;
  override pingEndpoint = "localhost/health";
  override sleepAfter = "10m";
  override entrypoint = ["/usr/local/bin/loonfs-server"];

  override envVars = {
    LOONFS_SERVER_CONFIG_TOML: serverConfig(this.env),
    LOONFS_AUTH_TOKEN: required(this.env, "LOONFS_AUTH_TOKEN"),
    LOONFS_CONTENT_TOKEN_SECRET: required(this.env, "LOONFS_CONTENT_TOKEN_SECRET"),
    AWS_ACCESS_KEY_ID: required(this.env, "AWS_ACCESS_KEY_ID"),
    AWS_SECRET_ACCESS_KEY: required(this.env, "AWS_SECRET_ACCESS_KEY"),
  };
}

export default {
  fetch(request: Request, env: Env) {
    const path = new URL(request.url).pathname;
    const isLoonFSPath =
      ["/health", "/readiness", "/metrics"].includes(path) || path.startsWith("/v0/");
    if (!isLoonFSPath) {
      return new Response("Not Found", { status: 404 });
    }
    return getContainer(env.LOONFS_CONTAINER, "primary").fetch(request);
  },
} satisfies ExportedHandler<Env>;
