# The LoonFS server image.
#
# Build it with the repository root as the context, because the build needs
# the whole workspace:
#
#   docker build -f crates/loonfs-server/Dockerfile -t loonfs-server:dev .
#
# The image carries the server binary and nothing else. Mount a config file
# and, for a local-fs store, the directory that store writes to.

# The tag names no version on purpose. `rust-toolchain.toml` pins
# `channel = "stable"`, and the rustup in this image honors that file, so the
# toolchain file picks the compiler whatever version the tag installed. The
# CI `msrv` job is what enforces the 1.85 floor.
FROM rust:bookworm AS builder

WORKDIR /src

# The whole workspace, because the members depend on each other and the lock
# file resolves all of them together.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

# One package, default features. `--all-features` would also build
# `loonfs-openapi`, the crate's second binary, which rides the non-default
# `openapi` feature. That binary regenerates the API spec during development
# and does not belong in a deployment.
RUN cargo build --locked --release -p loonfs-server

FROM debian:bookworm-slim

# The server reaches object storage over HTTPS and verifies those endpoints
# against the public root certificates. Nothing else is installed.
RUN apt-get update \
    && apt-get install --no-install-recommends --assume-yes ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# The server runs as a normal user. It writes only where a deployment mounts
# a volume, so it owns no path inside the image.
RUN groupadd --system --gid 10001 loonfs \
    && useradd --system --uid 10001 --gid loonfs --no-create-home \
        --shell /usr/sbin/nologin loonfs

COPY --from=builder /src/target/release/loonfs-server /usr/local/bin/loonfs-server

USER loonfs

# A convention, not a server default. The config's `bind` decides the address
# and the port; this line documents the port the examples use.
EXPOSE 9400

# Exec form, so the server is PID 1 and receives SIGTERM itself. A shell
# wrapper would hold PID 1 instead, and the server would never see the signal
# it shuts down on.
ENTRYPOINT ["/usr/local/bin/loonfs-server"]
CMD ["--config", "/etc/loonfs/config.toml"]
