# The LoonFS server image.
#
# Build it with the repository root as the context, because the build needs
# the whole workspace:
#
#   docker build -f crates/loonfs-server/Dockerfile -t loonfs-server:dev .
#
# The image carries the server binary and nothing else. Mount a config file
# and, for a local_fs store, the directory that store writes to.

# The tag names no version on purpose. `rust-toolchain.toml` pins the exact
# compiler version, and the rustup in this image installs that version, so the
# image builds with the same compiler as CI whatever version the tag installed.
FROM rust:bookworm AS builder

WORKDIR /src

# The whole workspace, because the members depend on each other and the lock
# file resolves all of them together.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

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

# glibc settings that keep the server's resident memory close to what it
# uses: two arenas instead of up to eight per core, a trim of the heap's free
# top above 128 KiB, and mmap for every allocation of 128 KiB or more, which
# returns that allocation to the kernel when it's freed. With these, the lab's
# Linux peaks were 10% to 32% above the same workload on macOS, and with
# glibc's defaults up to about three times above. The server's memory sizing
# assumes these values. A deployment's own environment overrides them.
ENV MALLOC_ARENA_MAX=2 \
    MALLOC_TRIM_THRESHOLD_=131072 \
    MALLOC_MMAP_THRESHOLD_=131072

# Numeric, not the name. A kubelet running a pod with `runAsNonRoot: true`
# reads this field to decide whether the user is root, and it cannot resolve
# a name to an id without starting the container first. A named user there
# fails the container with "cannot verify user is non-root", which also puts
# the image outside the restricted Pod Security Standard.
USER 10001:10001

# A convention, not a server default. The config's `bind` decides the address
# and the port; this line documents the port the examples use.
EXPOSE 9400

# Exec form, so the server is PID 1 and receives SIGTERM itself. A shell
# wrapper would hold PID 1 instead, and the server would never see the signal
# it shuts down on.
ENTRYPOINT ["/usr/local/bin/loonfs-server"]
CMD ["--config", "/etc/loonfs/config.toml"]
