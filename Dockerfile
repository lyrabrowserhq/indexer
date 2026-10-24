# syntax=docker/dockerfile:1

FROM docker.io/library/rust:1.99.0-alpine3.24@sha256:0cce0a5e0e8ba67b455257a3a02a1d99005f382748789d6464460028810f1627 AS build
WORKDIR /app
# hadolint ignore=DL3018
RUN apk add --no-cache musl-dev
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && touch src/lib.rs \
    && cargo build --release \
    && rm -rf src target/release/lyra-index target/release/deps/lyra_index*
COPY seeds.txt ./
COPY src ./src
RUN touch src/main.rs src/lib.rs \
    && cargo build --release && strip target/release/lyra-index

FROM docker.io/library/alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
# hadolint ignore=DL3018
RUN apk add --no-cache ca-certificates wget \
    && adduser -D -H -u 65532 -s /sbin/nologin lyra \
    && mkdir -p /var/lib/lyra-index/data \
    && chown -R 65532:65532 /var/lib/lyra-index
WORKDIR /var/lib/lyra-index
COPY --from=build /app/target/release/lyra-index /usr/local/bin/lyra-index
COPY --from=build /app/seeds.txt /var/lib/lyra-index/seeds.txt
COPY docker/entrypoint.sh /usr/local/bin/lyra-index-entry
RUN chmod 755 /usr/local/bin/lyra-index-entry
USER 65532
ENV BIND=0.0.0.0:8091
ENV INDEX_DIR=/var/lib/lyra-index/data
ENV RUST_LOG=info
EXPOSE 8091
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8091/health"]
ENTRYPOINT ["/usr/local/bin/lyra-index-entry"]
CMD ["serve"]
