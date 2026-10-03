# syntax=docker/dockerfile:1

FROM docker.io/library/rust:1.98.1-alpine3.24@sha256:7cc1c22d77d9432f7fe012a70e6d3e555af54c2a6832700ed7d553f1769ae89f AS build
WORKDIR /app
# hadolint ignore=DL3018
RUN apk add --no-cache musl-dev
COPY Cargo.toml Cargo.lock ./
COPY seeds.txt ./
COPY src ./src
RUN cargo build --release && strip target/release/lyra-index

FROM docker.io/library/alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6
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
