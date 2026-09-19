# syntax=docker/dockerfile:1
FROM docker.io/library/rust:1.98-alpine3.24@sha256:7cc1c22d77d9432f7fe012a70e6d3e555af54c2a6832700ed7d553f1769ae89f AS builder

WORKDIR /build
RUN apk add --no-cache cmake make musl-dev perl
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN cargo build --release --locked

FROM docker.io/library/alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6 AS runtime-files

RUN apk add --no-cache ca-certificates \
    && install -d -o 65532 -g 65532 -m 0750 \
        /var/lib/galaxync /etc/galaxync /usr/share/licenses/galaxync

COPY LICENSE /usr/share/licenses/galaxync/LICENSE

FROM scratch AS runtime

LABEL org.opencontainers.image.title="galaxync" \
      org.opencontainers.image.description="Ansible Galaxy collection mirror daemon" \
      org.opencontainers.image.source="https://github.com/psvmcc/galaxync" \
      org.opencontainers.image.licenses="MIT"

COPY --from=runtime-files /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=runtime-files --chown=65532:65532 /var/lib/galaxync/ /var/lib/galaxync/
COPY --from=runtime-files --chown=65532:65532 /etc/galaxync/ /etc/galaxync/
COPY --from=runtime-files /usr/share/licenses/galaxync/ /usr/share/licenses/galaxync/
COPY --from=builder --chown=65532:65532 /build/target/release/galaxync /galaxync

USER 65532:65532
VOLUME ["/var/lib/galaxync"]
EXPOSE 9898

ENTRYPOINT ["/galaxync"]
CMD ["--config", "/etc/galaxync/config.yaml"]
