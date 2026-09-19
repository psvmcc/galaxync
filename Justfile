set shell := ["bash", "-euo", "pipefail", "-c"]

image := "galaxync:dev"
config := "./config.yaml"

default: check

fmt:
    cargo fmt --all --check

test:
    cargo test --all-targets

clippy:
    cargo clippy --all-targets --all-features -- -D warnings

deny:
    cargo deny check advisories licenses sources

audit:
    cargo audit

licenses output="THIRD-PARTY-LICENSES.html":
    cargo about generate about.hbs > "{{ output }}"

check: fmt clippy test

build:
    cargo build --release --locked

run config_path="/etc/galaxync/config.yaml":
    cargo run --locked -- --config "{{ config_path }}"

config-validate config_path=config:
    cargo run --locked -- config validate --config "{{ config_path }}"

container-build image_name=image:
    podman build --pull=newer --tag "{{ image_name }}" --file Dockerfile .

container-run image_name=image config_path=config:
    podman run --rm --name galaxync \
        --userns=keep-id \
        --mount type=bind,src="$(realpath "{{ config_path }}")",dst=/etc/galaxync/config.yaml,ro=true \
        --mount type=volume,src=galaxync-data,dst=/var/lib/galaxync \
        --publish 127.0.0.1:9898:9898 \
        "{{ image_name }}"
