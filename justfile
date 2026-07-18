set dotenv-load := true
set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

npm := if os() == "windows" { "npm.cmd" } else { "npm" }

build:
    cargo build --workspace --all-targets --all-features --locked

test:
    cargo test --workspace --all-features --locked

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

openapi-lint:
    {{npm}} run openapi:lint

contract-check:
    cargo test -p run-anywhere-contracts --test openapi_parity --locked

msrv:
    rustup run 1.85.0 cargo test --workspace --all-features --locked

ci: fmt-check clippy test build openapi-lint contract-check
