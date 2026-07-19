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

db-migrate:
    sqlx migrate run --source migrations

db-reset:
    node -e "const raw = process.env.DATABASE_URL; let url; try { url = new URL(raw); } catch { console.error('db-reset refused: DATABASE_URL must be a valid PostgreSQL URL'); process.exit(1); } const loopback = new Set(['localhost', '127.0.0.1', '::1', '[::1]']); const database = decodeURIComponent(url.pathname.slice(1)); if (!['postgres:', 'postgresql:'].includes(url.protocol) || !loopback.has(url.hostname) || database !== 'run_anywhere_dev') { console.error('db-reset refused: DATABASE_URL must target run_anywhere_dev on a loopback host'); process.exit(1); }"
    sqlx database drop -y
    sqlx database create
    sqlx migrate run --source migrations

repository-test:
    cargo test -p run-anywhere-repository --all-features --locked

msrv:
    rustup run 1.85.0 cargo test --workspace --all-features --locked

ci: fmt-check clippy test build openapi-lint contract-check
