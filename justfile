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

api-build:
    cargo build -p run-anywhere-api --all-targets --all-features --locked

api-test:
    cargo test -p run-anywhere-api --all-features --locked

# Run the live PostgreSQL, MinIO, and JetStream probes after verifying that
# each local dependency is reachable.
api-integration-test: infra-check
    node -e "const {spawnSync}=require('node:child_process'); const result=spawnSync('cargo',['test','-p','run-anywhere-api','--all-features','--locked'],{stdio:'inherit',env:{...process.env,RUN_OBJECT_STORE_INTEGRATION:'true',RUN_QUEUE_INTEGRATION:'true'}}); if(result.error) throw result.error; process.exit(result.status ?? 1);"

api-check: api-test openapi-lint contract-check

# Verify the local Part 3 dependencies without mutating them. NATS monitoring
# must be available on port 8222 so this also proves JetStream is enabled.
infra-check:
    node -e "const net=require('node:net'); const tcp=(name,url)=>new Promise((resolve,reject)=>{const parsed=new URL(url); const socket=net.createConnection({host:parsed.hostname,port:Number(parsed.port)},()=>{socket.end(); resolve();}); socket.setTimeout(3000,()=>socket.destroy(new Error(name+' timed out'))); socket.on('error',error=>reject(new Error(name+': '+error.message)));}); const get=async(name,url)=>{const response=await fetch(url,{signal:AbortSignal.timeout(3000)}); if(!response.ok) throw new Error(name+': HTTP '+response.status);}; const source=new URL(process.env.NATS_URL); const nats=new URL('http://'+source.host); nats.port=process.env.NATS_MONITOR_PORT||'8222'; nats.pathname='/jsz'; Promise.all([tcp('PostgreSQL',process.env.DATABASE_URL),get('NATS JetStream',nats),get('MinIO',new URL('/minio/health/live',process.env.S3_ENDPOINT))]).then(()=>console.log('PostgreSQL, NATS JetStream, and MinIO are ready')).catch(error=>{console.error(error.message); process.exit(1);});"

# Refuse to replace an existing key. This key is for local development only.
dev-jwt-key:
    node -e "const fs=require('node:fs'); const path='.local/debug-jwt-private.pem'; if(fs.existsSync(path)){console.error(path+' already exists; refusing to overwrite it'); process.exit(1);} fs.mkdirSync('.local',{recursive:true});"
    openssl genpkey -algorithm Ed25519 -out .local/debug-jwt-private.pem

api-run: infra-check db-migrate
    cargo run -p run-anywhere-api --locked

msrv:
    rustup run 1.85.0 cargo test --workspace --all-features --locked

ci: fmt-check clippy test build openapi-lint contract-check
