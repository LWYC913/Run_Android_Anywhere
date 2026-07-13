# PART 03 — Control-plane API (Rust / Axum)

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Implement the **§7 v1 HTTP API** in Axum against the P2 repository: projects, pre-signed APK/test uploads, idempotent job creation + enqueue, job read/list, the SSE event stream, artifacts, debug-session creation, cancel, webhooks, admin workers, runtime-profiles — all behind **API-key auth with project scoping** (§7c). This Part owns the public surface and nothing below it.

---

## 2. Where this sits

**Upstream (must already exist):**
- **P1** `contracts` crate — the API request/response types, the `JobState` enum + transition guard, and the error taxonomy. Handlers return `contracts` types; they do not redefine them.
- **P1** `openapi/v1.yaml` — the executable contract this Part must satisfy exactly (paths, schemas, status codes).
- **P2** `repository` crate — every persistence call goes through it. Relevant signatures inlined below so you don't need to reopen P2:
  - `create_project`, `get_project`
  - `create_api_key`, `find_api_key_by_hash`, `revoke_api_key`, `touch_api_key_last_used`
  - `create_upload(project_id, kind, s3_key, sha256, size)`, `get_upload`
  - `create_job(CreateJob, idempotency_key) -> Job` (idempotent; unique on `(project_id, idempotency_key)`)
  - `get_job`, `list_jobs(project_id, status?, cursor)` (keyset)
  - `append_job_event`, `stream_job_events(job_id, after_seq)` (SSE source)
  - `add_artifact`, `list_artifacts(job_id)`
  - `list_runtime_profiles`, `get_runtime_profile`
  - `create_debug_session(job_id, jti, created_by, mode, expires_at)`
  - `list_workers`
  - `append_audit(actor, action, subject, payload)`

**Downstream (depends on this):**
- **P4** consumes the **job-created enqueue** this API emits (producer side); P4 owns the consumer/lease/DLQ side.
- **P7** generates its client from the OpenAPI this Part implements; it relies on the SSE stream shape.
- **P8/P9** call `POST /v1/jobs/{job_id}/debug-sessions` and depend on the returned token shape (a JWT validated later at the session gateway).
- **P6** (CLI/Compose) and **P14** (GitHub Action) drive this API.

---

## 3. In scope / Out of scope

**In scope:**
- All §7 endpoints, matching `openapi/v1.yaml`.
- API-key auth middleware: extract bearer key → `find_api_key_by_hash` → attach `{project_id, scopes}` to the request → `touch_api_key_last_used`. Enforce scopes per route (`project:read`, `project:write`, `debug:create`, `admin`).
- S3/MinIO client for **pre-signed upload URLs** (and pre-signed download where the dashboard/worker needs it).
- Idempotent `POST /v1/jobs`: honor `Idempotency-Key`, persist via repository, then **publish the job to the queue** (`jobs.queued` subject — producer only).
- SSE handler reading `stream_job_events`.
- Debug-session endpoint: mint a **short-lived JWT** (audience = job/session, `jti`, `mode`), persist via `create_debug_session`, audit it. (The signing key comes from config/secrets; validation happens in P9.)
- Webhook registration + a delivery hook on job state change (delivery worker can be minimal here; durability hardened in P13/P14).
- Request validation, the error taxonomy mapped to HTTP status + JSON body, structured logs with `job_id` correlation, basic OTel spans (§15e) and a `/metrics` endpoint.

**Out of scope (later Parts):**
- Queue **consumer**, leasing, capability matching, DLQ, reconciler (P4).
- Worker logic (P5). The API never talks to runtimes directly.
- Session gateway / Envoy / TURN and JWT **validation** (P9). This Part only *mints* the token.
- Dashboard (P7). Only the API + OpenAPI is produced here.
- Full secrets backend (P13/§15b) — read signing key/DB/S3 creds from env/config now.

---

## 4. Deliverables

1. **`crates/api`** filled in: Axum router, one module per resource (`projects`, `uploads`, `jobs`, `events`, `artifacts`, `debug_sessions`, `webhooks`, `workers`, `runtime_profiles`).
2. **Auth middleware** + scope guards.
3. **S3 client wrapper** producing pre-signed PUT (upload) and GET (download) URLs.
4. **Queue producer** wrapper that publishes a `JobQueued` message to `jobs.queued` (the message schema is a `contracts` type so P4/P5 share it).
5. **JWT minting** for debug sessions (sign with configured key; embed `aud`, `jti`, `mode`, `exp`).
6. **SSE endpoint** streaming `job_events` ordered by sequence.
7. **OpenAPI conformance check** in CI (e.g. schemathesis/dredd or a contract test) proving handlers match `openapi/v1.yaml`.
8. **Integration tests** against a throwaway Postgres + MinIO (testcontainers): auth, idempotent create, list pagination, SSE, debug-token mint + persisted row.
9. **Config**: env-driven DB URL, S3/MinIO endpoint + bucket, NATS URL, JWT signing key, token TTL.

---

## 5. Key interfaces & contracts (consume from P1/P2; produce here)

- **Endpoints:** exactly the §7 table — `POST /v1/projects`, `POST /v1/uploads/apk`, `POST /v1/jobs` (with `Idempotency-Key`), `GET /v1/jobs/{id}`, `GET /v1/jobs?project_id=&status=&cursor=`, `GET /v1/jobs/{id}/events` (SSE), `GET /v1/jobs/{id}/artifacts`, `POST /v1/jobs/{id}/debug-sessions`, `POST /v1/jobs/{id}/cancel`, `POST /v1/webhooks`, `GET /v1/workers`, `GET /v1/runtime-profiles`.
- **Job-create body (§7):** `{ project_id, apk_upload_id, test_upload_id?, runtime_profile, mode (headless_ci|browser_debug), min_isolation (vm_isolated|shared_kernel_privileged), automation {type, script_ref}, artifacts {screenshots, video, logcat, junit}, timeout_seconds }`.
- **Queue producer message (`JobQueued`, author in `contracts`):** `{ job_id, project_id, runtime_profile (with runtime_kind/abi/host_arch/isolation_tier), min_isolation, timeout_seconds }` → subject `jobs.queued`.
- **Debug token claims:** `{ aud: "<job_id>:<session_id>", jti, mode: viewer|controller, exp }`.
- **Pagination envelope:** `{ items, next_cursor }` (from P1).
- **Errors:** the P1 taxonomy (`validation`, `not_found`, `unauthorized`, `forbidden`, `conflict`, `quota_exceeded`, `infra_failed`) → stable HTTP mapping.

---

## 6. Architecture decisions to honor

- **Contract-first (§7).** The OpenAPI is the source of truth; handlers conform to it, not the reverse. If a handler needs a field the spec lacks, change the spec + `contracts` and note it for `ENHANCED_ARCHITECTURE.md`.
- **Idempotent job creation (§7, §8b)** is the API's responsibility to honor (pass the header through to the repository's unique constraint) — a CI retry must not double-enqueue.
- **Workers use pre-signed URLs (§15b).** The API hands out pre-signed S3 URLs; it never ships long-lived S3 creds to clients or workers.
- **Debug tokens are short-lived, audience-bound, audited (§7c, §11).** Mint here, validate at the gateway (P9). Persist the `jti` so single-use/expiry is enforceable.
- **`min_isolation` is enforced downstream, surfaced here (§6b).** The API accepts and stores it; P4 uses it to exclude redroid workers when `vm_isolated`.
- **Observability from day one (§15e).** Correlate logs by `job_id`; expose Prometheus metrics; start OTel spans at the edge.

---

## 7. Acceptance criteria

1. Every §7 endpoint exists and passes an OpenAPI conformance test against `openapi/v1.yaml`.
2. Unauthenticated requests are rejected; a valid key scopes the caller to its project; scope-guarded routes reject insufficient scopes.
3. `POST /v1/uploads/apk` returns a working pre-signed PUT URL; a subsequent upload + `get_upload` round-trips metadata.
4. `POST /v1/jobs` with a repeated `Idempotency-Key` returns the **same** job and publishes **one** `jobs.queued` message (assert the message count).
5. `GET /v1/jobs/{id}/events` streams events in sequence order as SSE and terminates cleanly on a terminal state.
6. `POST /v1/jobs/{id}/debug-sessions` returns a JWT with correct `aud/jti/mode/exp` and writes a `debug_sessions` row + audit entry.
7. `GET /v1/jobs` paginates by cursor; `GET /v1/runtime-profiles` and `GET /v1/workers` return seeded/live rows.
8. Errors map to the taxonomy with correct status codes and JSON bodies.
9. `/metrics` exposes at least request and job-create counters; logs carry `job_id`.

---

## 8. Risks & gotchas

- **Producer/consumer seam.** This Part only *publishes* `jobs.queued`; do not implement leasing or consumers here — that's P4. Keep the message schema in `contracts` so both sides agree.
- **Pre-signed URL scope.** Bound URLs to the exact key + short expiry; never return bucket-wide credentials.
- **SSE lifecycle.** Flush periodically, send heartbeats, and close the stream when the job reaches a terminal state, or clients hang.
- **Idempotency vs. enqueue atomicity.** If create succeeds but publish fails, you must not lose the job — publish should be retried by the reconciler (P4) from job state, or use an outbox. Note which you choose so P4 inherits it.
- **JWT key handling.** Read the signing key from config now, but treat it as a secret (§15b); never log it. P9 must validate with the matching public/shared key.
- **Don't leak internal state.** `GET /v1/workers` is `admin`-scoped; never expose ADB endpoints or internal runtime addresses through the public API.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the router/module layout, each handler's request→repository→response flow, the auth-middleware design, the pre-signed-URL and JWT-minting approach, the `jobs.queued` producer contract, the SSE design, and the test matrix (incl. the OpenAPI conformance check). **No code until approved.** Stay in scope (§3) — no consumers, no worker, no gateway. If any handler needs a contract or schema change, surface it so `openapi/v1.yaml`, `contracts`, and `ENHANCED_ARCHITECTURE.md` are updated before later Parts inherit it.

---

### Continuity note
**Previous:** `PART_02_DATA_LAYER.md` provides the repository this API calls; `PART_01` defines the types and OpenAPI it implements. **Next:** `PART_04_QUEUE_SCHEDULER_RECONCILER.md` consumes the `jobs.queued` messages this API publishes, adds leasing/capability-matching/DLQ, and builds the reconciler on P2's stale-job queries. **Also soon:** `PART_07` generates its client from this API; `PART_09` validates the debug JWT this API mints.
