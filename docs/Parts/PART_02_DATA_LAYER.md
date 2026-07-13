# PART 02 — Data Layer (Postgres schema, migrations, repository)

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Turn the §7b data model into real **`sqlx` migrations** and a typed **repository crate** that the API (P3) and the reconciler (P4) call. Seed the common runtime profiles. This Part owns persistence and nothing else.

---

## 2. Where this sits

**Upstream (must already exist — from P1):**
- The `contracts` crate with the job-state enum, `RuntimeProfile`, worker-protocol types, and the error taxonomy. Repository function signatures **use these types** rather than redefining them.
- The §7b schema sketch in `ENHANCED_ARCHITECTURE.md`.

Inlined upstream contract you need (from P1, do not redefine — import):
- `JobState` enum + transition guard (terminal states reachable only via `collecting_artifacts`/`cleaning_up`).
- `RuntimeProfile { android_api, device_profile, abi, host_arch, runtime_kind, image_ref, isolation_tier }`.
- `isolation_tier ∈ {vm_isolated, shared_kernel_privileged}`; `runtime_kind ∈ {android_emulator_container, redroid, cuttlefish, browser_native_wasm}`.

**Downstream (depends on this):**
- **P3 (API)** calls the repository for projects, uploads, jobs, events, artifacts, debug sessions, webhooks, workers, runtime-profiles.
- **P4 (reconciler)** queries for zombie jobs (stale `claimed`/`running`) and expired debug sessions, and updates job state.
- **P5 (worker)** indirectly persists via P3/P4, but relies on the `job_events` and `artifacts` tables existing.

---

## 3. In scope / Out of scope

**In scope:**
- The full §7b schema as ordered, reversible `sqlx` migrations.
- A repository crate with typed CRUD + the specific queries P3/P4 need.
- Seed migration for common `runtime_profiles`.
- A local Postgres dev setup (docker run snippet or Compose fragment) and migration-run instructions.

**Out of scope:**
- API handlers (P3), queue logic (P4), worker logic (P5).
- Auth enforcement (P3) — but `api_keys` table + hashing strategy are defined here.
- Object storage wiring (P3 owns S3/MinIO + pre-signed URLs); this Part only stores artifact **metadata** (s3_key, size, sha256).

---

## 4. Deliverables

1. **Migrations** (`migrations/NNNN_*.sql`) creating, in dependency order:
   `projects`, `api_keys`, `uploads`, `runtime_profiles`, `jobs`, `job_events`, `artifacts`, `workers`, `debug_sessions`, `audit_log` — with the columns sketched in §7b, sensible indexes, and FKs.
2. **Repository crate** (`crates/repository/`) exposing typed functions (signatures in §5).
3. **Seed migration** inserting common `runtime_profiles` (a spread across Android API levels / device profiles / ABIs / host arches; mark each row's `runtime_kind` and `isolation_tier`).
4. **Dev DB instructions** + a `just db-migrate` / `just db-reset` target.
5. **Repository tests** against a throwaway Postgres (testcontainers or a dev DB), covering the §7 acceptance queries.

---

## 5. Key interfaces & contracts (author these here)

Repository functions return `contracts` types. Indicative signatures (names flexible):

**Projects / auth**
- `create_project`, `get_project`
- `create_api_key(project_id, scopes) -> {id, plaintext_once}` (store only a **hash**; return plaintext once), `find_api_key_by_hash`, `revoke_api_key`, `touch_api_key_last_used`

**Uploads**
- `create_upload(project_id, kind, s3_key, sha256, size)`, `get_upload`

**Runtime profiles**
- `list_runtime_profiles`, `get_runtime_profile`

**Jobs**
- `create_job(CreateJob, idempotency_key) -> Job` — **idempotent**: same key returns the existing job (unique constraint on `(project_id, idempotency_key)`).
- `get_job`, `list_jobs(project_id, status?, cursor)` (keyset pagination)
- `transition_job_state(job_id, from, to)` — enforce the P1 transition guard at the query layer (conditional UPDATE on current state); reject illegal transitions.
- `assign_worker(job_id, worker_id)`, `set_job_result(job_id, result)`
- **Reconciler queries:** `find_stale_running_jobs(heartbeat_older_than)`, `requeue_or_fail_job`.

**Events / artifacts**
- `append_job_event(job_id, type, payload)` (append-only; ordered), `stream_job_events(job_id, after_seq)` (for the SSE source in P3)
- `add_artifact(job_id, kind, s3_key, size, sha256)`, `list_artifacts(job_id)`

**Workers**
- `upsert_worker(WorkerRegistration)`, `record_heartbeat(WorkerHeartbeat)`, `list_workers`, `find_workers_matching(runtime_kind, abi, host_arch, isolation_tier, has_capacity)` (used by P4's matcher).

**Debug sessions / audit**
- `create_debug_session(job_id, jti, created_by, mode, expires_at)`, `end_debug_session`, `find_expired_debug_sessions`
- `append_audit(actor, action, subject, payload)`

---

## 6. Architecture decisions to honor

- **Metadata in Postgres, blobs in S3** (§14): artifact rows store keys + checksums, never bytes.
- **Idempotent job creation** (§7, §8b): enforced by a unique constraint, not just app logic.
- **State transition guard at the data layer** (§7b/§8b): illegal transitions must be impossible even under concurrent workers — use conditional UPDATEs (`WHERE state = $from`).
- **Reconciler-ready** (§8b): the schema must make "find jobs whose worker heartbeat expired" a cheap query (index `jobs.state` + `workers.last_heartbeat_at`, or denormalize a `last_lease_extended_at` onto `jobs`).
- **Isolation/arch fields are real columns** (§6b/§15d) so P4 can match and P11 can schedule.

---

## 7. Acceptance criteria

1. `just db-migrate` applies all migrations from empty; `just db-reset` rolls back/recreates cleanly.
2. `create_job` with a repeated `Idempotency-Key` returns the **same** job id (unique constraint proven by a test).
3. `transition_job_state` rejects an illegal transition (e.g. `queued → passed`) and accepts a legal one, verified under a concurrent-update test (only one of two racing transitions wins).
4. `find_stale_running_jobs` returns jobs whose owning worker's heartbeat is older than the threshold (reconciler test).
5. `find_workers_matching` returns only workers whose `runtime_kind/abi/host_arch/isolation_tier` satisfy a job and that have spare capacity.
6. Seed migration creates a usable spread of `runtime_profiles` (at least one `vm_isolated` x86_64 emulator profile and one `shared_kernel_privileged` arm64 redroid profile).
7. Repository tests pass against a real Postgres.

---

## 8. Risks & gotchas

- **State races.** Two workers must never both drive the same job. Conditional UPDATEs + the unique idempotency constraint are the guardrails — test them explicitly.
- **Event ordering.** The SSE stream (P3) needs a stable order; give `job_events` a monotonic sequence (bigserial) and index by `(job_id, seq)`.
- **API-key storage.** Never store plaintext keys; hash them, return the plaintext exactly once at creation.
- **Over-reach.** Don't add S3 client code or pre-signed URL logic here — that's P3. This Part stops at metadata.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the migration list (with column/constraint/index detail), the repository function inventory (signatures referencing `contracts` types), the seed-profile list, and the test matrix. **No code until approved.** Stay in scope (§3). If the schema needs a column the architecture didn't list (e.g. `jobs.last_lease_extended_at` for the reconciler), note it so §7b in `ENHANCED_ARCHITECTURE.md` is updated and later Parts inherit it.

---

### Continuity note
**Previous:** `PART_01_FOUNDATIONS.md` defined the `contracts` types this repository imports. **Next:** `PART_03_CONTROL_PLANE_API.md` implements the §7 OpenAPI against this repository (pre-signed uploads, idempotent job create + enqueue, SSE from `job_events`, API-key auth). **Also soon:** `PART_04` builds the reconciler on the stale-job/expired-session queries defined here.
