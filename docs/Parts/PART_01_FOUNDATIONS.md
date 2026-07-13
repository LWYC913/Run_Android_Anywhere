# PART 01 — Foundations & Contracts

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Stand up the monorepo and author the **shared contracts** — the job-state machine, API types, worker-protocol types, error taxonomy, and the OpenAPI spec — that every later Part depends on, plus CI that builds the whole workspace. This Part writes almost no business logic; it fixes the seams so nothing downstream can drift.

---

## 2. Where this sits

**Upstream (must already exist):** only `ENHANCED_ARCHITECTURE.md`. This is the first Part.

**Downstream (depends on this):** *every* other Part. P2 uses the contract types in its repository signatures; P3 implements the OpenAPI; P4 uses the worker-protocol + job-state types; P5 implements the worker side of those types; P7/P8 generate clients from the OpenAPI.

Because everything depends on P1, the contracts defined here are the most expensive things to change later. Spend the session getting the **types and the OpenAPI** right, not on implementation.

---

## 3. In scope / Out of scope

**In scope:**
- Monorepo + Rust workspace skeleton.
- A `contracts` crate (pure types + the job-state machine, no I/O).
- The OpenAPI 3.1 spec for the v1 API (the §7 endpoints).
- Error taxonomy shared across services.
- Dev tooling: task runner, formatting, linting, and CI that compiles every crate and validates the OpenAPI.

**Out of scope (later Parts):**
- Any DB code (P2), API handlers (P3), queue code (P4), worker logic (P5).
- Auth *implementation* (P3) — but the auth-related types/headers belong in the contracts/OpenAPI here.
- Frontend (P7). The OpenAPI is produced here; the client is generated in P7.

---

## 4. Deliverables

1. **Repo layout** (proposed; adjust names but keep the separation):
   ```
   /                      workspace root
   ├─ Cargo.toml          (Rust workspace)
   ├─ justfile|Makefile   (dev tasks)
   ├─ crates/
   │   ├─ contracts/      types + state machine (no I/O)        ← the heart of P1
   │   ├─ api/            (empty skeleton; filled in P3)
   │   ├─ scheduler/      (empty skeleton; filled in P4)
   │   └─ worker/         (empty skeleton; filled in P5)
   ├─ openapi/
   │   └─ v1.yaml         OpenAPI 3.1 spec                       ← the API contract
   ├─ web/                (empty skeleton; filled in P7)
   ├─ packages/
   │   └─ browser-emulator/  (empty skeleton; filled in P8)
   ├─ deploy/
   │   ├─ compose/        (filled in P6)
   │   └─ k8s/            (filled in P11)
   ├─ docs/
   └─ .github/workflows/  CI
   ```
2. **`contracts` crate** containing the types in §6 below.
3. **`openapi/v1.yaml`** describing the §7 endpoints, with the request/response schemas matching the `contracts` types.
4. **CI** that: builds the workspace, runs clippy + rustfmt check, and lints/validates the OpenAPI spec.
5. **`docs/CONTRIBUTING.md`** stub explaining the contract-first rule: *types change in `contracts`, API shape changes in `openapi/v1.yaml`, and any change is reflected back into `ENHANCED_ARCHITECTURE.md`.*

---

## 5. Key interfaces & contracts (author these here)

### 5.1 Job state machine (the spine — used by P3, P4, P5)

States (from §7 of the architecture):
```
queued → claimed → provisioning_runtime → booting → installing_apk
       → running_tests → (debug_available) → collecting_artifacts → cleaning_up
       → passed | failed | cancelled | timed_out | infra_failed
```
Encode as a Rust enum plus an explicit **allowed-transitions** function. Terminal states: `passed`, `failed`, `cancelled`, `timed_out`, `infra_failed`. The architecture requires that **a job cannot reach a terminal state until artifacts are confirmed uploaded** (the finalizer, §14) — model the transition guard so `collecting_artifacts → terminal` is the only path and it carries an "artifacts_uploaded" precondition.

### 5.2 API types (mirror the OpenAPI)
- `CreateJobRequest` — fields per §7: `project_id`, `apk_upload_id`, optional `test_upload_id`, `runtime_profile`, `mode` (`headless_ci` | `browser_debug`), `min_isolation` (`vm_isolated` | `shared_kernel_privileged`), `automation` (`{type, script_ref}`), `artifacts` (`{screenshots, video, logcat, junit}`), `timeout_seconds`.
- `Job`, `JobSummary`, `JobEvent`, `Artifact`, `RuntimeProfile`, `WorkerStatus`, `DebugSessionRequest`/`DebugSessionToken`, `Webhook`.
- Pagination envelope (`{ items, next_cursor }`) for list endpoints.
- Auth: bearer API key on all endpoints; `Idempotency-Key` header on `POST /v1/jobs`.

### 5.3 Worker protocol types (used by P4, P5)
- `WorkerRegistration` `{ worker_id, runtimes[], kvm, gpu, arch, capacity }`.
- `WorkerHeartbeat` `{ worker_id, active_jobs, capacity, runtimes[], kvm, gpu, arch, lease_extends[], last_seen }`.
- `JobClaim`, `JobLeaseExtension`, `JobResult`.

### 5.4 `RuntimeProfile` (used by P2 seed, P4 matcher, P5/P12 adapters)
Must encode `{ android_api, device_profile, abi, host_arch, runtime_kind, image_ref, isolation_tier }` (§15d). `runtime_kind ∈ {android_emulator_container, redroid, cuttlefish, browser_native_wasm}`. `isolation_tier ∈ {vm_isolated, shared_kernel_privileged}`.

### 5.5 Error taxonomy
A shared error type with stable codes (e.g. `validation`, `not_found`, `unauthorized`, `forbidden`, `conflict`, `quota_exceeded`, `infra_failed`) so the API, worker, and clients agree on semantics.

---

## 6. Architecture decisions to honor

- **Contract-first.** The §7 API shape and the §7b data model are the references; the OpenAPI and `contracts` crate are their executable form.
- **Isolation tiering exists from the start** (§6b): `min_isolation` on jobs and `isolation_tier` on runtime profiles are first-class fields **now**, even though enforcement lands in P4/P5/P11.
- **`host_arch` is a first-class field** (§15d) so the matcher (P4) and adapters (P5/P12) can rely on it.
- **The finalizer is modeled in the state machine** (§14), not bolted on in the worker.

---

## 7. Acceptance criteria

1. `just build` (or `make build`) compiles the entire workspace, including the empty service skeletons.
2. The `contracts` crate exposes the job-state enum + a transition function with unit tests asserting: valid transitions pass, invalid ones are rejected, and no terminal state is reachable except via `collecting_artifacts`/`cleaning_up`.
3. `openapi/v1.yaml` validates against an OpenAPI 3.1 linter in CI and covers every §7 endpoint with schemas that match the `contracts` types.
4. CI runs clippy + rustfmt check + OpenAPI lint and is green.
5. `docs/CONTRIBUTING.md` states the contract-first rule.

---

## 8. Risks & gotchas

- **Over-building.** The temptation is to start writing handlers/DB code. Resist — those are P2/P3. P1's value is *only* the contracts and the build.
- **Type/spec divergence.** If you hand-write both the OpenAPI and the Rust types, they can drift. Either generate one from the other, or add a CI check that fails on mismatch.
- **Premature auth detail.** Define the *shape* (bearer key, scopes, idempotency header) here; implement validation in P3.

---

## 9. Plan-mode instructions

Produce an **implementation plan only** — file tree, the exact list of types and their fields, the OpenAPI endpoint/schema inventory, the CI steps, and the task-runner targets. **Do not write code until the plan is approved.** Stay strictly within scope (§3). If anything forces a change to the §7 API shape or §7b data model, call it out explicitly so it can be recorded back into `ENHANCED_ARCHITECTURE.md` before later Parts inherit it.

---

### Continuity note
**Next:** `PART_02_DATA_LAYER.md` turns the §7b data model into `sqlx` migrations + a repository crate whose function signatures use the `contracts` types defined here. **After that:** `PART_03` implements the OpenAPI authored here against that repository.
