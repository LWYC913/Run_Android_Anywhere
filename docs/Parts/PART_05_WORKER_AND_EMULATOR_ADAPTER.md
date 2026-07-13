# PART 05 — Worker Core + Android Emulator Adapter

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Build the **worker** — the process that turns a claimed job into a real Android run — and the **runtime adapter trait**, then implement the first adapter: **Android Emulator in a container (KVM)**. End state: a headless job claims, boots an emulator, installs the APK, runs the built-in smoke test, uploads logcat/screenshot artifacts through the **finalizer gate**, cleans up, and completes. This is §8 + §6b (default runtime) + §14 made real.

---

## 2. Where this sits

**Upstream (must already exist):**
- **P1** `contracts` — `JobState` + transition guard, worker-protocol types, error taxonomy. Inlined:
  - `WorkerRegistration { worker_id, runtimes[], kvm, gpu, arch, capacity }`, `WorkerHeartbeat { …, lease_extends[], … }`, `JobClaim`, `JobLeaseExtension`, `JobResult`.
  - `RuntimeProfile { android_api, device_profile, abi, host_arch, runtime_kind, image_ref, isolation_tier }`.
  - State spine: `claimed → provisioning_runtime → booting → installing_apk → running_tests → (debug_available) → collecting_artifacts → cleaning_up → passed|failed|timed_out|infra_failed`. **No terminal state before artifacts are confirmed uploaded.**
- **P4** queue — the **pull consumer + ack-deadline lease**; the worker claims here and extends the lease via heartbeat; acks on success, lets it term→DLQ on repeated failure.
- **P3** API — issues **pre-signed S3 URLs** for pulling APK/test artifacts and pushing results; the worker holds **no long-lived S3 creds** (§15b).

**Downstream (depends on this):**
- **P6** wires this worker into the one-command Compose demo.
- **P10** adds Appium on top of this worker loop.
- **P12** adds the redroid + Cuttlefish adapters behind the same trait this Part defines.
- **P9** exposes the emulator's gRPC `:8554` from a running job for browser debug.

---

## 3. In scope / Out of scope

**In scope:**
- **Worker loop (§8):** register (caps incl. `kvm`, `gpu`, `arch`, `runtimes`, `capacity`) → claim → pull artifacts (pre-signed) → start runtime via adapter → heartbeat-extend lease throughout → wait for boot/health → install APK (idempotent) → run smoke flow → collect + **upload artifacts (finalizer)** → wipe runtime → mark complete (idempotent transition).
- **Runtime adapter trait** abstracting runtime specifics (boot, health, install, exec/smoke, capture, teardown), with the **Android Emulator container** adapter as the reference impl.
- **Android Emulator adapter:** launch the emulator container headless (`-no-window`), with `-grpc <port>` enabled (default `:8554`) so P9 can attach later; internal-only ADB; boot/health detection; APK install; built-in smoke profile (launch main activity, wait for idle, screenshot, logcat, crash detection, optional short monkey).
- **Artifact finalizer (§14):** multipart upload for large blobs, retries w/ backoff, checksums; **the job cannot transition to a terminal state until uploads are confirmed** — model this as a gate in the worker that drives the `collecting_artifacts → cleaning_up → terminal` path.
- **Cleanup:** wipe runtime/session state; for `vm_isolated` untrusted jobs, destroy the runtime (don't reuse).
- **Timeouts:** honor `timeout_seconds`; on expiry, capture what's available, finalize, mark `timed_out`, clean up.
- Structured logs + OTel spans correlated by `job_id`/`worker_id` (§15e).

**Out of scope (later Parts):**
- Appium server + driver provisioning (P10) — leave a clean seam ("start Appium if requested").
- redroid/Cuttlefish adapters (P12) — only the **trait** + emulator adapter here.
- The browser debug **path** (gateway/Envoy/TURN, P9) — this Part only ensures `:8554` is enabled and reachable internally.
- K8s pod-per-job creation (P11) — here the worker starts a **local container**.

---

## 4. Deliverables

1. **`crates/worker`** — the worker binary: registration, claim loop, lease heartbeat, run orchestration, finalizer, cleanup, completion.
2. **`RuntimeAdapter` trait** (signatures in §5) + a registry keyed by `runtime_kind`.
3. **`AndroidEmulatorContainerAdapter`** implementing the trait against an Android Emulator container image (KVM).
4. **Smoke-test runner** (built-in profile) producing screenshot + logcat + crash signal + pass/fail.
5. **Artifact uploader** with multipart/retry/checksum and the **finalizer gate** tied to the state machine.
6. **Worker config:** API/queue URLs, declared capabilities, runtime image refs, ADB/gRPC ports, concurrency.
7. **Tests:** claim→run→finalize happy path against a real (or CI-available) emulator or a fake adapter; finalizer blocks terminal state until upload confirmed; idempotent re-run on redelivery; timeout path.

---

## 5. Key interfaces & contracts

Indicative trait (names flexible; must use `contracts` types):

```rust
#[async_trait]
trait RuntimeAdapter {
    fn runtime_kind(&self) -> RuntimeKind;
    async fn provision(&self, profile: &RuntimeProfile, job: &JobClaim) -> Result<RuntimeHandle>;
    async fn await_boot(&self, h: &RuntimeHandle) -> Result<()>;
    async fn install_apk(&self, h: &RuntimeHandle, apk: &LocalArtifact) -> Result<()>; // idempotent
    async fn run_smoke(&self, h: &RuntimeHandle, spec: &SmokeSpec) -> Result<SmokeOutcome>;
    async fn capture(&self, h: &RuntimeHandle, what: ArtifactKind) -> Result<LocalArtifact>;
    fn debug_endpoint(&self, h: &RuntimeHandle) -> Option<GrpcEndpoint>; // emulator :8554
    async fn teardown(&self, h: RuntimeHandle, destroy: bool) -> Result<()>;
}
```

- **Worker→API/queue:** `WorkerRegistration`, periodic `WorkerHeartbeat` (with `lease_extends`), `JobResult` (idempotent completion).
- **Finalizer contract:** artifacts confirmed in S3 (keys + checksums recorded via the API/repository) **before** any terminal transition.
- **Capabilities:** the worker advertises `kvm: true` for the emulator adapter; P4's matcher relies on this + `arch`/`runtimes`.

---

## 6. Architecture decisions to honor

- **Android Emulator is the default untrusted runtime (§6b)** — VM isolation via KVM; this adapter is the baseline others are compared against.
- **Enable `-grpc :8554` at launch (§11b)** so P9's debug path has a contract to attach to; keep it **internal-only**.
- **Hard finalizer (§14)** — no terminal state until artifacts are uploaded; this is a *state-machine* guarantee, not a best-effort upload.
- **Idempotent side-effects (§8b)** — install, upload, and completion must be safe under queue redelivery.
- **Destroy untrusted runtimes (§15c/§15)** — `vm_isolated` untrusted jobs get a fresh runtime; no data reuse unless a trusted snapshot pool is used (P13).
- **No long-lived secrets on the worker (§15b)** — pull/push via pre-signed URLs only.
- **Pre-signed pulls + multipart pushes (§14)** for robustness on large video/log artifacts.

---

## 7. Acceptance criteria

1. The worker registers, claims a queued job, and **heartbeat-extends the lease** for the job's duration (no spurious redelivery on a healthy run).
2. The emulator boots headless, the APK installs, and the smoke test captures a screenshot + logcat and emits pass/fail.
3. The job **cannot** reach `passed`/`failed` until artifacts are confirmed uploaded (kill the uploader mid-run → job stays pre-terminal / goes `infra_failed`, never a terminal "passed" without artifacts).
4. Redelivery of the same job (simulated worker crash) does not double-install or double-complete (idempotency proven).
5. A job exceeding `timeout_seconds` is finalized with available artifacts and marked `timed_out`, then cleaned up.
6. After completion, the runtime is wiped/destroyed (no leftover container for an untrusted `vm_isolated` job).
7. The adapter exposes an internal gRPC `:8554` endpoint for a running job (verified reachable in-cluster/host, not publicly).
8. Logs/traces correlate by `job_id`/`worker_id`.

---

## 8. Risks & gotchas

- **KVM availability.** The emulator adapter needs `/dev/kvm`; on a dev box without it, software mode is far slower — gate the adapter on advertised `kvm` capability and fail clearly otherwise (this is exactly why P12/redroid exists).
- **Boot detection flakiness.** "Booted" ≠ "ready"; wait for `sys.boot_completed` + package manager readiness, not a fixed sleep, or installs race the boot.
- **Finalizer is the whole point.** The most common bug is marking a job done before the video finishes uploading. Make the terminal transition *depend* on upload confirmation, and test the failure path explicitly.
- **Idempotent install.** Re-installing on redelivery must not error on "already installed"; use replace/idempotent semantics.
- **Teardown leaks.** A panic mid-run must still trigger teardown (use a drop guard / structured cleanup) or you leak containers the reconciler (P4) then has to reap.
- **Trait over-fitting.** Don't bake emulator-only assumptions (KVM, x86) into the trait — P12 must slot redroid (no KVM, ARM) and Cuttlefish behind it unchanged.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the worker loop state diagram, the `RuntimeAdapter` trait + the emulator adapter design, the smoke-runner steps, the finalizer/state-machine coupling, cleanup/timeout handling, capability advertisement, and the test matrix (incl. the finalizer-blocks-terminal test). **No code until approved.** Stay in scope (§3) — trait + emulator adapter only; no Appium, no other runtimes, no K8s pods, no debug path beyond enabling `:8554`. If the trait or worker protocol needs a new field, surface it for `contracts`/§8 in `ENHANCED_ARCHITECTURE.md`.

---

### Continuity note
**Previous:** `PART_04_QUEUE_SCHEDULER_RECONCILER.md` defines the consumer/lease this worker claims against; `PART_03` issues the pre-signed URLs it uses. **Next:** `PART_06_COMPOSE_STACK.md` assembles this worker + the API into the one-command local demo. **Also soon:** `PART_10` adds Appium to this loop; `PART_12` adds redroid + Cuttlefish behind the trait defined here; `PART_09` attaches the browser debug path to the `:8554` endpoint this adapter exposes.
