# PART 10 — Appium & Automation

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Add real QA automation to the worker: **Appium 2.x** running inside the worker (drivers provisioned separately), a **per-job internal Appium endpoint**, **test ingestion** (zip / git / container / built-in smoke), and **JUnit/JSON report parsing** into artifacts. This turns the grid from "smoke tests only" into an actual automation runner (§13).

---

## 2. Where this sits

**Upstream (must already exist):**
- **P5** worker + Android Emulator adapter — the run loop with the **"start Appium server if requested"** seam and the artifact finalizer. This Part fills that seam.
- **P3** API — the job-create `automation` block and pre-signed URLs for fetching test bundles. Inlined (§7):
  - `automation: { type: "appium", script_ref: "s3://.../tests/login-flow.zip" }`.
  - Artifact toggles include `junit: true`.
- **P2** repository — `artifacts` rows store report metadata (s3_key/size/sha256).

Inlined facts (§13):
- **Appium 2.x changed the driver model** — drivers (e.g. UiAutomator2) install **separately** from the server; the worker image must provision the needed drivers.
- Tests may be a **zip bundle, a git reference, a container image, or the built-in smoke profile**; results should be **JUnit/XML or JSON** where possible.
- ADB stays **internal-only**; Appium talks to the device over the worker's internal ADB.

**Downstream (depends on this):**
- **P11** ensures the worker/runtime pods include the Appium drivers in their images and that the internal Appium endpoint isn't publicly exposed.
- **P13** adds automation metrics (pass/fail rate, test duration) and hardening.

---

## 3. In scope / Out of scope

**In scope:**
- **Appium 2.x in the worker image** with the required drivers (UiAutomator2 at minimum) provisioned at build time.
- **Per-job internal Appium endpoint**: start an Appium server bound to the job's runtime, internal-only; tear it down with the job.
- **Test ingestion** for `automation.type = appium`:
  - **zip**: fetch via pre-signed URL, unpack, run.
  - **git**: clone a ref.
  - **container**: run a provided test-runner image against the internal Appium/ADB endpoint.
  - **built-in smoke**: the existing P5 profile remains the default when no automation is specified.
- **Report parsing**: collect JUnit/XML or JSON results → normalize → write as artifacts (with pass/fail summary feeding the job result).
- **Result → job state**: test outcome drives `passed`/`failed`; infra issues map to `infra_failed` (distinct).
- Logs/traces correlated by `job_id`; Appium server logs captured as an artifact.

**Out of scope (later Parts):**
- iOS/Safari or non-Android drivers.
- A hosted test-authoring UI.
- Parallel test sharding across multiple devices for one job (one emulator = one job, §1) — multi-device matrices are separate jobs orchestrated by the client/CI (P14).
- K8s image/secret wiring (P11) and automation dashboards (P13).

---

## 4. Deliverables

1. **Worker image update**: Appium 2.x server + drivers provisioned separately (documented driver list + versions).
2. **Appium lifecycle** in the worker run loop: start (per job, internal), health-check, run, capture logs, tear down.
3. **Test ingestion adapters**: zip, git, container, built-in smoke (a small strategy interface keyed by source type).
4. **Report parser**: JUnit/XML + JSON → normalized result + artifacts.
5. **Result mapping**: tie parsed outcomes to the job state machine + summary in `JobResult`.
6. **Tests**: a sample Appium zip runs against the emulator and produces a JUnit report artifact; failure and crash paths classified correctly; Appium teardown verified.

---

## 5. Key interfaces & contracts

- **Job automation contract (§7):** `automation { type, script_ref }`; `type=appium` triggers this path; absence ⇒ built-in smoke (P5).
- **Ingestion strategy:** `trait TestSource { async fn fetch_and_prepare(...) -> PreparedTests }` with zip/git/container/smoke impls.
- **Appium endpoint:** internal URL handed to the test runner/container; never exposed publicly; ADB internal-only.
- **Report contract:** normalized `{ total, passed, failed, skipped, duration, cases[] }` + raw report file stored as an artifact (`kind = junit|json`).
- **Finalizer (P5/§14):** reports + Appium logs are artifacts → must be uploaded before the job reaches a terminal state.

---

## 6. Architecture decisions to honor

- **Appium 2.x driver model (§13)** — drivers installed separately from the server; the worker image must bake them in; document versions.
- **Internal-only Appium/ADB (§15)** — no public ADB; the Appium endpoint is per-job and internal; tear down with the job.
- **One emulator = one job (§1)** — Appium drives a single device per job; multi-device is multiple jobs.
- **Reports are artifacts under the finalizer (§14)** — results upload before terminal state; partial results on timeout are still captured.
- **Outcome vs. infra distinction** — a test failure is `failed`; an Appium/runtime breakdown is `infra_failed` (matches the state machine + §15e SLO "success rate excluding infra_failed").
- **Built-in smoke stays the zero-config default (§13/§16)** — automation is opt-in via `automation`.

---

## 7. Acceptance criteria

1. The worker image contains Appium 2.x + the required drivers (provisioned separately), verifiable at build.
2. A job with `automation.type=appium` and a zip `script_ref` fetches, runs against the emulator, and produces a **JUnit report artifact**.
3. git and container test sources also run end-to-end (at least smoke-level coverage each).
4. Parsed results drive the job result: a failing test ⇒ `failed`; an Appium/runtime breakdown ⇒ `infra_failed`.
5. Appium server logs are captured as an artifact; the Appium endpoint is internal-only and torn down with the job.
6. On `timeout_seconds`, partial results/logs are still finalized before `timed_out`.
7. The built-in smoke profile still runs unchanged when no `automation` is specified.

---

## 8. Risks & gotchas

- **Driver/server version drift (Appium 2.x).** Mismatched server/driver versions fail at runtime; pin and document both in the worker image.
- **Endpoint exposure.** The per-job Appium endpoint must be internal-only; accidentally exposing it (or ADB) is a security hole (§15).
- **Untrusted test code.** Container/git test sources can run arbitrary code — run under the job's isolation tier and egress restrictions (§15); don't grant extra privileges.
- **Report format variance.** "JUnit" output varies across frameworks; normalize defensively and always keep the raw report as an artifact.
- **Boot/readiness races.** Start Appium only after the device is truly ready (reuse P5's readiness signal), or sessions fail to attach.
- **Teardown leaks.** A crashed test must still stop the Appium server and clean the runtime (drop-guard cleanup), or the reconciler (P4) has to reap it.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the worker-image driver provisioning, the per-job Appium lifecycle, the test-ingestion strategy interface + the four sources, the report parser + normalized schema, the result→state mapping, and the test matrix. **No code until approved.** Stay in scope (§3) — Android only, single device per job, no K8s/dashboards. If a new artifact kind or automation field is needed, surface it for §7/§13/§14 in `ENHANCED_ARCHITECTURE.md`.

---

### Continuity note
**Previous:** `PART_05_WORKER_AND_EMULATOR_ADAPTER.md` provides the run loop and the "start Appium if requested" seam this Part fills; `PART_03` defines the `automation` job field. **Next:** `PART_11_KUBERNETES_DEPLOY.md` scales the worker (with Appium drivers baked into images) onto Kubernetes. **Also soon:** `PART_13` adds automation metrics + hardening; `PART_14` orchestrates multi-device matrices from CI as separate jobs.
