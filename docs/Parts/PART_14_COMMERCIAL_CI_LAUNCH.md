# PART 14 — Commercial Layer, CI Integrations & Launch

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Make the open-core boundary concrete and ship the go-to-market: a **GitHub Action + reusable workflow + webhooks** (CI without long-polling), **team auth/SSO/RBAC** scaffolding, **billing/quotas**, **retention tiers**, **managed-runner hooks**, the **OSS/commercial split** enforced, **Apache-2.0 + commercial terms** with licensing/redistribution caveats, and the **community launch**. This realizes §19–§22 and launch Phases 2–4.

---

## 2. Where this sits

**Upstream (must already exist):**
- **P3** API — webhooks endpoint + the idempotent job API the Action/workflow drive; API-key + optional OIDC auth (§7c).
- **P7** dashboard shell — extended here with team/org/SSO/RBAC + billing/retention surfaces.
- **P13** telemetry/quotas — billing/retention/SLA features consume these metrics and the per-project quota mechanism.
- **P11** Kubernetes — the managed-runner fleet runs here; commercial features deploy on it.

Inlined boundary (§19/§20):
- **OSS keeps:** Rust control-plane core; worker agent; runtime adapter SDK; Android Emulator + redroid (+ Cuttlefish) adapters; Compose quickstart; basic dashboard; basic artifacts; Appium; WebRTC/debug where legally/technically safe; `@run-anywhere/browser-emulator` (remote WebRTC + experimental provider interface).
- **Commercial keeps:** managed KVM/GPU runner fleet; team auth/SSO; advanced dashboards; long retention; enterprise audit; private networking; large device matrix; SLA/support; billing/quotas; advanced malware/risk scanning.
- **Webhooks** let CI avoid long-polling the events stream (§7).
- **Licensing caveat (§19):** Android system images + GMS/Play Services can't be freely redistributed in a hosted product; redroid bundles third-party modules — examine licenses; keep image provisioning **user-supplied** where licensing requires.

**Downstream (depends on this):**
- This is the top of the stack; **P15** (research) stays explicitly off this revenue/QA path.

---

## 3. In scope / Out of scope

**In scope:**
- **GitHub Action + reusable workflow**: upload APK → create job (idempotent) → wait via **webhooks** (not long-poll) → surface artifacts/pass-fail in the CI run; a sample workflow in the demo repo (launch Phase 2).
- **Webhook delivery hardening**: signed payloads, retries/backoff, delivery logs (building on P3's basic hook).
- **Team auth/SSO/RBAC scaffolding** (commercial): org/team model, SSO/SAML, fine-grained roles beyond the OSS scopes, audit export (§7c/§20).
- **Billing/quotas**: subscriptions/credits, per-project/-org quota enforcement (on P13's mechanism), usage metering from telemetry.
- **Retention tiers**: OSS configurable local retention; hosted free (short) vs paid (long + export) (§14/§20).
- **Managed-runner hooks**: "deploy this app to hosted Run Anywhere" from the CLI; managed KVM/GPU fleet integration (launch Phase 4) — closed/hosted billing.
- **OSS/commercial split enforcement**: clean module/edition boundary so OSS builds exclude commercial-only features; licensing headers.
- **Licensing/terms**: **Apache-2.0** for SDK/player/CLI; separate **commercial license/terms** for hosted features; document the **image redistribution caveat** and keep image provisioning user-supplied where required.
- **Community launch plan execution**: demo repo, one-command demo, hosted demo video/screenshots, honest "what doesn't work," benchmarks/compat matrix publication, adapter plugin API announcement (§21).

**Out of scope (later/never-here):**
- Browser-native WASM research (P15).
- New runtimes (P12) or core scheduler changes (P4).
- Re-implementing telemetry/quotas (P13) — consume them.

---

## 4. Deliverables

1. **GitHub Action** (+ **reusable workflow**) and a sample `.github/workflows/*.yml` in the demo repo, webhook-driven.
2. **Webhook hardening**: HMAC-signed payloads, retry policy, delivery log/admin view.
3. **Team/org/SSO/RBAC** scaffolding in API + dashboard (commercial edition), with audit export.
4. **Billing/quotas**: plan model, usage metering from P13 telemetry, quota enforcement surfaced as `quota_exceeded`.
5. **Retention tiers** + export.
6. **Managed-runner hooks** + CLI "deploy to hosted" path (hosted billing closed).
7. **Edition boundary**: build flags/modules separating OSS vs commercial; license headers; **Apache-2.0** + commercial terms files; **redistribution-caveat docs**.
8. **Launch kit**: updated demo repo/README, demo video/screenshots, benchmark + compatibility matrix (from P12), adapter plugin API docs, community announcement.

---

## 5. Key interfaces & contracts

- **Webhooks (§7):** registered via `POST /v1/webhooks`; signed event payloads on job state changes; the Action consumes these instead of polling.
- **GitHub Action contract:** inputs (apk path, runtime profile, mode, timeout, project/key) → outputs (job id, status, artifact URLs); fails the CI step on `failed`/`infra_failed`.
- **RBAC:** commercial roles extend the OSS scopes (`project:read/write`, `debug:create`, `admin`) with org/team/SSO mappings.
- **Billing/quota:** metering keyed on the same per-project/runtime labels as P13; enforcement via P4's quota mechanism.
- **Edition flag:** a clear OSS-vs-commercial compile/runtime boundary; OSS artifacts must build and run without commercial modules.

---

## 6. Architecture decisions to honor

- **Open-core, not open hosted marketplace (§19/§20)** — give away developer utility; keep hosted runtime economics, scaling, enterprise, and billing closed.
- **CI without long-polling (§7)** — webhooks (signed, retried) are the CI integration path; the Action waits on them.
- **Apache-2.0 for SDK/player/CLI; commercial terms for hosted (§22)** — patent protection + commercial-friendly; AGPL only if forcing hosted contributions (noted as a tradeoff).
- **Image redistribution caveat (§19)** — don't bundle non-redistributable Android system images/GMS; user-supplied provisioning where licensing requires; document it.
- **Narrow positioning (§2/§25)** — market the QA/CI/browser-debug wedge; avoid "free replacement for Appetize/Genymotion/Anbox" overpromises.
- **Commercial features consume, don't fork, the core (§20)** — RBAC/billing/retention build on P3/P11/P13, keeping one control plane.

---

## 7. Acceptance criteria

1. A repo using the **GitHub Action** uploads an APK, runs a job, **waits via webhook**, and fails/passes the CI step on the job result — sample workflow included.
2. Webhook payloads are **signed** and **retried**; deliveries are logged/auditable.
3. The **commercial edition** adds org/team/SSO/RBAC + audit export; the **OSS edition builds and runs without** commercial modules (edition boundary proven).
4. **Billing/quota** meters usage from telemetry and enforces per-project/org caps (`quota_exceeded`); retention tiers apply (short free / long paid + export).
5. The **managed-runner "deploy to hosted"** CLI path works against the hosted fleet (billing closed).
6. **License files** (Apache-2.0 + commercial terms) and the **redistribution-caveat docs** are present; no non-redistributable images are bundled.
7. The **launch kit** is publishable: demo repo + one-command demo + video/screenshots + benchmark/compat matrix + adapter plugin API docs + honest limitations.

---

## 8. Risks & gotchas

- **License contamination.** Bundling Android system images/GMS or unexamined redroid modules into a hosted product risks redistribution violations — keep provisioning user-supplied where required and examine licenses (§19).
- **Edition leakage.** Commercial-only code leaking into OSS builds undermines the model and confuses contributors — enforce the boundary in CI (OSS build excludes commercial).
- **Webhook reliability.** Unsigned or non-retried webhooks cause flaky/forgeable CI gates — sign + retry + log.
- **Overpromising at launch.** Marketing "replace all device farms" disappoints and burns trust — hold the narrow wedge (§2/§25), publish honest limitations.
- **Quota/billing drift.** Billing computed from a different source than enforcement causes disputes — meter and enforce off the **same** telemetry/quota mechanism (P13/P4).
- **SSO scope creep.** Enterprise SSO/SAML is deep; scaffold cleanly and stage it rather than blocking launch on full RBAC.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the GitHub Action + reusable workflow design, webhook hardening, the team/SSO/RBAC scaffolding, the billing/quota + retention model (consuming P13/P4), the managed-runner hooks, the OSS/commercial edition boundary + license/redistribution docs, and the launch-kit checklist. **No code until approved.** Stay in scope (§3) — go-to-market + commercial layer; no research, no core scheduler/runtime changes. If a commercial feature needs a new API field or scope, surface it for §7/§7c/§19/§20 in `ENHANCED_ARCHITECTURE.md`.

---

### Continuity note
**Previous:** `PART_13_OBSERVABILITY_HARDENING_WARMPOOLS.md` provides the telemetry/quotas billing and SLAs rely on; `PART_03` exposes the webhooks/API the Action drives; `PART_11` runs the managed fleet. **Next (optional):** `PART_15_BROWSER_NATIVE_WASM_RESEARCH.md` — explicitly kept off this revenue/QA path. **Closes the loop:** the launch kit publishes the §6b benchmarks (P12) and the honest limitations stance from §18/§25.
