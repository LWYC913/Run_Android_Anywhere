# PART 13 — Observability, Hardening & Warm Pools

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Cross-cutting production readiness: **OpenTelemetry tracing** API→queue→worker→runtime, **Prometheus/Grafana** metrics + dashboards + SLOs, structured logs; **security hardening** (seccomp/AppArmor, gVisor/Kata for the control plane, egress/metadata enforcement, rate limits/quotas); and **snapshots + warm pools** for boot-time and cost. This completes §15c and §15e and tightens §15.

---

## 2. Where this sits

**Upstream (must already exist):**
- **P3/P4/P5** already emit **basic** structured logs + OTel spans + a few metrics (threaded from MVP per §15e). This Part makes them **complete and production-grade**, not net-new instrumentation from zero.
- **P11** Kubernetes deployment with the KVM/redroid pools, NetworkPolicy, and External Secrets — the substrate hardening attaches to.
- **P12** benchmarks (startup/RAM/CPU per runtime/arch) — feed **warm-pool sizing** and **boot-time SLOs**.
- **P9** debug path — **TURN bandwidth** + debug-session metrics live here.

Inlined targets (§15c/§15e/§15):
- Metrics: queue depth, claim latency, boot time, job duration, pass/fail rate, worker capacity/utilization, TURN bandwidth, debug-session count.
- SLO candidates: job-accept latency; p95 cold-boot per profile; job success rate **excluding `infra_failed`**; debug-session connect success.
- Hardening: seccomp/AppArmor + dropped caps on emulator pods; **control plane under gVisor/Kata**; deny-egress + block metadata; rate limits/quotas.
- Warm pools: N pre-booted runtimes per popular profile; hand one to an incoming job; **recycle (trusted) or destroy (untrusted, default)**.

**Downstream (depends on this):**
- **P14** sells SLAs/retention/observability as commercial value and relies on this telemetry for billing/quotas.

---

## 3. In scope / Out of scope

**In scope:**
- **Tracing**: complete OTel propagation across API → JetStream → worker → runtime (and into the debug path), with `job_id`/`worker_id` correlation everywhere.
- **Metrics**: the full §15e set exported to Prometheus; **Grafana dashboards** (queue, boot-time, utilization, debug/TURN) + **alerts** (queue backlog, reconciler reaps, boot-time regressions).
- **SLOs**: define + measure the candidate SLOs; wire alerting/error-budget views.
- **Logging**: structured JSON everywhere, consistent fields, redaction of secrets/tokens.
- **Hardening**:
  - **seccomp/AppArmor** profiles + capability dropping on emulator pods (beyond what `/dev/kvm` needs).
  - **gVisor or Kata** for the **control plane** (api/scheduler/web/reconciler) — never privileged.
  - Enforce **deny-egress + block cloud metadata** at runtime (validating P11's NetworkPolicy) and **flag jobs** attempting network scans/privilege abuse (§15).
  - **Rate limits + quotas** on the API + scheduler (per-project), surfaced as `quota_exceeded`.
- **Snapshots + warm pools** (§15c):
  - Snapshot/quick-boot images for the SDK emulator to restore fast.
  - Warm-pool manager: keep N pre-booted runtimes per popular `runtime_profile`, hand to incoming jobs, then **recycle (trusted) or destroy (untrusted)**; sized to demand/quota.

**Out of scope (later Parts):**
- Billing/SSO/retention tiers (P14) — this Part provides the telemetry they consume.
- New runtimes/adapters (P12 done).
- Browser-native research (P15).

---

## 4. Deliverables

1. **Completed OTel tracing** across all hops + a documented span/attribute convention.
2. **Prometheus metric set** (§15e) + **Grafana dashboards** + **alert rules**.
3. **SLO definitions** + error-budget/alerting wiring.
4. **Hardening**: seccomp/AppArmor profiles, capability drops, gVisor/Kata control-plane config, egress/metadata enforcement validation, abuse-flagging hooks.
5. **Rate-limit/quota** middleware (API + scheduler), per-project.
6. **Snapshot/quick-boot** support in the emulator adapter (and where applicable) + a **warm-pool manager** with recycle/destroy policy and demand-based sizing.
7. **Tests/validation**: trace continuity across a full job; dashboards populate; an SLO breach alerts; egress to metadata is blocked; warm pool reduces cold-boot latency (measured) and destroys untrusted runtimes after use.

---

## 5. Key interfaces & contracts

- **Trace context** propagated through the `JobQueued` message and worker protocol (add context fields if missing — surface to `contracts`).
- **Metric names/labels** standardized (`runtime_kind`, `host_arch`, `profile`, `project`) so dashboards/SLOs slice consistently.
- **Warm-pool manager interface**: `acquire(profile) -> RuntimeHandle | None` (falls back to cold boot), `release(handle, trusted: bool)` → recycle or destroy; integrates with the scheduler/worker without changing the matcher contract.
- **Quota contract (P4):** per-project caps enforced → `quota_exceeded` (P1 taxonomy).
- **Snapshot artifacts**: quick-boot images referenced by `runtime_profile.image_ref`/snapshot metadata.

---

## 6. Architecture decisions to honor

- **Observability is threaded from MVP, completed here (§15e)** — not bolted on; extend existing spans/metrics, keep `job_id`/`worker_id` correlation.
- **Control plane never privileged; gVisor/Kata for defense-in-depth (§6c/§15)** — only the dedicated KVM pool runs privileged (P11).
- **Egress deny + metadata block enforced and validated (§15)** — untrusted APKs cannot reach cloud credentials; flag scanning/privilege abuse.
- **Warm pools recycle trusted, destroy untrusted (§15c/§15)** — never reuse runtime data across untrusted jobs unless a **trusted** snapshot pool.
- **SLOs exclude `infra_failed` from success rate (§15e)** — separate test outcomes from infrastructure failures.
- **Snapshots + warm pools are latency *and* cost levers (§15c, Appendix A)** — size to demand/quota; MVP cold-boot remains valid, production runs warm.

---

## 7. Acceptance criteria

1. A single job produces a **continuous trace** spanning API → queue → worker → runtime (and debug path when used), correlated by `job_id`.
2. Grafana dashboards populate the §15e metrics; an induced **SLO breach** (e.g. boot-time regression) fires an alert.
3. Emulator pods run with **seccomp/AppArmor** + dropped capabilities; the **control plane** runs under **gVisor/Kata** and is non-privileged.
4. **Egress to cloud metadata is blocked** at runtime; a job attempting a network scan is **flagged**.
5. **Per-project rate limits/quotas** return `quota_exceeded` without affecting other projects.
6. A **warm pool** measurably reduces cold-boot latency for a popular profile; **untrusted** runtimes are **destroyed** after use; **trusted** ones recycled.
7. Snapshot/quick-boot restores an emulator faster than cold boot (measured).

---

## 8. Risks & gotchas

- **Trace gaps at the queue hop.** Context often drops crossing JetStream — explicitly propagate trace context in the message, or traces fragment.
- **seccomp/AppArmor over-restriction.** Too-tight profiles break the emulator/KVM; derive profiles empirically and test the emulator still runs.
- **gVisor + KVM don't mix.** Don't put the **emulator** under gVisor (it needs KVM); gVisor/Kata is for the **control plane** only (§6c) — keep the boundary clear.
- **Warm-pool cost.** Idle warm runtimes cost money; size to real demand/quota and destroy untrusted ones promptly (latency vs idle-cost lever, §15c).
- **Snapshot trust.** Reusing snapshots across **untrusted** jobs leaks state — snapshots/warm reuse are for **trusted** pools (§15).
- **Metric cardinality.** Per-job labels explode cardinality; label by profile/project/runtime, not by `job_id` (use traces/logs for per-job).
- **Quota fairness.** Global rate limits can starve small tenants; scope per-project with priority lanes (P4).

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the OTel span/attribute conventions + queue-hop propagation, the Prometheus metric/dashboard/alert/SLO set, the hardening plan (seccomp/AppArmor/cap-drop, gVisor/Kata control plane, egress/metadata validation, abuse flagging), the rate-limit/quota middleware, and the snapshot + warm-pool manager (interface, recycle/destroy policy, sizing) with a test/validation matrix. **No code until approved.** Stay in scope (§3) — telemetry/hardening/warm-pools; no billing/SSO. If trace propagation needs a new field on the queue message or worker protocol, surface it for §8/§8b/§15e in `ENHANCED_ARCHITECTURE.md`.

---

### Continuity note
**Previous:** `PART_11_KUBERNETES_DEPLOY.md` is the substrate this hardens; `PART_12` supplies the benchmarks that size warm pools and boot-time SLOs; `PART_05`/`PART_09` emit the base telemetry completed here. **Next:** `PART_14_COMMERCIAL_CI_LAUNCH.md` sells SLAs/retention on this telemetry and consumes its quotas/metrics for billing. **Also relevant:** `PART_15` (optional research) stays off this production/SLO path entirely.
