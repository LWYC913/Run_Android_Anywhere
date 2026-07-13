# PART 04 — Queue, Scheduler & Reconciler

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Build the reliability backbone between API and workers: **NATS JetStream** streams/subjects, a **pull-consumer dispatch with ack-deadline leases**, **capability matching** (runtime/abi/host_arch/isolation_tier ↔ worker caps), `max_deliver` + **DLQ**, the **control-plane reconciler** (zombie jobs, leaked pods, expired debug sessions), and per-project quotas. This is §8b made real.

---

## 2. Where this sits

**Upstream (must already exist):**
- **P1** `contracts` — `JobState` + transition guard, the worker-protocol types, and the `JobQueued` producer message (authored alongside P3). Inlined:
  - `JobQueued { job_id, project_id, runtime_profile(runtime_kind, abi, host_arch, isolation_tier), min_isolation, timeout_seconds }` on subject `jobs.queued`.
  - `WorkerRegistration { worker_id, runtimes[], kvm, gpu, arch, capacity }`.
  - `WorkerHeartbeat { worker_id, active_jobs, capacity, runtimes[], kvm, gpu, arch, lease_extends[], last_seen }`.
  - `JobClaim`, `JobLeaseExtension`, `JobResult`.
- **P2** `repository` — reconciler/matcher queries, inlined:
  - `transition_job_state(job_id, from, to)` (conditional UPDATE guard).
  - `find_stale_running_jobs(heartbeat_older_than)`, `requeue_or_fail_job`.
  - `find_workers_matching(runtime_kind, abi, host_arch, isolation_tier, has_capacity)`.
  - `find_expired_debug_sessions`, `end_debug_session`.
  - `record_heartbeat(WorkerHeartbeat)`, `list_workers`.
- **P3** publishes `JobQueued` to `jobs.queued` (the producer). This Part owns the **consumer** side.

**Downstream (depends on this):**
- **P5** workers claim from the consumer defined here, extend leases via heartbeat, and ack/term on completion.
- **P11** scales worker pools that this scheduler matches; the reconciler reaps leaked **pods** there.

---

## 3. In scope / Out of scope

**In scope:**
- JetStream stream + subject design: `jobs.queued` (work), lease/ack config, a **DLQ subject** (`jobs.dead`) for `max_deliver`-exhausted messages, and an events/heartbeat path if not already on the API.
- **Pull consumer** with an **ack deadline (visibility timeout)**; redelivery on missed ack.
- **Capability matcher:** given a `JobQueued`, select eligible workers via `find_workers_matching`; honor `min_isolation: vm_isolated` (exclude redroid/`shared_kernel_privileged`), `host_arch`, `runtime_kind`, `abi`, and spare capacity. Apply **backpressure** when no pool has capacity.
- **Reconciler loop** (periodic background task): stale `claimed`/`running` jobs whose lease expired → `requeue_or_fail_job` (requeue if under `max_deliver`, else `infra_failed`); leaked runtime pods/containers with no owning job → reap (in MVP/Compose this is container cleanup; in K8s P11 wires pod deletion); `debug_sessions` past `expires_at` → `end_debug_session` + audit.
- **Quotas/fairness:** per-project concurrency caps + simple priority lanes so one tenant can't starve the grid.
- Metrics (§15e): queue depth, claim latency, redelivery count, DLQ count, reconciler reaps.

**Out of scope (later Parts):**
- The worker run loop and runtime adapters (P5).
- K8s pod/Job creation and the actual pod-reaping mechanics (P11) — define the reconciler's *intent* here behind an abstraction; the Compose path reaps local containers.
- Warm-pool sizing logic (P13) — the matcher just sees worker capacity.

**MVP note (from §8b):** the scheduler/matcher **may be folded into the API process**; the reconciler still runs as a single background task. Keep the matcher behind a clean interface so it can be split into its own `scheduler` crate/service later (P11/§17).

---

## 4. Deliverables

1. **JetStream setup** (stream + consumers + DLQ) as code/config, idempotently created on boot.
2. **Dispatch/claim path:** consumer config (ack wait, `max_deliver`), claim semantics, lease extension via worker heartbeat, ack on success, term→DLQ on exhaustion.
3. **`crates/scheduler`** (or a module in `api` for MVP) housing the **capability matcher**.
4. **Reconciler** background task (stale jobs, leaked runtimes via an injectable reaper, expired debug sessions).
5. **Quota enforcement** (per-project concurrency + priority lanes), surfaced as `quota_exceeded` (P1 taxonomy) on the API when exceeded.
6. **Tests:** lease-expiry redelivery, DLQ on poison job, matcher correctness incl. `min_isolation`/`host_arch` exclusion, reconciler reaps a stale job, expired debug-session revocation.

---

## 5. Key interfaces & contracts

- **Subjects:** `jobs.queued` (work), `jobs.dead` (DLQ). Heartbeats/leases per the worker protocol.
- **Lease model:** ack deadline = visibility timeout; worker `JobLeaseExtension` (heartbeat) extends it; missed extension → redelivery.
- **Matcher input/output:** `JobQueued` → an eligible `worker_id` (or backpressure/no-match → leave queued). Uses `find_workers_matching(runtime_kind, abi, host_arch, isolation_tier, has_capacity)`.
- **Reconciler reaper interface:** `trait RuntimeReaper { fn reap_leaked(...) }` — Compose impl kills local containers; K8s impl (P11) deletes pods.
- **State transitions:** only via `transition_job_state` (P2 guard); reconciler uses `requeue_or_fail_job`.

---

## 6. Architecture decisions to honor

- **Pull consumer + ack-deadline lease (§8b)** — not fire-and-forget; a dead worker's job must redeliver.
- **`max_deliver` + DLQ (§8b)** — poison jobs land in `jobs.dead`, never loop forever.
- **Capability matching respects isolation (§6b/§8b)** — `min_isolation: vm_isolated` **excludes** redroid (`shared_kernel_privileged`) workers; `host_arch` mismatch is rejected (§15d).
- **Reconciler is mandatory even in MVP (§8b)** — zombie jobs, leaked runtimes, and expired debug sessions are reaped continuously.
- **Idempotency end-to-end (§8b)** — redelivery must be safe because all worker side-effects (P5) are idempotent; the scheduler relies on that, the worker guarantees it.
- **Reaper is abstracted (§10)** — same reconciler logic, different reaper impl for Compose vs K8s.

---

## 7. Acceptance criteria

1. A worker that claims a job and then stops heartbeating causes the job to be **redelivered** to another eligible worker after the ack deadline (test).
2. A job that fails `max_deliver` times lands on `jobs.dead` and is marked appropriately (no infinite loop).
3. The matcher returns only workers satisfying `runtime_kind/abi/host_arch/isolation_tier` with spare capacity; a `vm_isolated` job is **never** matched to a redroid worker; a `host_arch` mismatch is rejected.
4. The reconciler transitions a stale `running` job (expired lease) to requeue-or-`infra_failed` per `max_deliver`.
5. The reconciler revokes + audits a `debug_session` past `expires_at`.
6. Exceeding a project's concurrency quota yields `quota_exceeded` (no new dispatch) without affecting other projects.
7. Metrics expose queue depth, redelivery count, DLQ count, and reconciler reaps.

---

## 8. Risks & gotchas

- **Lease vs. heartbeat skew.** The ack deadline must comfortably exceed the heartbeat interval, or healthy jobs get redelivered. Make both configurable and document the relationship.
- **Double-dispatch.** Matching + claiming must be atomic enough that two workers don't both run one job — lean on the P2 conditional-UPDATE guard as the final arbiter even if the queue briefly double-delivers.
- **Reaper safety.** Reaping a "leaked" pod that actually belongs to a slow-starting job is destructive — key reaping on owning-job state + heartbeat, not time alone, and add grace.
- **Backpressure vs. starvation.** When pools are full, leaving jobs queued is correct; combine with quotas/priority so large tenants don't monopolize freed capacity.
- **MVP fold-in creep.** If the matcher lives in the API for MVP, keep it behind an interface so P11 can extract it without rewrites.
- **DLQ visibility.** A job in `jobs.dead` must be observable (metric + an admin path later); a silent DLQ hides real failures.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the JetStream stream/consumer/DLQ topology, the lease/ack timing model, the matcher algorithm and its query usage, the reconciler loop (with the injectable reaper), the quota model, and the test matrix. **No code until approved.** Stay in scope (§3) — no worker run loop, no K8s pod mechanics. If you need a new subject, a new worker-protocol field, or a denormalized column for cheaper reconciler queries, surface it for `contracts`/§7b/§8b in `ENHANCED_ARCHITECTURE.md`.

---

### Continuity note
**Previous:** `PART_03_CONTROL_PLANE_API.md` publishes the `jobs.queued` messages this Part consumes. **Next:** `PART_05_WORKER_AND_EMULATOR_ADAPTER.md` builds the worker that claims from this consumer, heartbeats leases, and acks/terms — the other half of this protocol. **Also soon:** `PART_11` supplies the K8s reaper implementation behind the reconciler's reaper interface and scales the worker pools this scheduler matches.
