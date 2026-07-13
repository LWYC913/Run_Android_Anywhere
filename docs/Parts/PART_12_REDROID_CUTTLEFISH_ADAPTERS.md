# PART 12 — redroid + Cuttlefish Adapters

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Add runtime breadth behind the existing adapter trait: the **redroid adapter** (no-KVM, privileged, kernel-module prerequisite, ARM-native, **trusted-only** isolation tagging) and the **Cuttlefish adapter** (KVM, high-fidelity, **first-class WebRTC**). Make the scheduler honor `host_arch` and `isolation_tier`, and ship a **compatibility/benchmark matrix**. This makes the §6b tradeoff real.

---

## 2. Where this sits

**Upstream (must already exist):**
- **P5** `RuntimeAdapter` trait + the Android Emulator adapter — the redroid and Cuttlefish adapters slot behind the **same trait** unchanged. Inlined trait surface: `provision`, `await_boot`, `install_apk` (idempotent), `run_smoke`, `capture`, `debug_endpoint`, `teardown`.
- **P4** scheduler/matcher — already keys on `runtime_kind/abi/host_arch/isolation_tier` and worker caps; this Part exercises the **non-default** kinds and arches.
- **P11** Kubernetes pools — the **redroid pool** (binder/ashmem) and **KVM pool** already exist; redroid is trusted-only there.
- **P9** debug path — Cuttlefish's first-class WebRTC plugs into the §11b gateway/Envoy/TURN path.

Inlined facts (§6b/§6c/§15/§15d):
- **redroid**: runs Android userspace on the **host kernel** via binder/ashmem, **no `/dev/kvm`**, `--privileged`, **ARM-native** (cheapest no-KVM device farm), `isolation_tier = shared_kernel_privileged`, **trusted-only** (escape = host compromise; never expose ADB).
- **Cuttlefish**: official AVD, **KVM** required, **first-class browser WebRTC**, high-fidelity device matrix, `isolation_tier = vm_isolated`.
- `runtime_profile` encodes `{android_api, device_profile, abi, host_arch}`; the **scheduler matches `host_arch`** (§15d).

**Downstream (depends on this):**
- **P13** benchmarks feed observability/SLOs and warm-pool sizing per profile.
- **P14** offers a broader device matrix as a commercial capability.

---

## 3. In scope / Out of scope

**In scope:**
- **redroid adapter** behind the P5 trait: provision a redroid container (privileged, host binder/ashmem prerequisite), boot/health, idempotent APK install via internal ADB, smoke/automation, capture, teardown. Tag profiles `runtime_kind = redroid`, `isolation_tier = shared_kernel_privileged`. ARM-native path emphasized.
- **Cuttlefish adapter** behind the P5 trait: provision a Cuttlefish instance (KVM), boot/health, install, smoke/automation, capture, **debug_endpoint via Cuttlefish's WebRTC** (mapped into the §11b path), teardown. Tag `runtime_kind = cuttlefish`, `isolation_tier = vm_isolated`.
- **Scheduler honoring `host_arch` + `isolation_tier`** end-to-end across all three runtimes (emulator/redroid/cuttlefish): a `vm_isolated` job never lands on redroid; an `arm64` job never lands on an `x86_64`-only worker.
- **Seed runtime profiles** for redroid (arm64 + x86) and Cuttlefish, added to P2's seed set.
- **Compatibility/benchmark matrix**: startup time, RAM/CPU, GPU mode, translation support (libndk/houdini for ARM-on-x86), per runtime/arch — published as a community doc.

**Out of scope (later Parts):**
- Waydroid/physical-device-lab adapters (future).
- Warm-pool sizing logic (P13) — the matrix informs it but isn't it.
- New K8s pools (P11 already provisioned them).
- Changing the trait (if you need to, that's a P5 contract change — surface it).

---

## 4. Deliverables

1. **redroid adapter** (`runtime_kind = redroid`) behind the P5 trait, with the host-module prerequisite documented and trusted-only enforcement.
2. **Cuttlefish adapter** (`runtime_kind = cuttlefish`) behind the trait, with WebRTC `debug_endpoint` mapped into the §11b gateway path.
3. **Scheduler coverage**: matcher tests across runtime_kind × host_arch × isolation_tier for all three runtimes.
4. **Seed profiles**: redroid arm64 + x86, Cuttlefish — appended to P2 seeds.
5. **Compatibility/benchmark matrix** doc (methodology + initial numbers on reference hardware).
6. **Tests**: a trusted job runs on redroid (no KVM) and on ARM; a high-fidelity job runs on Cuttlefish with a working debug session; isolation/arch exclusions enforced.

---

## 5. Key interfaces & contracts

- **Same `RuntimeAdapter` trait (P5)** — no new trait; new impls only. If a method doesn't fit, that's a P5 contract change to surface.
- **Profile tags:** redroid ⇒ `{runtime_kind: redroid, isolation_tier: shared_kernel_privileged, host_arch: arm64|x86_64}`; Cuttlefish ⇒ `{runtime_kind: cuttlefish, isolation_tier: vm_isolated}`.
- **Matcher contract (P4):** `min_isolation: vm_isolated` excludes redroid; `host_arch` must match worker arch; `runtime_kind` must be in worker `runtimes`.
- **Debug contract (P9/§11b):** Cuttlefish's WebRTC remote control mapped through the session gateway → (Envoy where needed) → runtime → TURN; redroid debug, if offered, follows the same gateway-gated rule (internal-only, never public ADB).
- **Benchmark schema:** `{ runtime_kind, host_arch, android_api, startup_ms, ram_mb, vcpu, gpu_mode, translation }`.

---

## 6. Architecture decisions to honor

- **Isolation-vs-deployability, not a maturity ladder (§6b)** — redroid is **first-class** for trusted/no-KVM/ARM; Cuttlefish for fidelity; emulator the untrusted default. The matrix encodes this.
- **redroid trusted-only (§6b/§15)** — shared kernel + privileged; never untrusted multi-tenant; never expose ADB; runs on its own pool (P11).
- **Cuttlefish needs KVM (§6c)** — schedules on the KVM pool; `vm_isolated`.
- **Scheduler matches `host_arch` (§15d)** — ARM-native redroid avoids translation; ARM-on-x86 uses libndk/houdini (slower, some apps fail) — the matrix documents this.
- **Reuse the §11b debug path (Cuttlefish WebRTC)** — don't build a parallel debug stack; map Cuttlefish into the gateway/TURN model.
- **No trait drift (§6/P5)** — adapters conform; contract changes go back to P5/`contracts`.

---

## 7. Acceptance criteria

1. A **trusted** job runs end-to-end on the **redroid** adapter with **no `/dev/kvm`**, including on an **ARM** worker (native, no translation).
2. A `vm_isolated` job is **never** scheduled to a redroid worker; an `arm64` job is **never** scheduled to an `x86_64`-only worker (matcher tests).
3. A high-fidelity job runs on the **Cuttlefish** adapter (KVM) and supports a **debug session** through the §11b path.
4. Seed profiles include redroid (arm64 + x86) and Cuttlefish; `GET /v1/runtime-profiles` lists them.
5. redroid runs **trusted-only** on its pool with **no public ADB**; untrusted load is rejected/never matched there.
6. The **compatibility/benchmark matrix** is published with startup/RAM/CPU/GPU/translation columns and initial numbers.
7. All three adapters pass the same smoke + finalizer + teardown checks behind the unchanged P5 trait.

---

## 8. Risks & gotchas

- **redroid host coupling.** binder/ashmem must be present on the node (P11 DaemonSet/image); if absent, redroid won't start — fail clearly and keep it pool-pinned.
- **Trusted-only enforcement is security-critical.** A scheduling bug that puts untrusted load on redroid is a host-compromise risk — make the `vm_isolated`-excludes-redroid rule airtight and tested (§15).
- **ARM-on-x86 translation pitfalls.** libndk/houdini is slower and some apps fail; the matrix must make this explicit so users pick arch correctly (§15d).
- **Cuttlefish heaviness.** Cuttlefish is heavier than the emulator (Appendix A); size pods accordingly and prefer bare-metal KVM (§6c).
- **Debug path duplication.** Don't fork a Cuttlefish-specific debug stack; reuse §11b — otherwise you maintain two signaling paths.
- **Benchmark honesty.** Publish methodology + hardware; misleading numbers erode the community trust the matrix is meant to build (launch Phase 3).

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the redroid and Cuttlefish adapter designs behind the P5 trait, their profile tagging, the scheduler coverage for `host_arch`/`isolation_tier`/`runtime_kind`, the seed-profile additions, the Cuttlefish→§11b debug mapping, and the benchmark-matrix methodology + test plan. **No code until approved.** Stay in scope (§3) — two adapters + matcher coverage + matrix; no new pools, no trait changes. If an adapter needs a trait method the P5 trait lacks, surface it for P5/`contracts`/§6b in `ENHANCED_ARCHITECTURE.md`.

---

### Continuity note
**Previous:** `PART_05_WORKER_AND_EMULATOR_ADAPTER.md` defines the trait these adapters implement; `PART_11` provisioned the redroid + KVM pools they run on; `PART_09` is the debug path Cuttlefish reuses. **Next:** `PART_13_OBSERVABILITY_HARDENING_WARMPOOLS.md` consumes this Part's benchmarks for SLOs and warm-pool sizing. **Also soon:** `PART_14` markets the broader device matrix as a commercial capability.
