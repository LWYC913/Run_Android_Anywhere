# PART 11 — Kubernetes Deploy

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Take the stack to scale on Kubernetes: **Helm charts** for all services, the **dedicated KVM node pool** (labels/taints/tolerations/affinity, privileged emulator pods, bare-metal preference), the **no-KVM redroid pool** (binder/ashmem DaemonSet), **per-job pod/Job creation**, strict **NetworkPolicy** (no public ADB, deny egress, block metadata), **external secrets**, and autoscaling via **pre-provisioned pools / Karpenter (not generic node auto-provisioning)**. This operationalizes §6c, §10, and the §15 isolation resolution.

---

## 2. Where this sits

**Upstream (must already exist):**
- **P3/P4/P5** images (api, scheduler-or-folded, worker + emulator adapter) and **P7** dashboard image; **P9** debug components (gateway, Envoy, coturn, token service); **P10** worker image with Appium drivers.
- **P4** reconciler with its **injectable reaper interface** — this Part supplies the **K8s reaper** (delete leaked pods).
- **ENHANCED_ARCHITECTURE.md §6c/§10/§15** — the substrate/scheduling/security constraints, inlined below.

Inlined constraints you must honor:
- Managed-K8s standard nodes generally **don't expose `/dev/kvm`**; enabling nested virt restricts machine series, **breaks node auto-provisioning** (GKE), and **requires `securityContext.privileged: true`** (§6c).
- **GKE:** nested virt = Standard-only, limited machine series, incompatible with node auto-provisioning, requires privileged pods. **AKS:** Gen2 VMs + **Kata Pod Sandboxing** available. **EKS:** practically **bare-metal** (`*.metal`).
- **redroid** needs **binder/ashmem modules** on the host (privileged DaemonSet or pre-baked image), is **trusted-only**, ARM-friendly, no KVM (§6b/§6c).
- Runtime pods: `/dev/kvm` where needed; **NetworkPolicy** deny-egress + block metadata + no public ADB; per-runtime requests/limits; job timeout + forced cleanup; **artifact upload before deletion (finalizer)** (§10/§14).

**Downstream (depends on this):**
- **P13** layers observability/hardening (seccomp/AppArmor, gVisor/Kata for the control plane, rate limits) and warm pools on this deployment.
- **P14** runs the hosted/commercial offering on this base.

---

## 3. In scope / Out of scope

**In scope:**
- **Helm charts** for: `api`, `web`, `scheduler` (or folded-into-api value), `worker` (per runtime type), `postgres` (external managed or StatefulSet for dev), `nats`, `minio`/external S3, plus debug-path (`session-gateway`, `envoy`, `coturn`, `token-service`).
- **KVM node pool**: label (`run-anywhere.io/kvm=true`) + **taint** (`...:NoSchedule`) so only emulator job pods land there; node affinity to pin; **privileged** emulator pods with `/dev/kvm`; **bare-metal preference** documented; treat nodes as **ephemeral**.
- **No-KVM redroid pool**: binder/ashmem via **privileged DaemonSet** or pre-baked node image; tag **trusted-only**; separate taint/labels.
- **Per-job pod/Job controller**: production isolation model = **one pod/Job per emulator session** (vs MVP long-running worker creating local containers); enforce timeout + forced cleanup + the **finalizer before deletion**.
- **NetworkPolicy**: deny-by-default egress; **block cloud metadata IPs**; allow only artifact store + required internal services; **no public ADB / no public emulator gRPC** (debug only via the §11b gateway path).
- **External secrets**: Vault or cloud secret manager via the **External Secrets Operator**; workers stay credential-free (pre-signed URLs); JWT signing key only in the token service (§15b).
- **Autoscaling**: **pre-provisioned KVM pools + cluster-autoscaler**, or **Karpenter pinned to specific instance types** — **not** generic node auto-provisioning (incompatible with GKE nested-virt pools). Scale-to-zero acknowledged (minutes of cold node bring-up).
- **K8s reaper** plugged into the P4 reconciler (delete leaked pods with grace).

**Out of scope (later Parts):**
- seccomp/AppArmor profiles, gVisor/Kata for the control plane, rate limits, warm-pool sizing (P13) — leave hooks/values.
- Billing/team/SSO (P14).
- New runtime adapters (P12) — but the redroid **pool** is provisioned here so P12's adapter has a home.

---

## 4. Deliverables

1. **Helm charts** (umbrella + per-service) with values for managed-vs-bare-metal, folded-vs-split scheduler, external-vs-in-cluster Postgres/S3.
2. **KVM pool manifests**: labels/taints/tolerations/affinity, privileged emulator pod spec with `/dev/kvm`, ephemeral-node guidance.
3. **redroid pool**: binder/ashmem DaemonSet (or node-image note), trusted-only tagging, separate scheduling.
4. **Per-job Job/Pod controller** (or worker-driven pod creation) honoring timeout/cleanup/finalizer.
5. **NetworkPolicies**: deny egress, block metadata, restrict ADB/gRPC, allow artifact store + internal deps.
6. **External Secrets** integration + a documented secret inventory (DB, S3, registry, JWT signing, TURN).
7. **Autoscaling config**: pre-provisioned pool + cluster-autoscaler and/or Karpenter pinned to instance types; explicit "no generic node auto-provisioning for KVM pools" note.
8. **K8s reaper** implementation behind the P4 reconciler interface.
9. **A reference deploy guide** (per-cloud caveats: GKE/AKS/EKS) + a smoke deploy that runs a headless job on the KVM pool and a trusted job on the redroid pool.

---

## 5. Key interfaces & contracts

- **Scheduling labels/taints:** `run-anywhere.io/kvm=true:NoSchedule` (emulator), a distinct redroid taint/label; matcher (P4) already keys on worker caps `{kvm, arch, runtimes}` — pool placement must reflect those.
- **Per-job pod contract:** carries the job's runtime profile (`runtime_kind/abi/host_arch/isolation_tier`), `/dev/kvm` if `vm_isolated`, NetworkPolicy attached, finalizer-before-delete.
- **Reaper interface (from P4):** K8s impl deletes leaked pods (no owning-job state) with grace.
- **Secrets:** ExternalSecret resources → mounted secrets; workers receive **no** S3 creds (pre-signed URLs).
- **Debug path services:** gateway/Envoy/coturn/token-service exposed only as designed in §11b (gateway is the only external door).

---

## 6. Architecture decisions to honor

- **Dedicated, single-purpose, ephemeral KVM pool (§6c/§10/§15)** — privileged is acceptable **only** there; nothing else schedules on it; recycle nodes aggressively.
- **Control plane never privileged (§6c/§15)** — api/scheduler/web/reconciler run unprivileged (gVisor/Kata hardening comes in P13).
- **Prefer bare-metal for KVM (§6c, Appendix A)** — avoid the nested-virt tax; document per-cloud reality (constraints move — verify at deploy time).
- **No generic node auto-provisioning for nested-virt pools (§6c/§10)** — pre-provisioned pools + cluster-autoscaler or Karpenter pinned to instance types.
- **redroid trusted-only on its own pool (§6b/§15)** — shared kernel + privileged ⇒ escape = host compromise; never untrusted multi-tenant; no public ADB.
- **NetworkPolicy is a product feature (§15)** — deny egress, block metadata, internal-only ADB/gRPC by default.
- **Finalizer before pod deletion (§10/§14)** — a pod is not deleted until artifacts are confirmed uploaded.

---

## 7. Acceptance criteria

1. `helm install` brings up the control plane (unprivileged) + a KVM worker pool; a headless job runs on a **tainted KVM node** and **nowhere else** (verify scheduling).
2. A privileged emulator pod gets `/dev/kvm`; the control-plane pods are **not** privileged.
3. A trusted job runs on the **redroid pool** with binder/ashmem available; untrusted load is **not** scheduled there.
4. **NetworkPolicy** blocks egress + cloud metadata + public ADB/gRPC; the debug path works **only** through the gateway (P9).
5. **External Secrets** supplies DB/S3/registry/JWT/TURN secrets; workers hold **no** S3 creds (pre-signed URLs only).
6. The **finalizer** prevents pod deletion before artifact upload; the **K8s reaper** removes a deliberately leaked pod via the reconciler.
7. Autoscaling uses pre-provisioned pools/Karpenter (pinned), **not** generic node auto-provisioning; scaling a queued backlog adds KVM capacity within the pool.
8. The deploy guide reproduces this on at least one cloud, with GKE/AKS/EKS caveats documented.

---

## 8. Risks & gotchas

- **`/dev/kvm` availability is the whole ballgame (§6c).** If the pool lacks KVM, emulator pods silently degrade to software mode or fail; verify KVM on the node and gate scheduling on it.
- **Privileged blast radius.** Privileged pods on shared nodes are dangerous — enforce the taint + single-job-per-pod + ephemeral nodes; never let general workloads onto the KVM pool.
- **Auto-provisioning incompatibility (GKE).** Generic node auto-provisioning won't create valid nested-virt nodes — use pinned Karpenter/pre-provisioned pools or scaling breaks confusingly.
- **redroid host compromise.** A redroid escape compromises the host kernel — strict trusted-only + no public ADB + its own pool (§15); don't co-locate untrusted load.
- **Metadata endpoint exfiltration.** Without blocking metadata IPs, untrusted APKs can reach cloud credentials — NetworkPolicy must block them.
- **Reaper over-deletion.** Deleting a slow-starting pod as "leaked" kills real jobs — key on owning-job state + grace, mirroring P4.
- **Moving cloud constraints.** The per-cloud nested-virt rules change; the guide must say "verify at deploy time" and cite the current docs.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the Helm chart structure + values, the KVM/redroid pool definitions (labels/taints/affinity/privileged/`/dev/kvm`), the per-job pod/Job controller, the NetworkPolicy set, the External Secrets wiring, the autoscaling strategy, the K8s reaper, and the per-cloud deploy guide outline. **No code/manifests until approved.** Stay in scope (§3) — leave seccomp/AppArmor/gVisor/warm-pools to P13. If scheduling needs a new label/field on the runtime profile or worker, surface it for §6c/§10 in `ENHANCED_ARCHITECTURE.md`.

---

### Continuity note
**Previous:** `PART_05`/`PART_09`/`PART_10` produce the worker, debug components, and Appium-enabled images this deploys; `PART_04` defines the reconciler this Part's reaper plugs into. **Next:** `PART_12_REDROID_CUTTLEFISH_ADAPTERS.md` adds the redroid + Cuttlefish adapters that run on the pools provisioned here. **Also soon:** `PART_13` hardens this deployment (seccomp/AppArmor, gVisor/Kata control plane, warm pools) and `PART_14` runs the hosted product on it.
