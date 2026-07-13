# PART 06 — Local Docker Compose Stack (one-command demo)

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Assemble everything built so far into a **single-host Docker Compose stack with profiles**, and ship the **one-command demo** (`run-anywhere local app.apk`) that uploads an APK, runs a headless smoke test, and shows artifacts. This is the OSS quickstart that earns first trust (§9, §16, launch Phase 1).

---

## 2. Where this sits

**Upstream (must already exist):**
- **P3** API (Axum) — the service the CLI and dashboard call; needs Postgres, NATS, MinIO, JWT signing key, S3 bucket via config.
- **P4** queue/scheduler/reconciler — runs (folded into API for MVP is allowed, §8b); the reconciler must be live.
- **P5** worker + Android Emulator adapter — the thing that actually runs the job; advertises `kvm` capability.
- **P2** migrations + seeded `runtime_profiles`; **P1** contracts/OpenAPI.

Inlined facts you rely on:
- Job-create body (§7) and the `headless_ci` mode.
- The worker needs `/dev/kvm` for the `emulator` profile; the `redroid` profile needs **binder/ashmem host modules** (§9) — redroid adapter itself lands in P12, but the **profile/lane** is defined here.
- Workers pull/push via **pre-signed URLs** against MinIO (§15b).

**Downstream (depends on this):**
- **P7** dashboard is added as the `web` service and consumes the same API.
- **P9** adds the `debug` profile (Envoy + TURN + token service) on top of this Compose base.
- **P10** adds the `appium` profile.
- **P14** GitHub Action exercises this same stack/CLI shape in CI.

---

## 3. In scope / Out of scope

**In scope:**
- A **Compose file with profiles** (per §9):
  - `basic`: `api`, `web` (placeholder until P7), `postgres`, `nats`, `minio`.
  - `emulator`: one **KVM** Android Emulator worker (P5 adapter).
  - `redroid`: the **no-KVM** lane scaffold (worker configured for redroid; adapter arrives in P12; document the host-module prerequisite).
  - `debug`: placeholder hooks for Envoy + TURN + token service (wired in P9).
  - `appium`: placeholder (wired in P10).
- **One-command demo:** a `run-anywhere` CLI (thin client over the P3 API) implementing at least `local <app.apk>` = create project (or default) → pre-signed upload → create `headless_ci` job → stream events (SSE) → print artifact links.
- **Bootstrapping:** migrations run on `api` start; MinIO bucket auto-created; a default project + API key minted for local use; seeded runtime profiles available.
- **Docs:** a README quickstart (prereqs incl. Linux+KVM for `emulator`; what works and what doesn't, §9), and the exact `docker compose --profile emulator up` + `run-anywhere local app.apk` flow.

**Out of scope (later Parts):**
- The dashboard UI itself (P7) — `web` is a placeholder container here.
- Real debug/Appium services (P9/P10) — only profile placeholders + ports reserved.
- redroid **adapter** implementation (P12) — only the Compose lane + prerequisite docs.
- Kubernetes (P11). Compose is explicitly **not** the production scaling story (§9).

---

## 4. Deliverables

1. **`deploy/compose/docker-compose.yml`** with the five profiles and healthchecks/dependencies wired (`api` waits for `postgres`/`nats`/`minio`).
2. **`run-anywhere` CLI** (small Rust or TS binary) with `local`, plus `upload`, `jobs`, `logs/events`, `artifacts` subcommands over the P3 API.
3. **Bootstrap**: migration step, MinIO bucket init, default project/API-key generation, env/`.env.example`.
4. **Emulator worker service** (KVM device passthrough: `/dev/kvm`) using the P5 image.
5. **redroid lane scaffold** + a documented host-module prerequisite note.
6. **README quickstart** + a short "what doesn't work yet / host requirements" section (honest, per launch Phase 1).
7. **A smoke script/CI job** that runs the whole demo against a sample APK and asserts artifacts appear.

---

## 5. Key interfaces & contracts

- **CLI → API:** uses the §7 endpoints exactly (uploads/apk, jobs, jobs/{id}/events, jobs/{id}/artifacts); bearer API key from local bootstrap.
- **Profiles (§9):** `basic | emulator | redroid | debug | appium` — additive; `debug`/`appium` reserve ports/config for P9/P10.
- **Worker capabilities:** the `emulator` worker advertises `kvm: true`; the matcher (P4) routes the demo job to it.
- **Artifact flow:** worker uploads to MinIO via pre-signed URLs; CLI prints pre-signed download links from `GET /v1/jobs/{id}/artifacts`.

---

## 6. Architecture decisions to honor

- **Compose is for dev/demo/single-host only (§9)** — do not smuggle multi-tenant scaling concerns in; that's K8s (P11).
- **Profiles, not forks (§9)** — one Compose file, opt-in services; `debug`/`appium`/`redroid` are lanes, not separate stacks.
- **Honest quickstart (launch Phase 1, §18)** — document KVM/host-module requirements and current limitations; under-promise.
- **Pre-signed URLs even locally (§15b)** — keep the worker credential-free against MinIO, matching prod.
- **One-command promise (§16/§25):** `run-anywhere local app.apk` is the headline; keep it genuinely one command after `compose up`.
- **redroid is first-class but trusted-only (§6b)** — the lane exists from the start; the adapter and trust caveats come in P12/§15.

---

## 7. Acceptance criteria

1. `docker compose --profile basic up` brings up api/web/postgres/nats/minio healthy; migrations applied; bucket + default project/API key created.
2. `docker compose --profile emulator up` adds a KVM emulator worker that registers with `kvm: true`.
3. `run-anywhere local app.apk` (on a KVM host) uploads, creates a `headless_ci` job, streams events to completion, and prints working artifact links (screenshot + logcat).
4. The reconciler is running (a deliberately killed worker mid-job results in redelivery or `infra_failed`, per P4).
5. The `redroid`, `debug`, and `appium` profiles start their placeholders without breaking `basic`/`emulator`.
6. The README lets a new user reproduce the demo from scratch, including stated host requirements and known gaps.
7. The CI smoke job runs the end-to-end demo and asserts artifacts exist.

---

## 8. Risks & gotchas

- **KVM on dev machines.** macOS/Windows hosts lack `/dev/kvm`; document the remote-Linux-worker path (§9) so the demo doesn't silently fall back to unusably slow software mode.
- **First-run friction.** Bucket/project/key bootstrap must be automatic; if a user has to hand-create MinIO buckets or keys, the "one command" promise breaks.
- **Profile leakage.** Placeholders for `debug`/`appium` must not open public ports or appear "done"; keep them clearly inert until P9/P10.
- **Healthcheck ordering.** `api` racing an unmigrated DB or an unready NATS/MinIO causes flaky first boots — gate startup on healthchecks.
- **Sample APK licensing.** Ship/point to a freely redistributable sample APK; don't bundle anything with redistribution constraints (§19 caveat).
- **Don't overscope the CLI.** It's a thin API client; business logic stays in the API. Resist adding orchestration to the CLI.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the Compose service/profile topology (with KVM passthrough and healthchecks), the CLI command set and its API calls, the bootstrap sequence, the README outline, and the CI smoke job. **No code until approved.** Stay in scope (§3) — placeholders only for debug/appium/redroid-adapter. If the demo reveals a missing API affordance (e.g. a "default project" convenience), surface it for §7/`openapi/v1.yaml` rather than working around it in the CLI.

---

### Continuity note
**Previous:** `PART_05_WORKER_AND_EMULATOR_ADAPTER.md` is the worker this stack runs; `PART_03` is the API the CLI drives. **Next:** `PART_07_DASHBOARD.md` replaces the `web` placeholder with the real Next.js UI on this same stack. **Also soon:** `PART_09` activates the `debug` profile (Envoy/TURN/token service); `PART_10` activates the `appium` profile; `PART_11` takes this to Kubernetes for scale.
