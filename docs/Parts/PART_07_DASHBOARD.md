# PART 07 — Next.js Dashboard

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Build the **Next.js (App Router) dashboard** that consumes the P1 OpenAPI: project view, job submit, job list/detail, **live event stream (SSE)**, artifact viewer, runtime-profile picker, and optional OIDC login. This is the UI half of the MVP (§16) and replaces the `web` placeholder from P6.

---

## 2. Where this sits

**Upstream (must already exist):**
- **P1** `openapi/v1.yaml` — generate the typed API client from this; do not hand-roll request types.
- **P3** API — the live endpoints (§7), the **SSE event stream**, pre-signed artifact URLs, debug-session creation, API-key auth, optional OIDC (§7c).
- **P6** Compose stack — the dashboard runs as the `web` service against the local API.

Inlined contracts you rely on:
- Endpoints (§7): projects, uploads/apk, jobs (create w/ `Idempotency-Key`), jobs/{id}, jobs list (cursor), jobs/{id}/events (SSE), jobs/{id}/artifacts, jobs/{id}/debug-sessions, jobs/{id}/cancel, runtime-profiles, workers (admin).
- Job-create body (§7): `{ project_id, apk_upload_id, test_upload_id?, runtime_profile, mode, min_isolation, automation, artifacts, timeout_seconds }`.
- Job states (§7) for status rendering: `queued … running_tests … debug_available … passed|failed|cancelled|timed_out|infra_failed`.
- Debug-session response = a short-lived JWT (consumed by P8/P9, not decoded here).

**Downstream (depends on this):**
- **P8** `@run-anywhere/browser-emulator` mounts inside the job-detail/debug view; this dashboard provides the host page and passes the debug token to it.
- **P9** debug path is launched from the "Open debug session" action here.
- **P14** adds team auth/SSO/RBAC surfaces on top of this dashboard shell.

---

## 3. In scope / Out of scope

**In scope:**
- **Typed API client** generated from `openapi/v1.yaml`.
- **Project view**: list/select project; show/manage API keys (create returns plaintext once — surface that UX carefully).
- **Job submit**: APK upload (pre-signed PUT flow), runtime-profile picker (from `GET /v1/runtime-profiles`), `mode` + `min_isolation` selection, artifact toggles, timeout — POST with an `Idempotency-Key`.
- **Job list/detail**: paginated list (cursor), detail with current state, timeline of events.
- **Live event stream**: subscribe to the **SSE** endpoint; render lifecycle transitions in real time; stop on terminal state.
- **Artifact viewer**: screenshots, video player, logcat tail, JUnit/test report rendering — via pre-signed download URLs.
- **Debug entry point**: a button on a running/failed job that calls `POST …/debug-sessions` and opens the P8 viewer (the viewer itself ships in P8/P9; here it can be a stub that receives the token).
- **Optional OIDC login** (§7c) for the dashboard, mapping identities to roles; API-key mode still works for local/OSS.
- Sensible empty/loading/error states; accessible, responsive layout.

**Out of scope (later Parts):**
- The WebRTC viewer/input internals (P8) and the gateway/Envoy/TURN path (P9) — this Part hands off a token to that component.
- Team/org/SSO/RBAC admin (P14).
- Billing/retention UI (P14).

---

## 4. Deliverables

1. **`web/`** Next.js App Router app (TypeScript) with the generated client.
2. **Pages/routes**: projects, project detail (+ keys), job submit, jobs list, job detail (events + artifacts + debug button), workers (admin, read-only), settings/login.
3. **SSE client hook** for live job events with reconnect/backoff.
4. **Upload flow** using pre-signed PUT.
5. **Runtime-profile picker** populated from the API.
6. **Optional OIDC** auth integration (feature-flagged) + API-key fallback.
7. **A documented "debug viewer slot"** where P8's component mounts and receives `{ sessionToken, jobId }`.
8. **Tests**: client generation check, key UI flows (submit, list, detail, SSE rendering) via component/e2e tests against the Compose API.

---

## 5. Key interfaces & contracts

- **API client** mirrors `openapi/v1.yaml` exactly; regenerate on spec change.
- **SSE**: consume `GET /v1/jobs/{id}/events`; events carry `{ type, payload }` ordered by sequence (P2/P3).
- **Upload**: `POST /v1/uploads/apk` → pre-signed PUT → PUT file → use returned `apk_upload_id` in job create.
- **Debug handoff**: `POST /v1/jobs/{id}/debug-sessions` → `{ token }` → pass to the P8 `EmulatorProvider.connect(token)` slot.
- **Auth**: bearer API key (local/OSS) or OIDC session (optional); never embed long-lived secrets in client bundles.

---

## 6. Architecture decisions to honor

- **OpenAPI is the contract (§7)** — generate the client; if the UI needs a field the spec lacks, change the spec, not the client by hand.
- **SSE for live updates (§7/§16)** — the dashboard reflects job lifecycle in real time off `job_events`.
- **Debug viewer is a separate package (§12)** — the dashboard only provides the host page + token; the viewer is P8 so it can be reused beyond the dashboard.
- **OIDC optional, API keys core (§7c)** — OSS users run with API keys; OIDC is an opt-in for teams.
- **Secure handling of keys/tokens** — show the plaintext API key exactly once (matches P2's "return plaintext once"); treat debug tokens as short-lived and never persist them.
- **Honest states** — surface `infra_failed` distinctly from `failed` (test outcome vs. infrastructure), matching the state machine.

---

## 7. Acceptance criteria

1. The API client is generated from `openapi/v1.yaml` and a CI check fails on drift.
2. A user can create a project, mint an API key (shown once), upload an APK via pre-signed PUT, pick a runtime profile, and submit a `headless_ci` job.
3. The job-detail page streams live events via SSE and stops cleanly at a terminal state, distinguishing `passed/failed/cancelled/timed_out/infra_failed`.
4. Artifacts render: screenshot gallery, video player, logcat tail, and a JUnit/test report view, all via pre-signed URLs.
5. The "Open debug session" button calls the debug-session endpoint and mounts the viewer slot with the returned token (stub acceptable until P8).
6. Optional OIDC login works when configured and falls back to API-key mode otherwise.
7. Key flows pass component/e2e tests against the Compose API.

---

## 8. Risks & gotchas

- **SSE reconnects.** Network blips drop the stream; implement reconnect with backoff and resync from job state on reconnect, or the UI shows stale status.
- **Pre-signed upload CORS.** Browser PUTs to MinIO/S3 need correct CORS on the bucket; document/configure it or uploads fail only in the browser.
- **One-time key reveal.** If the user misses the plaintext key, they must rotate — make the reveal UX unmistakable.
- **Token leakage.** Don't log or persist debug tokens; they're audience-bound and short-lived by design (§7c).
- **Spec drift.** Hand-editing the generated client to "just ship" silently breaks the contract; always regenerate.
- **Scope creep into the viewer.** Resist building WebRTC/input here; that's P8 and must stay reusable.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: route map, the generated-client approach, the SSE hook design, the upload flow, the runtime-profile picker, the optional OIDC integration, the debug-viewer slot contract, and the test plan. **No code until approved.** Stay in scope (§3) — no WebRTC internals, no gateway, no team/billing admin. If the UI needs a new endpoint or field, surface it for §7/`openapi/v1.yaml` and `ENHANCED_ARCHITECTURE.md`.

---

### Continuity note
**Previous:** `PART_06_COMPOSE_STACK.md` runs this dashboard as the `web` service; `PART_03` is the API it consumes. **Next:** `PART_08_BROWSER_EMULATOR_PACKAGE.md` builds the `@run-anywhere/browser-emulator` component that mounts in this dashboard's debug slot. **Also soon:** `PART_09` powers the "Open debug session" action end-to-end; `PART_14` extends this shell with team auth/SSO/RBAC.
