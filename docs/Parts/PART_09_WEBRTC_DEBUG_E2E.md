# PART 09 — WebRTC Debug End-to-End (gateway / Envoy / TURN / JWT)

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Wire the full §11b debug path so a browser can securely view/control a running or failed emulator job: the **session gateway** (JWT validation + per-session routing), **Envoy** (gRPC-Web ↔ gRPC), the emulator's internal **gRPC `:8554`**, **TURN/coturn** for NAT traversal, and the **JWT token service**/validation — connecting the P8 `RemoteWebRtcProvider` to the P5 emulator. This is the "open a failed run in the browser" promise (§16, §25) made real and safe.

---

## 2. Where this sits

**Upstream (must already exist):**
- **P3** API — **mints** the debug-session JWT (`{ aud: "<job_id>:<session_id>", jti, mode: viewer|controller, exp }`) and persists a `debug_sessions` row. This Part **validates** that token.
- **P5** worker/emulator adapter — launches the emulator with **`-grpc :8554`** (internal only) and exposes a `debug_endpoint()` for a running job.
- **P8** `RemoteWebRtcProvider` — the browser client built against the gRPC-Web/WebRTC contract; it calls `connect(token)` and expects ICE/TURN config from the session bootstrap.
- **P6** Compose stack — has a `debug` profile placeholder (Envoy + TURN + token service) to fill here.

Inlined contract (§11b data flow):
```
Browser (RemoteWebRtcProvider)
  → Session gateway  (JWT check: aud/jti/mode/exp, per-session route)
    → Envoy          (gRPC-Web ↔ gRPC)
      → Emulator gRPC :8554  (control + signaling, internal only)
WebRTC media: Emulator ⇄ TURN/coturn ⇄ Browser  (relayed when P2P blocked)
```

**Downstream (depends on this):**
- **P11** deploys these components in Kubernetes (the emulator is essentially always behind NAT there → **TURN is required**, §11b) with NetworkPolicy.
- **P13** hardens (rate limits, audit completeness) and adds TURN-bandwidth metrics.

---

## 3. In scope / Out of scope

**In scope:**
- **Session gateway**: validate the debug JWT (audience = this job/session, check `jti` for single-use/revocation, enforce `mode`, check `exp`), then route to the correct emulator pod/container's Envoy endpoint. **No runtime is reachable without passing this.**
- **Envoy** config as the **gRPC-Web proxy** to the emulator's gRPC (unary + server-streaming).
- **Emulator gRPC `:8554`** kept **internal only** (never publicly exposed).
- **TURN/coturn**: deploy + issue short-lived TURN credentials/ICE config in the session bootstrap; relay media when P2P is blocked. **Required** behind cluster NAT; optional for single-host.
- **JWT token service / validation**: the signing key lives only in the token service (§15b); the gateway validates; `jti` enables single-use + revocation; tie revocation to the reconciler's expired-session reaping (P4).
- **Debug-session lifecycle behavior**: short-lived, one viewer/controller by default, auto-expire on timeout or job completion, **full audit** (who/when/which job) — completing the §7c/§11 rules.
- Activate the **Compose `debug` profile** end-to-end; provide an e2e test that opens a session against a running emulator job and exchanges media.

**Out of scope (later Parts):**
- The browser viewport/input internals (P8) — already built; this Part connects it.
- K8s deployment of these components (P11) — here they run under the Compose `debug` profile; Helm/NetworkPolicy come in P11.
- Rate limiting/quotas + TURN-bandwidth dashboards (P13).

---

## 4. Deliverables

1. **Session gateway** service: JWT validation (aud/jti/mode/exp), per-session routing to the right emulator Envoy, deny-by-default for anything unauthenticated.
2. **Envoy** gRPC-Web proxy config fronting the emulator gRPC `:8554`.
3. **coturn** deployment + **short-lived TURN credential issuance** in the session bootstrap (ICE config returned to the browser).
4. **Token validation** wired to the token service (signing key isolated), with `jti` single-use/revocation honored.
5. **Debug-session bootstrap endpoint/flow**: given a valid token, return ICE/TURN config + the gateway route the `RemoteWebRtcProvider` needs.
6. **Audit + auto-expire**: every session start/stop audited; sessions auto-close on job completion/timeout (coordinated with P4's reconciler).
7. **Compose `debug` profile** filled in; an **e2e test**: mint token (P3) → connect (P8) → gateway → Envoy → emulator → media via TURN → input round-trip → expiry/teardown.

---

## 5. Key interfaces & contracts

- **JWT claims (from P3):** `aud = "<job_id>:<session_id>"`, `jti`, `mode ∈ {viewer, controller}`, `exp`. Gateway validates all four; `controller` required for input.
- **Gateway route:** maps `aud` → the specific emulator's Envoy endpoint; rejects mismatches.
- **Envoy:** gRPC-Web (unary + server-streaming) ↔ emulator gRPC `:8554`.
- **TURN bootstrap:** short-lived `{ iceServers, turnCredential, ttl }` handed to the browser; secret never sent to the client beyond the ephemeral credential.
- **Revocation:** `jti` invalidation on session end/expiry; the reconciler (P4) revokes `debug_sessions` past `expires_at`.

---

## 6. Architecture decisions to honor

- **Four-component path is mandatory (§11b)** — gateway + Envoy + emulator gRPC + TURN; do not collapse the gateway or skip TURN in cluster (the emulator is behind NAT).
- **No unauthenticated path to a runtime (§11/§7c)** — the gateway is the only door; emulator gRPC and ADB stay internal-only.
- **Short-lived, audience-bound, audited tokens (§7c)** — validate aud/jti/mode/exp; single-use via `jti`; audit everything.
- **gRPC-Web constraints (§11b)** — design within unary + server-streaming; the archived React lib is reference only.
- **Signing key isolation (§15b)** — only the token service holds it; the gateway verifies.
- **Auto-expire on completion (§11)** — a finished/timed-out job's debug session closes automatically; the reconciler enforces backstop revocation.

---

## 7. Acceptance criteria

1. A browser with a **valid** debug token connects through gateway → Envoy → emulator gRPC and receives live video; input round-trips in `controller` mode.
2. An **invalid/expired/mismatched-aud** token is rejected at the gateway; no runtime is reachable.
3. A **`viewer`-mode** token cannot send input (enforced server-side, not just in P8's client).
4. With P2P blocked, media is **relayed via TURN** and the session still works (surfaced as "relayed").
5. The emulator gRPC `:8554` and ADB are **not** reachable except through the gateway path (verified).
6. Session start/stop is **audited**; a session auto-closes on job completion/timeout, and the reconciler revokes any session past `expires_at`.
7. The Compose `debug` profile runs the full e2e test green.

---

## 8. Risks & gotchas

- **TURN is not optional in cluster.** Skipping it "works on my laptop" then fails behind NAT in P11 — wire and test it now (§11b).
- **Gateway as the only door.** Any direct route to Envoy/emulator that bypasses JWT validation is a critical hole — default-deny and verify there's no side path.
- **Server-side mode enforcement.** Viewer/controller must be enforced at the gateway/runtime, not only in the browser (P8 gating is UX).
- **`jti` replay.** Without single-use/revocation, a leaked token is reusable until `exp`; honor `jti` and tie it to session lifecycle + reconciler revocation.
- **Envoy gRPC-Web misconfig.** Bidi-streaming assumptions break through Envoy; keep to unary + server-streaming.
- **Credential leakage.** Only ephemeral TURN credentials reach the client; the TURN shared secret and JWT signing key never leave the server side (§15b).
- **Clock skew.** `exp`/`jti` checks fail intermittently under skew; sync clocks and allow small leeway.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the gateway validation+routing design, the Envoy gRPC-Web config approach, the coturn deployment + ephemeral-credential issuance, the token-validation/`jti`-revocation flow, the session-bootstrap contract handed to P8, the audit/auto-expire behavior, and the e2e test under the Compose `debug` profile. **No code until approved.** Stay in scope (§3) — no K8s manifests (P11), no rate-limit dashboards (P13). If the token claims or bootstrap contract need a field, surface it for §7c/§11b in `ENHANCED_ARCHITECTURE.md`.

---

### Continuity note
**Previous:** `PART_08_BROWSER_EMULATOR_PACKAGE.md` is the browser client this path connects; `PART_03` mints the token this Part validates; `PART_05` exposes the emulator `:8554` this Part fronts. **Next:** `PART_10_APPIUM_AUTOMATION.md` proceeds in parallel on the automation track. **Also soon:** `PART_11` deploys gateway/Envoy/TURN in Kubernetes with NetworkPolicy; `PART_13` adds rate limits and TURN-bandwidth metrics.
