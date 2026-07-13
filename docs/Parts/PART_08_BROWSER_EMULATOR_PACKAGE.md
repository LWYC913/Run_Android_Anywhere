# PART 08 — Browser Emulator Package (`@run-anywhere/browser-emulator`)

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

Build the shared TypeScript package `@run-anywhere/browser-emulator`: the **`EmulatorProvider` interface**, a **`RemoteWebRtcProvider`** (a fresh, thin client over the emulator's gRPC-Web protocol — the archived React lib is **reference only**), a **mock provider** for UI dev, and the shared UI/input components. This is the one viewport/controller surface reused by debug sessions and any future player (§12).

---

## 2. Where this sits

**Upstream (must already exist):**
- **P7** dashboard — provides the host page and the **debug-viewer slot** that mounts this package and passes `{ sessionToken, jobId }`.
- **P3** API — mints the **debug-session JWT** (`{ aud, jti, mode, exp }`) that `connect(token)` carries.
- **ENHANCED_ARCHITECTURE.md §11b/§12** — the signaling stack contract this client targets.

Inlined contracts you rely on (from §12):

```ts
type EmulatorProviderKind =
  | "remote_webrtc"
  | "remote_screenshot_fallback"
  | "browser_native_wasm_experimental";

interface EmulatorProvider {
  kind: EmulatorProviderKind;
  connect(sessionToken: string): Promise<void>;
  disconnect(): Promise<void>;
  sendTouch(event: TouchInput): void;
  sendKey(event: KeyInput): void;
  sendText(text: string): void;
  captureScreenshot(): Promise<Blob>;
}
```

- The **emulator speaks gRPC** (`-grpc :8554`); browsers reach it via **Envoy gRPC-Web** (unary + server-streaming only); media flows over **WebRTC**, relayed via **TURN** behind NAT (§11b). The full path is wired in P9 — **this Part builds the browser-side client against that contract**, and uses the mock provider until P9 is live.
- `android-emulator-webrtc` (React) is **archived → reference only**; treat the **gRPC streaming protocol as the contract** (§11b).

**Downstream (depends on this):**
- **P9** connects this client through the real gateway → Envoy → emulator gRPC → TURN path and validates the JWT at the gateway.
- **P15** adds a `BrowserNativeWasmProvider` behind a flag, reusing this package's UI/input surface unchanged.

---

## 3. In scope / Out of scope

**In scope:**
- The **`EmulatorProvider`** interface + input types (`TouchInput`, `KeyInput`, etc.) as the package's public contract.
- **`RemoteWebRtcProvider`**: establishes the WebRTC peer connection and the gRPC-Web signaling/control channel; renders video; sends touch/key/text over the data channel; supports a **read-only viewer mode** vs **controller mode** (from the JWT `mode`).
- **`MockProvider`**: a deterministic fake (static frames / canvas) so the dashboard and components can be developed and tested without a live runtime.
- **`RemoteScreenshotFallbackProvider`** (admin/testing only): periodic screenshot polling when WebRTC isn't available.
- **Shared components**: `EmulatorViewport`, `InputBridge`, `TouchMapper`, `KeyboardMapper`, `ConnectionQualityIndicator` (FPS/bitrate/RTT), `DebugToolbar`, `ScreenshotButton`, `LogcatPanel`, `ArtifactPanel`, `SessionTimer`.
- **Provider-agnostic input layer**: one input surface; only *delivery* differs per provider (WebRTC data channel vs in-browser canvas later).
- Clean teardown on `disconnect()` / session expiry; surfaced connection states (connecting/connected/relayed/failed/expired).

**Out of scope (later Parts):**
- The **gateway/Envoy/TURN/token-service** deployment and JWT **validation** (P9) — this Part assumes a reachable gRPC-Web endpoint contract and a token it forwards.
- `BrowserNativeWasmProvider` implementation (P15) — only leave the provider-kind seam.
- Gamepad mapping and advanced metrics (later).

---

## 4. Deliverables

1. **`packages/browser-emulator/`** TS package: public `EmulatorProvider` interface + input types.
2. **`RemoteWebRtcProvider`** built fresh against the emulator gRPC-Web/WebRTC protocol (reference the archived lib for the wire shape only).
3. **`MockProvider`** + **`RemoteScreenshotFallbackProvider`**.
4. **Shared components** (viewport, input bridge, toolbar, logcat/artifact panels, quality indicator, session timer).
5. **A storybook/dev harness** driving the components via `MockProvider`.
6. **Viewer/controller mode** handling derived from the session token's `mode`.
7. **Tests**: provider interface conformance, input event mapping, mode gating (controller-only inputs blocked in viewer mode), teardown on disconnect/expiry.

---

## 5. Key interfaces & contracts

- **Public API**: the `EmulatorProvider` interface above + `EmulatorViewport` React component taking a provider instance.
- **Connect contract**: `connect(sessionToken)` where the token is the P3 debug JWT; the provider forwards it to the gateway (P9) — the package does **not** validate or decode trust from it, it just carries it.
- **Signaling contract (§11b)**: gRPC-Web (unary + server-streaming) for control/signaling; WebRTC for media; ICE/TURN config supplied by the session bootstrap (P9).
- **Mode**: `viewer` disables input senders; `controller` enables them.
- **Provider swap**: switching providers must not change the component API (the §24 test "browser package switches providers without UI change").

---

## 6. Architecture decisions to honor

- **Build the WebRTC client fresh (§11b)** — the archived React lib is reference only; the **gRPC streaming protocol is the contract**.
- **One package, multiple providers (§12)** — debug, fallback, and future browser-native and player sessions all share this UI/input surface.
- **Production default is browser-as-viewer (§12)** — the runtime is server-side; the browser views/controls. Browser-as-runtime is experimental and isolated to P15.
- **Provider-agnostic input (§12)** — the input layer is shared; only delivery differs.
- **Security posture (§11/§7c)** — no unauthenticated signaling; the package assumes the gateway enforces auth and just forwards the short-lived token; honor `mode` and auto-teardown on expiry.
- **TURN-relayed media is normal (§11b)** — surface "relayed" as a healthy connection state, not an error.

---

## 7. Acceptance criteria

1. The package exports a stable `EmulatorProvider` interface and `EmulatorViewport`; the dashboard mounts it via the P7 slot.
2. `MockProvider` drives all components in the dev harness with no live backend.
3. `RemoteWebRtcProvider` is implemented against the gRPC-Web/WebRTC protocol contract (verified against P9 in that Part; here, against a protocol-level fake/contract test).
4. Controller-only inputs (`sendTouch/sendKey/sendText`) are blocked when the token `mode` is `viewer`.
5. `disconnect()` and session-expiry both fully tear down the peer connection and channels (no leaked media).
6. Swapping `MockProvider` ↔ `RemoteWebRtcProvider` requires **no** change to component usage.
7. Connection states (connecting/connected/relayed/failed/expired) are surfaced to the UI.

---

## 8. Risks & gotchas

- **gRPC-Web limits.** Only unary + server-streaming are supported through Envoy; design the control/signaling channel within that constraint (§11b) — don't assume bidi streaming.
- **Don't depend on the archived lib.** Importing `android-emulator-webrtc` couples you to unmaintained code; reimplement the thin client.
- **ICE/TURN config provenance.** TURN credentials/ICE servers must come from the session bootstrap (P9), short-lived; never hardcode TURN secrets in the bundle.
- **Mode enforcement is defense-in-depth only.** Client-side viewer/controller gating is UX; the gateway/runtime must also enforce it — don't treat the client as the security boundary.
- **Teardown leaks.** Failing to close peer connections on unmount/expiry leaks media and keeps sessions "alive" — test teardown explicitly.
- **Input mapping fidelity.** Touch/key coordinate mapping between the viewport and device resolution is fiddly; centralize it in `TouchMapper`/`KeyboardMapper`.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the package layout, the `EmulatorProvider` interface + input types, the `RemoteWebRtcProvider` design against the gRPC-Web/WebRTC contract, the mock/fallback providers, the component inventory, the viewer/controller mode handling, and the test plan (incl. provider-swap and teardown). **No code until approved.** Stay in scope (§3) — no gateway/Envoy/TURN deployment, no browser-native provider. If the protocol contract needs clarification or a field, surface it for §11b/§12 in `ENHANCED_ARCHITECTURE.md`.

---

### Continuity note
**Previous:** `PART_07_DASHBOARD.md` provides the host page/slot and the debug token this package consumes. **Next:** `PART_09_WEBRTC_DEBUG_E2E.md` wires the real gateway → Envoy → emulator gRPC → TURN path that `RemoteWebRtcProvider` connects through, and validates the JWT. **Also soon:** `PART_15` adds a `BrowserNativeWasmProvider` behind a flag, reusing this package's UI/input surface unchanged.
