# PART 15 — Browser-Native WASM Research (experimental, optional)

Codename: **Open Source** · Plan-mode brief · Global reference: `ENHANCED_ARCHITECTURE.md`
Attach this file **and** `ENHANCED_ARCHITECTURE.md` to the plan-mode session.

---

## 1. Mission

A **contained experiment**: add a `BrowserNativeWasmProvider` behind a flag and run a **v86/WebVM feasibility spike** for tiny demo workloads, reusing the P8 UI/input surface — **without touching the revenue/QA path**. This is the §12 "browser as runtime" idea kept honest: research and demos only, never advertised as arbitrary-APK QA (§12 feasibility verdict, §23 Phase 5).

---

## 2. Where this sits

**Upstream (must already exist):**
- **P8** `@run-anywhere/browser-emulator` — the shared package with the `EmulatorProvider` interface and the **`browser_native_wasm_experimental`** provider-kind seam already reserved. This Part fills that seam **without changing the component API** (the §24 test "browser package switches providers without UI change" must still hold).

Inlined contract (from §12):
```ts
type EmulatorProviderKind =
  | "remote_webrtc"
  | "remote_screenshot_fallback"
  | "browser_native_wasm_experimental";   // ← this Part

interface EmulatorProvider {
  kind; connect(sessionToken); disconnect();
  sendTouch(); sendKey(); sendText(); captureScreenshot();
}
```

**Downstream:** none. This is a leaf, intentionally isolated from the product promise.

**Feasibility verdict to respect (§12):** modern Android emulation needs **KVM/hardware acceleration the browser sandbox doesn't expose**; arbitrary APKs bring native ARM libs, Play Services assumptions, GPU needs, and anti-emulator checks. **v86/WebVM prove browser-hosted VM concepts, not production Android APK QA.**

---

## 3. In scope / Out of scope

**In scope:**
- A **`BrowserNativeWasmProvider`** implementing `EmulatorProvider`, gated behind an explicit **experimental flag** (off by default; never selectable on the QA/CI path).
- A **v86/WebVM feasibility spike**: run a tiny x86/Linux (or minimal Android-adjacent) workload in-browser to validate the in-browser VM concept and the shared input/viewport surface (canvas delivery vs WebRTC).
- **Honest capability docs**: what runs (toy/demo workloads), what does not (modern Android APKs, GPU, Play Services, native ARM), and why (the §12 verdict).
- Reuse of the P8 components (`EmulatorViewport`, `InputBridge`, etc.) via in-browser canvas delivery, proving the input layer is delivery-agnostic.

**Out of scope (explicitly):**
- Any path from this provider to **real jobs, the scheduler, runtimes, billing, or SLOs** — it must remain off the revenue/QA path (§14/§19/§23).
- Claims of arbitrary-APK compatibility (forbidden by §12/§21/§25).
- Production hardening/observability for this provider (it's research).
- Changing the P8 component API or the `EmulatorProvider` interface.

---

## 4. Deliverables

1. **`BrowserNativeWasmProvider`** in the P8 package, behind an experimental flag, satisfying the `EmulatorProvider` interface.
2. A **feasibility-spike harness** (v86/WebVM) running a tiny demo workload via the shared P8 UI.
3. A **findings doc**: what worked, performance, hard limits, and an explicit "not for Android APK QA" statement with the §12 rationale.
4. **Tests**: the provider conforms to the interface; switching to it requires **no** component API change; it is **not** selectable on the QA/CI path (guard test).

---

## 5. Key interfaces & contracts

- **`EmulatorProvider` (P8)** — implemented unchanged; `connect()` may take a local/spike config rather than a remote session token (document the divergence, keep the signature).
- **Delivery:** in-browser **canvas** (vs the WebRTC data channel of `RemoteWebRtcProvider`); the **input layer is shared** — only delivery differs (§12).
- **Flag contract:** experimental-only; default off; excluded from any job/runtime selection (`runtime_kind = browser_native_wasm` is research-tagged and never matched by the scheduler for real jobs).
- **Component API invariance:** the §24 provider-swap test must pass.

---

## 6. Architecture decisions to honor

- **Research-only, never the default (§12/§23/§25)** — keep it entirely off the revenue/QA path; never advertise arbitrary-APK support.
- **One shared package, differ only in providers (§12)** — reuse P8's UI/input; don't fork a second viewer.
- **Honest limitations (§18/§21/§25)** — document what browser-native can't do and why; under-promise.
- **No interface drift (P8/§12)** — the provider conforms to the existing `EmulatorProvider`; the component API is invariant.
- **Isolation from product promise (§19/§23 Phase 5)** — a contained experiment that shares the UI without risking the core promise.

---

## 7. Acceptance criteria

1. `BrowserNativeWasmProvider` implements `EmulatorProvider` and is **off by default**, behind an experimental flag.
2. The v86/WebVM spike runs a **tiny demo workload** in-browser via the shared P8 viewport/input.
3. Switching `RemoteWebRtcProvider` ↔ `BrowserNativeWasmProvider` requires **no** change to component usage (provider-swap test passes).
4. The provider is **not** selectable on the QA/CI/job path (guard test); `browser_native_wasm` is never matched by the scheduler for real jobs.
5. The findings doc clearly states capabilities/limits and the "not for Android APK QA" verdict with rationale.

---

## 8. Risks & gotchas

- **Scope contamination.** The biggest risk is this leaking into the product as if it were QA-capable — keep it flagged, unmatched by the scheduler, and loudly documented as research (§12/§25).
- **Performance disappointment.** Browser-hosted VMs are slow and limited; frame it as a concept proof, not a runtime — manage expectations in docs.
- **Anti-emulator/native/GPU walls.** Real APKs hit native ARM libs, Play Services, GPU needs, and anti-emulator checks the browser can't satisfy (§12) — don't attempt to "make APKs work."
- **Interface temptation.** Don't bend the `EmulatorProvider` interface to fit the spike; if it truly can't fit, that's a P8 contract discussion to surface, not a silent fork.
- **Time sink.** This is optional and last for a reason — timebox the spike; it must never block the product Parts.

---

## 9. Plan-mode instructions

Produce an **implementation plan only**: the `BrowserNativeWasmProvider` design behind an experimental flag, the v86/WebVM spike harness reusing P8, the guard ensuring it's off the QA/CI/scheduler path, the findings-doc outline, and the conformance/provider-swap/guard tests. **No code until approved.** Stay in scope (§3) — research only; no product/scheduler/billing integration; no interface changes. If the spike reveals the interface can't accommodate browser-native cleanly, surface it for §12 in `ENHANCED_ARCHITECTURE.md` rather than forking the UI.

---

### Continuity note
**Previous:** `PART_08_BROWSER_EMULATOR_PACKAGE.md` reserved the `browser_native_wasm_experimental` provider seam and the shared UI/input surface this experiment fills. **Next:** none — this is the final, optional Part, intentionally isolated from the revenue/QA path. **Loops back to:** the §25 promise — the shared browser UI is valuable across providers, while browser-native Android execution stays research-only until proven.
