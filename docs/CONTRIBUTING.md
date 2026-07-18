# Contributing

Run Android Anywhere is contract-first. Part 01 intentionally contains shared types and specifications, not service behavior. Database access, HTTP handlers, queue processing, runtime execution, deployment manifests, and frontend code belong to their later Parts.

## Contract changes

The Rust contracts and OpenAPI document are jointly authoritative. A public contract change must be made as one reviewable change:

1. Record the architectural decision in `docs/Architecture/ENHANCED_ARCHITECTURE.md` and update the affected Part brief.
2. Update the public Rust types or state-machine rules in `crates/contracts`.
3. Update `openapi/v1.yaml`, including operation security, examples, and error responses.
4. Add or update contract, serialization, transition, and OpenAPI-parity tests.
5. Note compatibility or migration work required by downstream Parts.

Do not merge a Rust-only or OpenAPI-only public contract change. The parity check deliberately fails when their schema or operation surfaces drift.

## Local checks

Install the Rust toolchain, Node.js, and `just`, then install the pinned OpenAPI tooling with `npm ci`. Run `just ci` before opening a pull request. Useful focused commands are:

- `just build` — compile every workspace target.
- `just test` — run all Rust tests.
- `just fmt-check` — check Rust formatting without rewriting files.
- `just clippy` — lint all targets and features with warnings denied.
- `just openapi-lint` — validate and lint the OpenAPI 3.1 document.
- `just contract-check` — compare Rust schemas and the documented API surface.
- `just msrv` — test the workspace on the declared minimum Rust version.

Run `just fmt` only when you intend to rewrite Rust formatting.

## Part boundaries

The `api`, `scheduler`, and `worker` binaries are compiling placeholders in Part 01. Keep them free of business logic until Parts 03, 04, and 05 respectively. The `web`, browser-emulator, Compose, and Kubernetes directories are tracked placeholders until their corresponding Parts begin.
