# Part 04 Implementation and Pull Request Handoff

## Pull Request Metadata

| Field | Value |
| --- | --- |
| Source branch | **codex/part-04-queue-scheduler-reconciler** |
| Target branch | **master** |
| Suggested PR title | **feat(scheduler): implement Part 04 queue, scheduler, and reconciler** |
| Suggested commit subject | **feat(scheduler): implement Part 04 queue, scheduler, and reconciler** |
| Change type | Feature, database migration, internal protocol, infrastructure, observability, and tests |
| Architecture source | [Enhanced Architecture](../Architecture/ENHANCED_ARCHITECTURE.md) |
| Part specification | [Part 04 — Queue, Scheduler & Reconciler](PART_04_QUEUE_SCHEDULER_RECONCILER.md) |

The architecture and Part 04 source documents are intentionally unchanged by this implementation.

## Pull Request Summary

This change implements Part 04 as a standalone Rust scheduler service backed by PostgreSQL and NATS JetStream.

The API remains the only producer of new **jobs.queued** work. An elected scheduler instance owns queue consumption, canonical database validation, fairness, worker matching, fenced claims, worker control requests, JetStream acknowledgements, durable dead-letter handling, quota enforcement, stale recovery, debug-session reconciliation, and runtime cleanup.

Multiple scheduler instances may run simultaneously. A session-level PostgreSQL advisory lock elects one active leader while the remaining instances stay passive. Losing the database session or lock stops leader-owned processing before another instance can take over.

The implementation remains within Part 04 boundaries:

- The secure worker protocol and dispatch contracts are implemented.
- The Compose runtime reaper is implemented through the Docker Engine API.
- The Part 5 worker execution loop is not implemented.
- Kubernetes resource deletion remains assigned to Part 11.
- Warm pools are not implemented.
- JetStream is the only queue backend; no Redis path is added.

## Why This Change Is Needed

Before this change, the repository had the API and persistence foundation but no production scheduler capable of safely converting durable queue messages into exactly fenced worker ownership.

Part 04 closes that gap by providing:

- Durable queue topology and startup drift reconciliation.
- Active-passive scheduler leadership.
- Fair, quota-aware worker dispatch.
- Exact worker and lease fencing across every mutation.
- Scheduler-owned JetStream acknowledgement semantics.
- Durable and idempotent DLQ publication.
- Crash and redelivery recovery.
- Stale worker, lease, cancellation, finalizer, and debug-session reconciliation.
- Safe Compose runtime cleanup.
- Production-oriented NATS security and encrypted-storage templates.
- Scheduler metrics and CI acceptance coverage.

## Architecture Overview

The implemented request path is:

1. The API validates and persists a job.
2. The API inserts an identifier-only **JobQueued** event in the PostgreSQL outbox.
3. The API outbox publisher publishes the event to **jobs.queued**.
4. The elected scheduler pulls the JetStream delivery.
5. The scheduler compares the message with the canonical PostgreSQL job and runtime profile.
6. Valid work enters the bounded fairness buffer.
7. The scheduler selects a compatible worker and atomically claims the job.
8. The scheduler sends a fenced **JobDispatch** request to that worker.
9. Worker heartbeats and transitions return through scheduler-owned control subjects.
10. The scheduler persists semantic completion before confirming the JetStream acknowledgement.
11. Reconciliation repairs abandoned leases, cancellations, finalizers, expired sessions, DLQ publication, and stale runtime resources.

### Ownership Boundaries

| Component | Responsibility |
| --- | --- |
| API | Submission validation, idempotency, outstanding quota admission, PostgreSQL outbox insertion, and **jobs.queued** publication |
| PostgreSQL | Canonical job state, project quotas, worker state, lease fences, outbox records, audit events, and leadership lock |
| JetStream | Durable delivery, redelivery, advisory capture, and dead-letter retention |
| Scheduler leader | Pull consumption, canonical validation, fairness, matching, claim, dispatch, ACK decisions, DLQ, and reconciliation |
| Passive scheduler | Wait for leadership without consuming or reconciling work |
| Worker | Accept a fenced dispatch and report heartbeats, transitions, and results through its own scoped subjects |
| Runtime reaper | Inventory and idempotently remove only resources proven unsafe after the configured grace period |

## JetStream Topology

Topology is provisioned idempotently at scheduler startup.

Safe operational fields such as descriptions, capacities, retention windows, duplicate windows, message limits, and replica counts are reconciled. Semantic incompatibilities such as the wrong retention policy, storage type, subject ownership, filter subject, delivery policy, or durable consumer definition fail startup with an explicit incompatibility error.

### Streams

| Stream | Subject or advisory | Retention | Storage | Important defaults |
| --- | --- | --- | --- | --- |
| **JOB_QUEUE** | **jobs.queued** | Work queue | File | Discard new, 10 GiB, 256 KiB message limit, configurable replicas |
| **JOB_DLQ** | **jobs.dead** | Limits | File | 2 GiB, 30-day retention, 30-day duplicate window |
| **JOB_ADVISORIES** | Maximum-delivery and terminated-message advisories for the dispatch consumer | Limits | File | 256 MiB, 30-day retention |

The queue duplicate window is aligned with durable advisory retention so a crash-recovery redrive can retain a stable idempotency horizon.

### Consumers

| Consumer | Stream | Behavior |
| --- | --- | --- |
| **job-dispatch-v1** | **JOB_QUEUE** | Durable pull consumer, explicit ACK, 60-second ACK wait, 256 maximum ACK pending, default maximum delivery of 4 |
| **job-advisory-recovery-v1** | **JOB_ADVISORIES** | Durable pull consumer for maximum-delivery and terminated-message recovery |

The dispatch consumer uses four deliveries by default:

- Deliveries one through three are eligible execution attempts.
- The final delivery exists for durable exhaustion and DLQ processing.
- Capacity, compatibility, or quota blocking uses progress acknowledgements and does not consume the execution-attempt budget.

## Queue Payload and Canonical Validation

The persisted **JobQueued** payload contains scheduling identifiers and safe scalar scheduling data only:

- Job ID.
- Project ID.
- Runtime profile ID.
- Minimum isolation tier.
- Timeout seconds.

It does not contain:

- APK contents.
- Signed object-store URLs.
- API keys.
- Debug JWTs.
- Storage credentials.
- Full runtime image references.

The scheduler always loads the canonical job and full runtime profile from PostgreSQL before matching or dispatching.

For compatibility with older queued records, the custom **JobQueued** decoder accepts the legacy full-profile shape, reduces it to the runtime profile ID in memory, and never serializes the legacy full profile again.

### Delivery Decisions

| Condition | Scheduler action |
| --- | --- |
| Malformed or oversized payload | Insert sanitized durable DLQ event, publish-confirm it, terminate delivery, and delete the source message |
| Missing or database-inconsistent canonical job | Durable DLQ using a stable poison-message key |
| Canonical payload differs from the queued payload | Redrive the canonical identifier-only payload, then retire the inconsistent source |
| Job is already terminal | Safely complete any required finalization and ACK the unused delivery |
| Queued cancellation | Finalize an empty artifact set, complete cleanup, transition to cancelled, and ACK |
| Valid queued job | Insert into the bounded fairness buffer |
| Existing valid database lease on redelivery | Rebind the new delivery to the exact lease without incrementing attempts or starting another run |

## Fairness and Scheduling

Priority remains internal. No public priority field is introduced.

| Job mode | Internal lane |
| --- | --- |
| **browser_debug** | Interactive |
| **headless_ci** | Batch |

The default scheduling policy is:

- Weighted round-robin with three interactive selections for every one batch selection.
- Round-robin projects within each lane.
- FIFO ordering by canonical creation time and job ID within each project.
- Batch promotion after five minutes to prevent CI starvation.
- A bounded in-memory fairness buffer with progress renewal for every held JetStream delivery.

### Deterministic Worker Selection

Eligible workers are ordered by:

1. Lowest utilization.
2. Lowest active-job count.
3. Newest heartbeat.
4. Worker ID.

The matcher requires:

- Worker state is online.
- Heartbeat is newer than the configured freshness threshold.
- Spare worker capacity exists.
- Runtime kind matches.
- Host architecture matches exactly.
- ABI compatibility is satisfied.
- KVM is present when required.
- Worker isolation capability satisfies the requested tier.

All matching checks are repeated inside the atomic repository claim after the job, project, and worker rows are locked.

## Project Quotas

Migration **0008_project_job_quotas** adds:

| Column | Default | Meaning |
| --- | --- | --- |
| **projects.max_concurrent_jobs** | 2 | Maximum active leases for a project |
| **projects.max_outstanding_jobs** | 100 | Maximum nonterminal submitted jobs for a project |

The migration also adds:

- Positive range constraints.
- A constraint requiring concurrent quota not to exceed outstanding quota.
- A partial index for nonterminal outstanding jobs.
- A partial index for active nonterminal leases.
- A reversible down migration that removes the indexes, constraints, and columns.

### Outstanding Admission

New submissions lock the project row and count nonterminal jobs before insertion.

Idempotent replay lookup happens before quota validation. Therefore, retrying an existing idempotency key still returns the original job even when the project is currently at its outstanding limit.

A genuinely new submission at the limit returns the existing HTTP 429 **quota_exceeded** response.

### Concurrent Claim Quota

The claim transaction locks in this order:

1. Job row.
2. Project row.
3. Worker row.

It counts active project leases while holding the project lock. At the concurrent limit, the repository returns a typed quota block without:

- Assigning a worker.
- Reserving worker capacity.
- Incrementing job delivery attempts.
- Affecting another project's work.
- Consuming the poison-message budget.

Both quota values are exposed in the public **Project** contract and OpenAPI response.

## Worker Control Protocol

The scheduler defines these internal NATS subjects:

| Subject | Direction | Purpose |
| --- | --- | --- |
| **control.workers.&lt;worker_id&gt;.register** | Worker to scheduler | Register or refresh worker capabilities |
| **control.workers.&lt;worker_id&gt;.heartbeat** | Worker to scheduler | Extend an exact job lease and report capacity |
| **control.jobs.&lt;worker_id&gt;.transition** | Worker to scheduler | Request a fenced state transition |
| **control.jobs.&lt;worker_id&gt;.result** | Worker to scheduler | Persist a fenced job result |
| **workers.&lt;worker_id&gt;.dispatch** | Scheduler to worker | Send a fenced **JobDispatch** request |

Shared contracts include:

- **JobDispatch**.
- Fenced transition requests.
- Typed accepted, retry, stale-lease, and quota responses.
- Existing **WorkerRegistration**, **WorkerHeartbeat**, **JobClaim**, **JobResult**, and **LeaseId** contracts.

Every job mutation checks the exact worker ID and lease ID. Late heartbeats, transitions, or results from a superseded worker are rejected as stale lease operations.

### Reply-Subject Protection

Worker-controlled request reply subjects are treated as untrusted input.

Before decoding or mutating anything, the scheduler verifies that a control request from worker **W** can only receive a response below:

**_INBOX.workers.W.>**

This prevents the scheduler from becoming a confused deputy that publishes worker-controlled responses to queue, DLQ, dispatch, or another worker's inbox subjects.

## Acknowledgement Ownership

Workers never receive raw JetStream ACK subjects.

The scheduler maintains an in-memory acknowledgement registry that binds:

- Job ID.
- Worker ID.
- Lease ID.
- JetStream source sequence.
- Current delivery.

The scheduler performs:

- **Progress** while a message is buffered, waiting for capacity, or supported by a healthy heartbeat.
- Confirmed **Ack** only after semantic completion is durably stored.
- **Nak** after a recoverable stale lease is CAS-requeued.
- **Term** after durable dead-letter handling.

The registry uses atomic replacement and removal operations so result handling and reconciliation cannot both take ownership of the same local ACK binding.

## Dispatch and Result Ordering

The fenced dispatch path is:

1. Validate canonical job and queue data.
2. Select compatible workers deterministically.
3. Call the atomic repository claim.
4. Increment attempts only after a successful claim.
5. Send **JobDispatch** to the claimed worker.
6. Require an acceptance reply within the configured control deadline.
7. Bind the JetStream delivery to the exact worker and lease.
8. Extend the PostgreSQL lease and send JetStream progress on accepted heartbeats.
9. Persist worker result and finalizer state before acknowledging JetStream.

If a scheduler crashes:

- Before claim, JetStream redelivers unowned work.
- After claim, redelivery observes and rebinds the existing database lease.
- Before DLQ publication, the durable PostgreSQL outbox remains publishable.
- After terminal commit but before ACK, redelivery recognizes terminal canonical state and safely ACKs.

## Durable Dead-Letter Handling

Dead-letter payloads are sanitized and contain identifiers, attempts, reason, and timestamps rather than original queue payloads or secrets.

The DLQ process is:

1. Insert or find an idempotent **JobDeadLetter** entry in the PostgreSQL outbox.
2. Publish it to **jobs.dead**.
3. Wait for the JetStream publication acknowledgement.
4. Mark the outbox record published.
5. Terminate the exhausted source delivery.
6. Delete the source-stream message.

Stable keys include:

- **dlq:&lt;job_id&gt;:&lt;attempt&gt;** for execution exhaustion.
- A source-stream-sequence identity for malformed or inconsistent poison messages.

The durable advisory consumer repairs a scheduler crash during the maximum-delivery or terminated-message path. It explicitly retrieves the source message, checks canonical PostgreSQL state and durable exhaustion markers, then either redrives canonical work or completes the DLQ retirement.

## Reconciler

Reconciliation runs only while leadership is held and defaults to every ten seconds.

It processes every lease-bearing nonterminal state rather than depending on a generic running state.

### Stale Lease Recovery

If the lease expired or the worker heartbeat became stale:

- Below the run-attempt limit, perform a CAS requeue.
- Release the old worker's capacity.
- Remove the old ACK binding.
- Nak the delivery for redelivery.

At exhaustion:

- Enter the legal finalizer path.
- Set the pending outcome to infrastructure failure.
- Persist the sanitized DLQ outbox record.
- Reap stale runtime resources where safe.
- Confirm artifact finalization.
- Complete legal **collecting_artifacts → cleaning_up → infra_failed** transitions.

Queued or active cancellations are never resurrected as runnable jobs. They enter cancellation finalization instead.

### Debug Session Reconciliation

Expired debug sessions and sessions whose owning job ended are:

- Ended atomically.
- Given a reasoned audit event.
- Processed idempotently so repeated reconciliation does not duplicate the end event.

### Failure Isolation

Runtime inventory or deletion errors fail closed for destructive cleanup, but do not prevent independent debug-session, cancellation, or DLQ reconciliation work from progressing.

## Runtime Reaper

The **RuntimeReaper** abstraction exposes:

- Inventory of managed runtime resources.
- Idempotent reap operations.

A runtime resource is represented by:

- Resource ID.
- Job ID.
- Worker ID.
- Lease ID.
- Creation time.

The Docker Engine adapter supports local Unix sockets, Windows named pipes, loopback HTTP for development, and HTTPS endpoints.

Managed Compose resources must carry:

- **io.run-anywhere.managed=true**.
- **job-id**.
- **worker-id**.
- **lease-id**.
- **runtime-kind**.

Part 5 workers are responsible for applying these labels.

### Reaper Safety Rules

- Active resources with an exact matching persisted lease are always kept, regardless of age.
- Unknown database ownership fails closed.
- Missing jobs, terminal jobs, and lease mismatches begin an unsafe-observation grace period.
- Reaping is allowed only after 120 seconds by default.
- The grace period is measured from the first unsafe ownership observation, not from container creation time.
- The container is inspected again immediately before deletion.
- Labels must still match the exact requested resource fence.
- Repeated deletion returns an already-absent success outcome.

Kubernetes cleanup is deliberately not implemented in this part.

## Security Model

### Connection Requirements

Production NATS endpoints must:

- Use **tls://** or **wss://**.
- Use an NKey seed or JWT credentials file.
- Avoid credentials embedded in the URL.
- Give the API, scheduler, and each worker distinct identities.

Plaintext NATS is accepted only for loopback development.

Scheduler and API secret-bearing configuration values redact their debug output.

### Subject Authorization

The provided production NATS policy follows least privilege:

- The API can publish **jobs.queued** but cannot consume work or manage streams.
- The scheduler can consume its two durable consumers, perform only the required JetStream stream and consumer API operations, ACK only those consumers, publish canonical redrives and DLQ events, dispatch to workers, and answer scoped worker inboxes.
- Each worker can subscribe only to its own dispatch subject and inbox.
- Each worker can publish only its own registration, heartbeat, transition, and result subjects.
- Workers cannot publish queue or DLQ messages.
- Workers cannot manage JetStream.
- Workers cannot issue raw JetStream acknowledgements.

The scheduler policy uses explicit API subjects rather than the broad **$JS.API.>** permission.

### Storage Security

The deployment example:

- Pins NATS **2.10.26-alpine**.
- Enables mutual TLS verification.
- Uses public NKey identities in server configuration.
- Enables JetStream **chachapoly** encryption.
- Reads the encryption key from **JS_KEY**.
- Stores JetStream data on an externally managed encrypted volume.
- Keeps the monitoring endpoint on loopback.
- Uses a read-only container filesystem, no-new-privileges, and a tmpfs temporary directory.

Production operators must replace all example identities, certificates, and volume names before deployment.

## Configuration

### NATS and Scheduler Security

| Variable | Default | Notes |
| --- | --- | --- |
| **NATS_URL** | **nats://127.0.0.1:4222** | Remote endpoints require TLS |
| **NATS_CREDENTIALS_FILE** | Empty | JWT and NKey credentials file |
| **NATS_NKEY_SEED_FILE** | Empty | Alternative raw seed file |
| **NATS_TLS_CA_FILE** | Empty | Optional custom CA |
| **NATS_TLS_CLIENT_CERT_FILE** | Empty | Must be paired with the client key |
| **NATS_TLS_CLIENT_KEY_FILE** | Empty | Must be paired with the client certificate |
| **SCHEDULER_METRICS_BIND_ADDR** | **127.0.0.1:9091** | Must remain loopback |

### Topology and Retention

| Variable | Default |
| --- | --- |
| **SCHEDULER_JOB_STREAM_MAX_BYTES** | 10737418240 |
| **SCHEDULER_DLQ_STREAM_MAX_BYTES** | 2147483648 |
| **SCHEDULER_ADVISORY_STREAM_MAX_BYTES** | 268435456 |
| **SCHEDULER_STREAM_REPLICAS** | 1 |
| **SCHEDULER_DLQ_RETENTION_SECONDS** | 2592000 |
| **SCHEDULER_ADVISORY_RETENTION_SECONDS** | 2592000 |
| **SCHEDULER_PULL_BATCH_SIZE** | 64 |
| **SCHEDULER_MAX_ACK_PENDING** | 256 |

### Timing and Attempts

| Variable | Default |
| --- | --- |
| **SCHEDULER_WORKER_HEARTBEAT_SECONDS** | 15 |
| **SCHEDULER_STALE_WORKER_SECONDS** | 45 |
| **SCHEDULER_ACK_WAIT_SECONDS** | 60 |
| **SCHEDULER_DATABASE_LEASE_SECONDS** | 75 |
| **SCHEDULER_MAX_RUN_ATTEMPTS** | 3 |
| **SCHEDULER_RECONCILE_INTERVAL_SECONDS** | 10 |
| **SCHEDULER_REAP_GRACE_SECONDS** | 120 |

Startup rejects timing values unless:

**heartbeat < stale-worker threshold < JetStream ACK wait < database lease TTL**

Durations are also checked for positivity and safe conversion to PostgreSQL/Chrono durations.

### Fairness and Reaper

| Variable | Default |
| --- | --- |
| **SCHEDULER_FAIRNESS_BUFFER_SIZE** | 1024 |
| **SCHEDULER_INTERACTIVE_WEIGHT** | 3 |
| **SCHEDULER_BATCH_WEIGHT** | 1 |
| **SCHEDULER_BATCH_AGING_SECONDS** | 300 |
| **SCHEDULER_DOCKER_ENDPOINT** | Empty, which disables the Docker adapter |
| **SCHEDULER_DOCKER_TIMEOUT_SECONDS** | 10 |

Docker endpoint validation rejects plaintext non-loopback TCP endpoints.

## Observability

The scheduler exposes a Prometheus-compatible loopback metrics endpoint.

| Metric | Meaning |
| --- | --- |
| **raa_scheduler_queue_depth{state="pending"}** | Pending durable deliveries |
| **raa_scheduler_queue_depth{state="ack_pending"}** | Delivered but unacknowledged work |
| **raa_scheduler_claim_latency_seconds** | Database claim latency histogram |
| **raa_scheduler_redeliveries_total** | JetStream redeliveries observed |
| **raa_scheduler_dlq_total{reason}** | Newly published dead letters by reason |
| **raa_scheduler_quota_blocked_total** | Project concurrency blocks |
| **raa_scheduler_reconciler_recoveries_total{result}** | Reconciliation outcomes |
| **raa_scheduler_reaper_total{result}** | Reaper outcomes |
| **raa_scheduler_pending_dispatches{lane}** | Buffered work by interactive or batch lane |

Only newly published DLQ events increment the DLQ counter; idempotent replays do not double-count.

The scheduler:

- Extracts the API's W3C **traceparent**, **tracestate**, and **baggage** headers from **jobs.queued**.
- Enforces a strict allowlist and size limit for propagated trace headers.
- Injects trace headers into worker dispatch requests and DLQ events.
- Correlates logs using job ID, worker ID, and lease ID.
- Does not log queue payloads, credentials, or NKey seeds.

## Public Contract and OpenAPI Changes

### Project Response

The public project representation adds:

- **max_concurrent_jobs**.
- **max_outstanding_jobs**.

This is an additive response change.

### Internal Worker Contracts

The shared contracts crate adds:

- Identifier-only **JobQueued** serialization.
- Legacy **JobQueued** decoding compatibility.
- **JobDispatch**.
- Fenced state-transition requests.
- Typed control responses.
- Sanitized **JobDeadLetter** construction and stable idempotency keys.

No public priority field is added.

OpenAPI parity tests verify that Rust contracts and **openapi/v1.yaml** remain aligned.

## Main Files Changed

### Scheduler Service

| File | Responsibility |
| --- | --- |
| **crates/scheduler/src/service.rs** | Startup, metrics listener, leadership lifecycle, task orchestration, and shutdown |
| **crates/scheduler/src/leader.rs** | Session-level PostgreSQL advisory-lock leadership |
| **crates/scheduler/src/topology.rs** | Stream and consumer definitions, compatibility checks, and safe reconciliation |
| **crates/scheduler/src/dispatcher.rs** | Pull ingestion, canonical validation, fairness, claim, dispatch, ACK decisions, and redelivery rebind |
| **crates/scheduler/src/control.rs** | Worker registration, heartbeat, transition, result handling, and reply fencing |
| **crates/scheduler/src/acknowledgements.rs** | Exact local job, worker, lease, and delivery bindings |
| **crates/scheduler/src/advisory.rs** | Durable maximum-delivery and terminated-message recovery |
| **crates/scheduler/src/dlq.rs** | PostgreSQL outbox-backed DLQ publication and source retirement |
| **crates/scheduler/src/reconciler.rs** | Stale lease, finalizer, debug session, ACK, DLQ, and runtime reconciliation |
| **crates/scheduler/src/reaper.rs** | RuntimeReaper abstraction and Docker Engine implementation |
| **crates/scheduler/src/fairness.rs** | Weighted lanes, project round-robin, FIFO, and batch aging |
| **crates/scheduler/src/metrics.rs** | Scheduler metric registry and loopback HTTP endpoint |
| **crates/scheduler/src/config.rs** | Validated scheduler, topology, fairness, security, timing, and reaper settings |
| **crates/scheduler/src/security.rs** | Least-privilege subject constants and policy parity checks |
| **crates/scheduler/src/subjects.rs** | Subject construction and strict trace-header propagation |
| **crates/scheduler/src/nats_connection.rs** | TLS and NKey/JWT-aware connection construction |

### API, Contracts, and Repository

| Area | Main changes |
| --- | --- |
| **crates/api** | Secure NATS configuration/connection, quota error mapping, and identifier-only queue publication |
| **crates/contracts** | Project quotas, queue wire format, worker dispatch/control types, and dead-letter contracts |
| **crates/repository** | Atomic quota admission, atomic claims, matcher enforcement, lease fencing, stale recovery, finalization, DLQ outbox, and debug-session reconciliation |
| **openapi/v1.yaml** | Public project quota fields and identifier-only queue schema parity |

### Migration and Deployment

| File | Purpose |
| --- | --- |
| **migrations/0008_project_job_quotas.up.sql** | Quota columns, constraints, and partial indexes |
| **migrations/0008_project_job_quotas.down.sql** | Reversible migration rollback |
| **deploy/compose/nats-server.production.conf.example** | Production NATS TLS, NKey, permissions, JetStream, and encryption policy |
| **deploy/compose/nats-production.compose.yaml** | Hardened pinned NATS deployment example |
| **deploy/compose/NATS_PRODUCTION.md** | Operator instructions and worker-identity expansion guidance |
| **.env.example** | Scheduler defaults, credentials, TLS, metrics, and encrypted-storage warning |
| **justfile** | Scheduler build, test, integration, run, and opt-in Docker reaper commands |
| **.github/workflows/ci.yml** | PostgreSQL 16, pinned NATS 2.10.26, secure NATS, live scheduler variables, and integration coverage |

## Test Coverage

### Unit Tests

Coverage includes:

- Topology defaults, invariants, editable drift, and incompatible drift.
- Timing and security configuration validation.
- Identifier-only and legacy queue decoding.
- Canonical delivery decisions.
- Worker matcher exclusions and deterministic ordering.
- Three-to-one lane weighting.
- Project round-robin and per-project FIFO.
- Batch aging.
- Duplicate and full fairness-buffer behavior.
- ACK binding replacement and race safety.
- DLQ sanitization, idempotency, and metrics.
- Reply-subject scope validation.
- Reaper label validation, lease safety, grace behavior, and idempotent deletion.
- Metrics rendering.
- Production and test NATS permission parity.

### SQLx/PostgreSQL Integration Tests

Coverage includes:

- Concurrent outstanding quota admission.
- Idempotent replay while the project is at quota.
- Atomic project concurrency claim races.
- No attempt increment on quota block.
- Exact lease fencing and late-worker rejection.
- Configurable worker freshness enforcement.
- Stale lease recovery and capacity release.
- Cancellation-preserving stale recovery.
- Legal artifact and cleanup finalizer transitions.
- Durable DLQ outbox insertion.
- Exactly-once expired debug-session ending and audit.
- Quota migration columns, constraints, indexes, and reversibility.

### Live NATS/PostgreSQL Tests

The gated live tests cover:

- Idempotent topology provisioning.
- Safe editable topology repair.
- Failure on incompatible subject drift.
- Scheduler stop and restart before final ACK.
- Redelivery rebinding to an existing valid lease without a second attempt.
- Heartbeats extending PostgreSQL and JetStream beyond the original ACK wait.
- Worker A becoming stale and the job reaching worker B.
- Late worker A result rejection.
- Terminal persistence before source removal.
- Project concurrency saturation without attempt or DLQ consumption.
- No matching spare capacity without attempt or DLQ consumption.
- Three abandoned claims producing exactly one dead letter.
- Durable exhaustion replay idempotency.
- Source message deletion after confirmed DLQ handling.
- Maximum-delivery advisory recovery.
- Scoped test cleanup.

The live test binaries share production JetStream resource names. A dedicated PostgreSQL session advisory lock serializes their topology use across separate Cargo integration-test processes.

### NATS Security Integration

The secure NATS test proves:

- API publish is allowed and API consumption/management is denied.
- Scheduler can provision only the Part 04 topology.
- Scheduler cannot create an unrelated stream.
- Worker alpha cannot consume worker beta dispatch.
- Workers cannot publish queue or DLQ messages.
- Workers cannot manage JetStream.
- Workers cannot issue raw ACKs.
- Scheduler can ACK only the configured durable consumers.
- Valid scoped control replies work.
- Malicious cross-worker inbox replies are denied.

### Runtime Reaper Integration

- The normal CI path uses the fake in-memory reaper.
- An opt-in Docker Engine test creates a labeled container and verifies exact fenced removal.
- The Docker probe requires **RUN_DOCKER_REAPER_INTEGRATION=true** and a locally available test image.

## Validation Results

The following local validation completed successfully:

    cargo fmt --all -- --check
    cargo check --workspace --all-targets --all-features --offline
    cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
    cargo test -p run-anywhere-scheduler -p run-anywhere-contracts --all-features --offline
    cargo test --workspace --lib --bins --all-features --offline
    cargo test --workspace --all-features --offline --no-run
    npm.cmd run openapi:lint
    rustup run 1.85.0 cargo check --workspace --all-targets --all-features --offline
    git diff --check

Observed results:

- 42 scheduler tests passed.
- 33 contract tests passed.
- 3 OpenAPI parity tests passed.
- 107 workspace library and binary tests passed.
- Every workspace test executable compiled.
- Clippy passed with warnings denied.
- OpenAPI lint reported a valid API description.
- Rust 1.85.0 MSRV check passed.
- The two protected architecture documents have no diff.

### Local Environment Limitation

Authenticated PostgreSQL/NATS services and a Docker daemon were not available in the local execution environment. Therefore:

- The service-dependent live bodies were not executed locally.
- Their test executables compiled successfully.
- Their default gated entrypoints returned successfully.
- CI is configured with PostgreSQL 16, pinned NATS 2.10.26, a separate secured NATS server, and the required live-test environment flags.
- The opt-in real Docker reaper test remains operator-triggered.

The production Compose file was configuration-rendered with representative environment values. A direct local **nats-server -t** check was unavailable because the server binary and Docker daemon were not running.

## Backward Compatibility

| Area | Compatibility |
| --- | --- |
| Public Project response | Additive quota fields |
| Queue messages | New serialization is identifier-only; legacy full-profile messages remain readable |
| Existing project rows | Receive safe defaults of 2 concurrent and 100 outstanding jobs |
| Existing API idempotency | Preserved before quota validation |
| Existing job states | Reuses existing transition guard and finalizer states |
| Existing worker contracts | Existing registration, heartbeat, claim, result, and lease types are retained |
| Queue backend | JetStream remains the only supported backend |

## Operational Behavior and Failure Recovery

| Failure point | Recovery behavior |
| --- | --- |
| Scheduler exits before claim | JetStream redelivers; no database ownership exists |
| Scheduler exits after claim | Redelivery rebinds to the exact valid database lease |
| Worker stops heartbeating | Reconciler CAS-requeues below the attempt limit and releases capacity |
| Late old-worker result | Rejected by worker and lease fence |
| Scheduler exits before DLQ publish | PostgreSQL outbox remains pending and idempotently publishable |
| Scheduler exits after DLQ publish | Stable message ID and published marker prevent duplicate semantic DLQ records |
| Scheduler exits after terminal commit but before ACK | Redelivery sees terminal canonical state and ACKs safely |
| Project concurrency is full | Delivery stays alive with progress; attempts do not increase |
| No compatible worker exists | Delivery stays pending with progress; attempts do not increase |
| Reaper cannot establish ownership | Destructive cleanup fails closed |
| Leadership database session is lost | Leader work stops and another instance may acquire the lock |

## Deployment and Rollout Plan

1. Review and apply migration **0008_project_job_quotas**.
2. Confirm every project has acceptable default quota values or apply project-specific overrides.
3. Provision an encrypted JetStream volume.
4. Configure NATS TLS certificates, public NKeys, and a strong **JS_KEY**.
5. Give the API and scheduler separate scoped credentials.
6. Create a distinct worker identity and permission block for every worker.
7. Set scheduler timing so the required strict ordering is preserved.
8. Start NATS and verify monitoring reports JetStream enabled.
9. Start two or more scheduler instances if high availability is required.
10. Confirm exactly one instance reports leadership.
11. Verify **/metrics** on the loopback scheduler metrics listener.
12. Confirm the scheduler reports compatible topology.
13. Observe queue pending, ACK-pending, quota, redelivery, DLQ, and recovery metrics.
14. Run the live acceptance suite in a dedicated integration environment before production promotion.
15. When Part 5 is introduced, ensure every runtime receives the required managed, job, worker, lease, and runtime-kind labels.

Until Part 5 workers are deployed, the scheduler can provision and reconcile the control plane, but runnable work will remain pending because no worker execution loop accepts dispatches.

## Rollback Plan

1. Stop scheduler instances so no new claims or reconciliation mutations occur.
2. Roll back the application binaries to a version that does not require the new quota columns.
3. Run the down migration only after confirming the old binaries are active.
4. Keep JetStream streams in place during rollback unless operators have separately backed up and intentionally decided to delete durable queue data.
5. Restore the previous NATS policy only after confirming no Part 04 scheduler or worker connection still depends on it.

Topology provisioning intentionally does not auto-delete streams during rollback because that would destroy durable work and DLQ history.

## Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Duplicate worker ownership after crash | Canonical PostgreSQL lease plus exact worker/lease fencing and redelivery rebind |
| Attempts consumed while waiting for capacity | Periodic progress and attempts incremented only by successful atomic claim |
| Cross-project starvation | Project round-robin within weighted lanes |
| Batch starvation | Five-minute aging promotion |
| Project quota race | Project-row serialization in admission and claim transactions |
| Worker capability changes between selection and claim | Matcher repeated under row locks |
| Duplicate DLQ records | PostgreSQL outbox, stable event key, confirmed publish, and durable published marker |
| Maximum-delivery message stranded in source stream | Durable advisory stream and explicit raw-message recovery |
| Worker-controlled reply exfiltration | Exact worker inbox-prefix validation plus server authorization |
| Scheduler over-privilege | Explicit JetStream API and consumer-specific ACK permissions |
| Old runtime deleted by age alone | Exact ownership check and grace measured from unsafe observation |
| Unknown database state during cleanup | Fail-closed reaper decision |
| Concurrent live tests mutate the same durable topology | Shared PostgreSQL advisory test lock |

## Explicitly Out of Scope

- Worker runtime execution loop.
- APK download or execution behavior.
- Kubernetes runtime deletion.
- Warm pools.
- Redis queue support.
- Public scheduling priority.
- Public worker control endpoints.
- Kubernetes or cloud-specific autoscaling.

## Reviewer Guide

Recommended review order:

1. Migration and repository transaction behavior.
2. Shared queue and worker contracts.
3. Topology compatibility and security permissions.
4. Dispatcher canonical decisions and fairness.
5. Control-plane lease fencing and reply-subject protection.
6. ACK registry and terminal ordering.
7. DLQ and advisory crash recovery.
8. Reconciler state transitions.
9. Reaper destructive-operation safety.
10. Configuration, metrics, deployment templates, and CI.
11. Live acceptance tests and cleanup guarantees.

### Reviewer Checklist

- [ ] Queue payloads contain identifiers and scheduling scalars only.
- [ ] Project admission remains idempotent before quota checks.
- [ ] Claim lock order is job, project, then worker.
- [ ] Attempt count changes only on successful claim.
- [ ] Worker compatibility is checked again atomically.
- [ ] Workers never receive raw JetStream ACK subjects.
- [ ] Result persistence happens before confirmed ACK.
- [ ] Existing valid leases rebind on redelivery.
- [ ] Cancellations cannot be requeued as runnable work.
- [ ] DLQ publication is durable, sanitized, and idempotent.
- [ ] Source messages are deleted only after durable DLQ confirmation.
- [ ] Leadership loss stops consumption and reconciliation.
- [ ] Reaper never deletes an exact active lease.
- [ ] Unknown reaper ownership fails closed.
- [ ] NATS permissions match the intended API, scheduler, and per-worker boundary.
- [ ] Metrics remain on loopback.
- [ ] Migration down behavior is acceptable.
- [ ] Architecture and Part 04 specification documents remain unchanged.
- [ ] Part 5 and Part 11 work did not enter this change.

## Commit and Push Instructions

Create the feature branch:

    git switch -c codex/part-04-queue-scheduler-reconciler

Review the complete change:

    git status --short
    git diff --check
    git diff --stat

Stage the Part 04 implementation:

    git add -- .env.example .github/workflows/ci.yml Cargo.lock justfile openapi/v1.yaml
    git add -- crates/api crates/contracts crates/repository crates/scheduler
    git add -- migrations/0008_project_job_quotas.up.sql migrations/0008_project_job_quotas.down.sql
    git add -- deploy/compose
    git add -- docs/Parts/PART_04_IMPLEMENTATION_PR.md

Inspect the staged change:

    git diff --cached --check
    git diff --cached --stat

Create the commit:

    git commit -m "feat(scheduler): implement Part 04 queue, scheduler, and reconciler"

Push the branch:

    git push -u origin codex/part-04-queue-scheduler-reconciler

## Ready-to-Use PR Description

### Summary

Implements Part 04 as an active-passive PostgreSQL-led scheduler with durable JetStream queue consumption, quota-aware fair dispatch, exact worker/lease fencing, scheduler-owned acknowledgements, durable DLQ/advisory recovery, stale reconciliation, Compose runtime cleanup, secure NATS deployment templates, metrics, and comprehensive test coverage.

### Key Changes

- Provisions and reconciles **JOB_QUEUE**, **JOB_DLQ**, **JOB_ADVISORIES**, and their durable pull consumers.
- Adds project outstanding and concurrent quotas with atomic PostgreSQL enforcement.
- Adds internal three-to-one interactive/batch fairness, project round-robin, FIFO, and batch aging.
- Adds deterministic worker matching and repeats every capability check inside the claim transaction.
- Adds secure worker dispatch and fenced control subjects.
- Keeps raw JetStream ACK authority exclusively in the scheduler.
- Adds crash-safe redelivery rebind, progress renewal, stale requeue, and finalizer behavior.
- Adds sanitized PostgreSQL-outbox-backed DLQ publication and advisory recovery.
- Adds debug-session reconciliation and Docker Engine runtime reaping with strict lease safety.
- Adds scheduler metrics, configuration validation, production NATS templates, and CI coverage.
- Keeps the architecture and Part 04 specification documents unchanged.

### Validation

- Formatting passed.
- Clippy passed with warnings denied.
- Workspace checks and all test targets compiled.
- 42 scheduler tests passed.
- 33 contract tests and 3 OpenAPI parity tests passed.
- 107 workspace library/binary tests passed.
- OpenAPI lint passed.
- Rust 1.85.0 MSRV check passed.

### Live-Test Note

The local environment did not provide authenticated PostgreSQL/NATS services or a Docker daemon, so service-dependent live bodies were not run locally. They compile successfully and CI is configured to run PostgreSQL/NATS acceptance and authorization tests. The real Docker cleanup probe remains intentionally opt-in.

### Scope

This PR does not implement the Part 5 worker loop, Kubernetes deletion, warm pools, Redis, or a public priority field.

## References

- [Enhanced Architecture](../Architecture/ENHANCED_ARCHITECTURE.md)
- [Part 04 Specification](PART_04_QUEUE_SCHEDULER_RECONCILER.md)
- [NATS JetStream Consumers](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers)
- [NATS Stream Retention](https://docs.nats.io/nats-concepts/jetstream/streams)
- [NATS Security](https://docs.nats.io/nats-concepts/security)
- [NATS Subject Authorization](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/authorization)
- [NATS JetStream Encryption at Rest](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/encryption_at_rest)
- [PostgreSQL Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
