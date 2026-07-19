CREATE TABLE workers (
    id TEXT PRIMARY KEY
        CONSTRAINT workers_id_format CHECK (id ~ '^wrk_[A-Za-z0-9_-]+$'),
    runtimes TEXT[] NOT NULL
        CONSTRAINT workers_runtimes_valid CHECK (
            cardinality(runtimes) > 0
            AND array_ndims(runtimes) = 1
            AND array_lower(runtimes, 1) = 1
            AND array_position(runtimes, NULL) IS NULL
            AND runtimes <@ ARRAY[
                'android_emulator_container',
                'redroid',
                'cuttlefish',
                'browser_native_wasm'
            ]::TEXT[]
        ),
    kvm BOOLEAN NOT NULL,
    gpu BOOLEAN NOT NULL,
    arch TEXT NOT NULL
        CONSTRAINT workers_arch_valid CHECK (arch IN ('x86_64', 'aarch64')),
    capacity BIGINT NOT NULL
        CONSTRAINT workers_capacity_range CHECK (
            capacity BETWEEN 0 AND 4294967295
        ),
    active_jobs BIGINT NOT NULL DEFAULT 0
        CONSTRAINT workers_active_jobs_range CHECK (
            active_jobs BETWEEN 0 AND 4294967295
        ),
    state TEXT NOT NULL DEFAULT 'online'
        CONSTRAINT workers_state_valid CHECK (state IN ('online', 'draining', 'offline')),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reported_last_seen_at TIMESTAMPTZ,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT workers_active_jobs_within_capacity CHECK (active_jobs <= capacity),
    CONSTRAINT workers_updated_order CHECK (updated_at >= registered_at)
);

CREATE INDEX workers_runtimes_gin_idx ON workers USING GIN (runtimes);

CREATE INDEX workers_state_arch_heartbeat_idx
    ON workers (state, arch, last_heartbeat_at DESC);

CREATE TABLE jobs (
    id TEXT PRIMARY KEY
        CONSTRAINT jobs_id_format CHECK (id ~ '^job_[A-Za-z0-9_-]+$'),
    project_id TEXT NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    apk_upload_id TEXT NOT NULL,
    test_upload_id TEXT,
    runtime_profile_id TEXT NOT NULL REFERENCES runtime_profiles (id),
    worker_id TEXT REFERENCES workers (id),
    mode TEXT NOT NULL
        CONSTRAINT jobs_mode_valid CHECK (mode IN ('headless_ci', 'browser_debug')),
    min_isolation TEXT NOT NULL
        CONSTRAINT jobs_min_isolation_valid CHECK (
            min_isolation IN ('vm_isolated', 'shared_kernel_privileged')
        ),
    automation JSONB NOT NULL
        CONSTRAINT jobs_automation_valid CHECK (
            jsonb_typeof(automation) = 'object'
            AND automation ? 'type'
            AND jsonb_typeof(automation -> 'type') = 'string'
            AND automation ->> 'type' IN ('built_in_smoke', 'appium')
            AND (
                automation ->> 'type' = 'built_in_smoke'
                OR (
                    automation ? 'script_ref'
                    AND jsonb_typeof(automation -> 'script_ref') = 'string'
                    AND btrim(automation ->> 'script_ref') <> ''
                    AND automation ->> 'script_ref' !~ '[[:cntrl:]]'
                )
            )
        ),
    requested_artifacts JSONB NOT NULL
        CONSTRAINT jobs_requested_artifacts_valid CHECK (
            jsonb_typeof(requested_artifacts) = 'object'
            AND requested_artifacts ?& ARRAY['screenshots', 'video', 'logcat', 'junit']
            AND jsonb_typeof(requested_artifacts -> 'screenshots') = 'boolean'
            AND jsonb_typeof(requested_artifacts -> 'video') = 'boolean'
            AND jsonb_typeof(requested_artifacts -> 'logcat') = 'boolean'
            AND jsonb_typeof(requested_artifacts -> 'junit') = 'boolean'
        ),
    timeout_seconds BIGINT NOT NULL
        CONSTRAINT jobs_timeout_positive CHECK (timeout_seconds > 0),
    idempotency_key TEXT NOT NULL
        CONSTRAINT jobs_idempotency_key_nonempty CHECK (btrim(idempotency_key) <> ''),
    state TEXT NOT NULL DEFAULT 'queued'
        CONSTRAINT jobs_state_valid CHECK (
            state IN (
                'queued',
                'claimed',
                'provisioning_runtime',
                'booting',
                'installing_apk',
                'running_tests',
                'debug_available',
                'collecting_artifacts',
                'cleaning_up',
                'passed',
                'failed',
                'cancelled',
                'timed_out',
                'infra_failed'
            )
        ),
    pending_outcome TEXT
        CONSTRAINT jobs_pending_outcome_valid CHECK (
            pending_outcome IS NULL
            OR pending_outcome IN ('passed', 'failed', 'cancelled', 'timed_out', 'infra_failed')
        ),
    outcome TEXT
        CONSTRAINT jobs_outcome_valid CHECK (
            outcome IS NULL
            OR outcome IN ('passed', 'failed', 'cancelled', 'timed_out', 'infra_failed')
        ),
    failure JSONB
        CONSTRAINT jobs_failure_valid CHECK (
            failure IS NULL
            OR (
                jsonb_typeof(failure) = 'object'
                AND failure ?& ARRAY['code', 'message']
                AND jsonb_typeof(failure -> 'code') = 'string'
                AND failure ->> 'code' IN (
                    'validation',
                    'unauthorized',
                    'forbidden',
                    'not_found',
                    'conflict',
                    'quota_exceeded',
                    'infra_failed',
                    'internal_error'
                )
                AND jsonb_typeof(failure -> 'message') = 'string'
                AND btrim(failure ->> 'message') <> ''
            )
        ),
    artifacts_finalized BOOLEAN NOT NULL DEFAULT FALSE,
    cleanup_completed BOOLEAN NOT NULL DEFAULT FALSE,
    lease_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_lease_extended_at TIMESTAMPTZ,
    delivery_attempts BIGINT NOT NULL DEFAULT 0
        CONSTRAINT jobs_delivery_attempts_nonnegative CHECK (delivery_attempts >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CONSTRAINT jobs_project_id_idempotency_key UNIQUE (project_id, idempotency_key),
    CONSTRAINT jobs_apk_upload_project_fk
        FOREIGN KEY (project_id, apk_upload_id)
        REFERENCES uploads (project_id, id),
    CONSTRAINT jobs_test_upload_project_fk
        FOREIGN KEY (project_id, test_upload_id)
        REFERENCES uploads (project_id, id),
    CONSTRAINT jobs_distinct_uploads CHECK (
        test_upload_id IS NULL OR test_upload_id <> apk_upload_id
    ),
    CONSTRAINT jobs_lease_consistent CHECK (
        (
            lease_id IS NULL
            AND lease_expires_at IS NULL
            AND last_lease_extended_at IS NULL
        )
        OR (
            lease_id IS NOT NULL
            AND lease_id ~ '^lease_[A-Za-z0-9_-]+$'
            AND worker_id IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND last_lease_extended_at IS NOT NULL
            AND lease_expires_at > last_lease_extended_at
        )
    ),
    CONSTRAINT jobs_lease_has_worker CHECK (
        lease_id IS NULL OR worker_id IS NOT NULL
    ),
    CONSTRAINT jobs_queued_unassigned CHECK (
        state <> 'queued' OR (worker_id IS NULL AND lease_id IS NULL)
    ),
    CONSTRAINT jobs_cleanup_after_artifacts CHECK (
        NOT cleanup_completed OR artifacts_finalized
    ),
    CONSTRAINT jobs_terminal_state_consistent CHECK (
        (
            state = 'passed'
            AND pending_outcome IS NOT NULL
            AND pending_outcome = 'passed'
            AND outcome IS NOT NULL
            AND outcome = 'passed'
        )
        OR (
            state = 'failed'
            AND pending_outcome IS NOT NULL
            AND pending_outcome = 'failed'
            AND outcome IS NOT NULL
            AND outcome = 'failed'
        )
        OR (
            state = 'cancelled'
            AND pending_outcome IS NOT NULL
            AND pending_outcome = 'cancelled'
            AND outcome IS NOT NULL
            AND outcome = 'cancelled'
        )
        OR (
            state = 'timed_out'
            AND pending_outcome IS NOT NULL
            AND pending_outcome = 'timed_out'
            AND outcome IS NOT NULL
            AND outcome = 'timed_out'
        )
        OR (
            state = 'infra_failed'
            AND pending_outcome IS NOT NULL
            AND pending_outcome = 'infra_failed'
            AND outcome IS NOT NULL
            AND outcome = 'infra_failed'
        )
        OR (
            state NOT IN ('passed', 'failed', 'cancelled', 'timed_out', 'infra_failed')
            AND outcome IS NULL
            AND finished_at IS NULL
        )
    ),
    CONSTRAINT jobs_terminal_evidence_complete CHECK (
        state NOT IN ('passed', 'failed', 'cancelled', 'timed_out', 'infra_failed')
        OR (artifacts_finalized AND cleanup_completed AND finished_at IS NOT NULL)
    ),
    CONSTRAINT jobs_started_order CHECK (
        started_at IS NULL OR started_at >= created_at
    ),
    CONSTRAINT jobs_finished_order CHECK (
        finished_at IS NULL OR finished_at >= created_at
    )
);

CREATE INDEX jobs_project_created_idx
    ON jobs (project_id, created_at DESC, id DESC);

CREATE INDEX jobs_project_state_created_idx
    ON jobs (project_id, state, created_at DESC, id DESC);

CREATE INDEX jobs_queued_created_idx
    ON jobs (state, created_at, id)
    WHERE state = 'queued';

CREATE INDEX jobs_worker_state_idx
    ON jobs (worker_id, state)
    WHERE worker_id IS NOT NULL;

CREATE INDEX jobs_runtime_profile_state_idx
    ON jobs (runtime_profile_id, state);

CREATE INDEX jobs_active_lease_expiry_idx
    ON jobs (lease_expires_at, id)
    WHERE lease_id IS NOT NULL;
