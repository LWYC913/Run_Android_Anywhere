CREATE FUNCTION reject_append_only_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME
        USING ERRCODE = '55000';
    RETURN NULL;
END;
$$;

CREATE TABLE job_events (
    id TEXT PRIMARY KEY
        CONSTRAINT job_events_id_format CHECK (id ~ '^evt_[A-Za-z0-9_-]+$'),
    sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type TEXT NOT NULL
        CONSTRAINT job_events_event_type_nonempty CHECK (btrim(event_type) <> ''),
    state TEXT
        CONSTRAINT job_events_state_valid CHECK (
            state IS NULL
            OR state IN (
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
    payload JSONB NOT NULL DEFAULT '{}'::JSONB
        CONSTRAINT job_events_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT job_events_job_sequence_key UNIQUE (job_id, sequence)
);

CREATE TRIGGER job_events_append_only
BEFORE UPDATE OR DELETE ON job_events
FOR EACH ROW EXECUTE FUNCTION reject_append_only_mutation();

CREATE TRIGGER job_events_append_only_truncate
BEFORE TRUNCATE ON job_events
FOR EACH STATEMENT EXECUTE FUNCTION reject_append_only_mutation();

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY
        CONSTRAINT artifacts_id_format CHECK (id ~ '^art_[A-Za-z0-9_-]+$'),
    job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    kind TEXT NOT NULL
        CONSTRAINT artifacts_kind_valid CHECK (
            kind IN (
                'apk_metadata',
                'install_log',
                'logcat',
                'screenshot',
                'video',
                'appium_log',
                'junit',
                'crash_trace',
                'runtime_metrics',
                'debug_session_audit'
            )
        ),
    s3_key TEXT NOT NULL
        CONSTRAINT artifacts_s3_key_nonempty CHECK (btrim(s3_key) <> ''),
    file_name TEXT
        CONSTRAINT artifacts_file_name_nonempty CHECK (
            file_name IS NULL OR btrim(file_name) <> ''
        ),
    size_bytes BIGINT NOT NULL
        CONSTRAINT artifacts_size_nonnegative CHECK (size_bytes >= 0),
    sha256 TEXT NOT NULL
        CONSTRAINT artifacts_sha256_format CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT artifacts_job_s3_key_key UNIQUE (job_id, s3_key)
);

CREATE INDEX artifacts_job_created_idx
    ON artifacts (job_id, created_at, id);
