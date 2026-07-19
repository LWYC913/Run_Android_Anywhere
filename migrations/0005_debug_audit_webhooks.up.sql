CREATE TABLE debug_sessions (
    id TEXT PRIMARY KEY
        CONSTRAINT debug_sessions_id_format CHECK (id ~ '^dbg_[A-Za-z0-9_-]+$'),
    job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    jti TEXT NOT NULL UNIQUE
        CONSTRAINT debug_sessions_jti_nonempty CHECK (btrim(jti) <> ''),
    created_by TEXT NOT NULL
        CONSTRAINT debug_sessions_created_by_nonempty CHECK (btrim(created_by) <> ''),
    mode TEXT NOT NULL
        CONSTRAINT debug_sessions_mode_valid CHECK (mode IN ('viewer', 'controller')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    CONSTRAINT debug_sessions_expiry_order CHECK (expires_at > created_at),
    CONSTRAINT debug_sessions_ended_order CHECK (
        ended_at IS NULL OR ended_at >= created_at
    )
);

CREATE INDEX debug_sessions_unended_expiry_idx
    ON debug_sessions (expires_at, id)
    WHERE ended_at IS NULL;

CREATE TABLE audit_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor TEXT NOT NULL
        CONSTRAINT audit_log_actor_nonempty CHECK (btrim(actor) <> ''),
    action TEXT NOT NULL
        CONSTRAINT audit_log_action_nonempty CHECK (btrim(action) <> ''),
    subject TEXT NOT NULL
        CONSTRAINT audit_log_subject_nonempty CHECK (btrim(subject) <> ''),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL DEFAULT '{}'::JSONB
        CONSTRAINT audit_log_payload_object CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX audit_log_subject_timestamp_idx
    ON audit_log (subject, timestamp DESC, id DESC);

CREATE INDEX audit_log_timestamp_idx
    ON audit_log (timestamp DESC, id DESC);

CREATE TRIGGER audit_log_append_only
BEFORE UPDATE OR DELETE ON audit_log
FOR EACH ROW EXECUTE FUNCTION reject_append_only_mutation();

CREATE TRIGGER audit_log_append_only_truncate
BEFORE TRUNCATE ON audit_log
FOR EACH STATEMENT EXECUTE FUNCTION reject_append_only_mutation();

CREATE TABLE webhooks (
    id TEXT PRIMARY KEY
        CONSTRAINT webhooks_id_format CHECK (id ~ '^wh_[A-Za-z0-9_-]+$'),
    project_id TEXT NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    url TEXT NOT NULL
        CONSTRAINT webhooks_url_valid CHECK (
            url ~ '^[A-Za-z0-9+.-]+:'
            AND url !~ '[[:space:][:cntrl:]]'
        ),
    events TEXT[] NOT NULL
        CONSTRAINT webhooks_events_valid CHECK (
            cardinality(events) = 1
            AND array_ndims(events) = 1
            AND array_lower(events, 1) = 1
            AND array_position(events, NULL) IS NULL
            AND events <@ ARRAY['job_state_changed']::TEXT[]
        ),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT webhooks_project_url_key UNIQUE (project_id, url)
);

CREATE INDEX webhooks_active_project_idx
    ON webhooks (project_id, created_at DESC, id DESC)
    WHERE active;

CREATE INDEX webhooks_active_events_gin_idx
    ON webhooks USING GIN (events)
    WHERE active;
