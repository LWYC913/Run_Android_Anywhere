-- Preserve the newest unended session if an older installation accumulated
-- more than one before the invariant was enforced.
WITH superseded_sessions AS (
    SELECT id
    FROM (
        SELECT
            id,
            row_number() OVER (
                PARTITION BY job_id
                ORDER BY created_at DESC, id DESC
            ) AS position
        FROM debug_sessions
        WHERE ended_at IS NULL
    ) AS ranked
    WHERE position > 1
)
UPDATE debug_sessions AS session
SET ended_at = GREATEST(session.created_at, CURRENT_TIMESTAMP)
FROM superseded_sessions
WHERE session.id = superseded_sessions.id;

CREATE UNIQUE INDEX debug_sessions_one_unended_per_job_idx
    ON debug_sessions (job_id)
    WHERE ended_at IS NULL;

CREATE TABLE outbox_messages (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_key TEXT NOT NULL UNIQUE
        CONSTRAINT outbox_messages_event_key_nonempty CHECK (btrim(event_key) <> ''),
    subject TEXT NOT NULL
        CONSTRAINT outbox_messages_subject_nonempty CHECK (btrim(subject) <> ''),
    payload JSONB NOT NULL
        CONSTRAINT outbox_messages_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    trace_headers JSONB NOT NULL DEFAULT '{}'::JSONB
        CONSTRAINT outbox_messages_trace_headers_object CHECK (
            jsonb_typeof(trace_headers) = 'object'
        ),
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attempts BIGINT NOT NULL DEFAULT 0
        CONSTRAINT outbox_messages_attempts_nonnegative CHECK (attempts >= 0),
    locked_by TEXT,
    locked_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT outbox_messages_lock_consistent CHECK (
        (locked_by IS NULL AND locked_at IS NULL)
        OR (locked_by IS NOT NULL AND btrim(locked_by) <> '' AND locked_at IS NOT NULL)
    ),
    CONSTRAINT outbox_messages_last_error_nonempty CHECK (
        last_error IS NULL OR btrim(last_error) <> ''
    ),
    CONSTRAINT outbox_messages_publish_order CHECK (
        published_at IS NULL OR published_at >= created_at
    )
);

CREATE INDEX outbox_messages_pending_idx
    ON outbox_messages (subject, available_at, id)
    WHERE published_at IS NULL;

CREATE INDEX outbox_messages_stale_lock_idx
    ON outbox_messages (locked_at, id)
    WHERE published_at IS NULL AND locked_at IS NOT NULL;
