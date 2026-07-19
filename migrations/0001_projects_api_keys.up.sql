CREATE TABLE projects (
    id TEXT PRIMARY KEY
        CONSTRAINT projects_id_format CHECK (id ~ '^proj_[A-Za-z0-9_-]+$'),
    name TEXT NOT NULL
        CONSTRAINT projects_name_nonempty CHECK (btrim(name) <> ''),
    owner TEXT NOT NULL
        CONSTRAINT projects_owner_nonempty CHECK (btrim(owner) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE api_keys (
    id TEXT PRIMARY KEY
        CONSTRAINT api_keys_id_format CHECK (id ~ '^key_[A-Za-z0-9_-]+$'),
    project_id TEXT NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    key_hash BYTEA NOT NULL UNIQUE
        CONSTRAINT api_keys_hash_length CHECK (octet_length(key_hash) = 32),
    scopes TEXT[] NOT NULL
        CONSTRAINT api_keys_scopes_valid CHECK (
            cardinality(scopes) > 0
            AND array_ndims(scopes) = 1
            AND array_lower(scopes, 1) = 1
            AND array_position(scopes, NULL) IS NULL
            AND scopes <@ ARRAY[
                'project:read',
                'project:write',
                'debug:create',
                'admin'
            ]::TEXT[]
        ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    CONSTRAINT api_keys_last_used_order CHECK (
        last_used_at IS NULL OR last_used_at >= created_at
    ),
    CONSTRAINT api_keys_revoked_order CHECK (
        revoked_at IS NULL OR revoked_at >= created_at
    )
);

CREATE INDEX api_keys_project_revocation_idx
    ON api_keys (project_id, revoked_at);

CREATE INDEX api_keys_active_project_idx
    ON api_keys (project_id, created_at DESC)
    WHERE revoked_at IS NULL;
