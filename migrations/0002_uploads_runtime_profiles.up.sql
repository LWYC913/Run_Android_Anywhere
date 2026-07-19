CREATE TABLE uploads (
    id TEXT PRIMARY KEY
        CONSTRAINT uploads_id_format CHECK (id ~ '^upl_[A-Za-z0-9_-]+$'),
    project_id TEXT NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    kind TEXT NOT NULL
        CONSTRAINT uploads_kind_valid CHECK (kind IN ('apk', 'test', 'script')),
    s3_key TEXT NOT NULL UNIQUE
        CONSTRAINT uploads_s3_key_nonempty CHECK (btrim(s3_key) <> ''),
    sha256 TEXT NOT NULL
        CONSTRAINT uploads_sha256_format CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    size_bytes BIGINT NOT NULL
        CONSTRAINT uploads_size_nonnegative CHECK (size_bytes >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uploads_project_id_id_key UNIQUE (project_id, id)
);

CREATE INDEX uploads_project_created_idx
    ON uploads (project_id, created_at DESC, id DESC);

CREATE TABLE runtime_profiles (
    id TEXT PRIMARY KEY
        CONSTRAINT runtime_profiles_id_format CHECK (id ~ '^rtp_[A-Za-z0-9_-]+$'),
    android_api INTEGER NOT NULL
        CONSTRAINT runtime_profiles_android_api_valid CHECK (
            android_api BETWEEN 0 AND 65535
        ),
    device_profile TEXT NOT NULL
        CONSTRAINT runtime_profiles_device_profile_nonempty CHECK (
            btrim(device_profile) <> ''
        ),
    abi TEXT NOT NULL
        CONSTRAINT runtime_profiles_abi_valid CHECK (
            abi IN ('x86', 'x86_64', 'armeabi_v7a', 'arm64_v8a')
        ),
    host_arch TEXT NOT NULL
        CONSTRAINT runtime_profiles_host_arch_valid CHECK (
            host_arch IN ('x86_64', 'aarch64')
        ),
    runtime_kind TEXT NOT NULL
        CONSTRAINT runtime_profiles_runtime_kind_valid CHECK (
            runtime_kind IN (
                'android_emulator_container',
                'redroid',
                'cuttlefish',
                'browser_native_wasm'
            )
        ),
    image_ref TEXT NOT NULL
        CONSTRAINT runtime_profiles_image_ref_nonempty CHECK (btrim(image_ref) <> ''),
    isolation_tier TEXT NOT NULL
        CONSTRAINT runtime_profiles_isolation_tier_valid CHECK (
            isolation_tier IN ('vm_isolated', 'shared_kernel_privileged')
        ),
    CONSTRAINT runtime_profiles_natural_key UNIQUE (
        android_api,
        device_profile,
        abi,
        host_arch,
        runtime_kind,
        isolation_tier
    )
);

CREATE INDEX runtime_profiles_matcher_idx
    ON runtime_profiles (runtime_kind, host_arch, abi, isolation_tier);
