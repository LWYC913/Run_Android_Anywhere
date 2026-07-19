INSERT INTO runtime_profiles (
    id,
    android_api,
    device_profile,
    abi,
    host_arch,
    runtime_kind,
    image_ref,
    isolation_tier
)
VALUES
    (
        'rtp_android_35_pixel_6_x86_64_emulator',
        35,
        'pixel_6',
        'x86_64',
        'x86_64',
        'android_emulator_container',
        'ghcr.io/lwyc913/run-android-anywhere-emulator:android-35-v1',
        'vm_isolated'
    ),
    (
        'rtp_android_34_pixel_5_x86_64_emulator',
        34,
        'pixel_5',
        'x86_64',
        'x86_64',
        'android_emulator_container',
        'ghcr.io/lwyc913/run-android-anywhere-emulator:android-34-v1',
        'vm_isolated'
    ),
    (
        'rtp_android_35_generic_arm64_redroid',
        35,
        'generic_phone',
        'arm64_v8a',
        'aarch64',
        'redroid',
        'ghcr.io/lwyc913/run-android-anywhere-redroid:android-35-arm64-v1',
        'shared_kernel_privileged'
    ),
    (
        'rtp_android_34_generic_x86_64_redroid',
        34,
        'generic_phone',
        'x86_64',
        'x86_64',
        'redroid',
        'ghcr.io/lwyc913/run-android-anywhere-redroid:android-34-x86_64-v1',
        'shared_kernel_privileged'
    )
ON CONFLICT (id) DO UPDATE SET
    android_api = EXCLUDED.android_api,
    device_profile = EXCLUDED.device_profile,
    abi = EXCLUDED.abi,
    host_arch = EXCLUDED.host_arch,
    runtime_kind = EXCLUDED.runtime_kind,
    image_ref = EXCLUDED.image_ref,
    isolation_tier = EXCLUDED.isolation_tier;
