DELETE FROM runtime_profiles AS profile
WHERE profile.id IN (
    'rtp_android_35_pixel_6_x86_64_emulator',
    'rtp_android_34_pixel_5_x86_64_emulator',
    'rtp_android_35_generic_arm64_redroid',
    'rtp_android_34_generic_x86_64_redroid'
)
AND NOT EXISTS (
    SELECT 1
    FROM jobs
    WHERE jobs.runtime_profile_id = profile.id
);
