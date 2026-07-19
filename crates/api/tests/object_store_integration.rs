use std::{env, time::Duration};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use reqwest::header::{HeaderName, HeaderValue};
use run_anywhere_api::object_store::{ObjectStore, S3ObjectStore};
use run_anywhere_contracts::Sha256;
use sha2::{Digest as _, Sha256 as Sha256Hasher};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn signed_put_head_and_get_round_trip_against_minio() -> TestResult {
    if env::var("RUN_OBJECT_STORE_INTEGRATION").as_deref() != Ok("true") {
        eprintln!("skipping MinIO round-trip; RUN_OBJECT_STORE_INTEGRATION is not true");
        return Ok(());
    }

    let endpoint = required("S3_ENDPOINT")?;
    let region = required("S3_REGION")?;
    let bucket = required("S3_BUCKET")?;
    let access_key = required("S3_ACCESS_KEY_ID")?;
    let secret_key = required("S3_SECRET_ACCESS_KEY")?;
    let credentials =
        Credentials::new(access_key, secret_key, None, None, "part-three-integration");
    let sdk_config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new(region))
        .credentials_provider(credentials)
        .force_path_style(true)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(sdk_config);
    if s3_client
        .head_bucket()
        .bucket(&bucket)
        .send()
        .await
        .is_err()
    {
        s3_client.create_bucket().bucket(&bucket).send().await?;
    }
    let store = S3ObjectStore::new(
        s3_client.clone(),
        bucket.clone(),
        Duration::from_secs(60),
        Duration::from_secs(60),
    )?;

    let content = b"Run Android Anywhere Part 3 signed upload";
    let sha256 = Sha256::new(format!("{:x}", Sha256Hasher::digest(content)))?;
    let key = format!(
        "projects/proj_integration/uploads/apk/{}",
        Uuid::new_v4().simple()
    );
    let signed = store
        .presign_upload(
            &key,
            "application/vnd.android.package-archive",
            content.len() as u64,
            &sha256,
        )
        .await?;
    let client = reqwest::Client::new();
    let mut upload = client.put(signed.url.as_str()).body(content.to_vec());
    for (name, value) in signed.required_headers {
        upload = upload.header(
            HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(&value)?,
        );
    }
    upload.send().await?.error_for_status()?;

    store
        .verify_upload(&key, content.len() as u64, &sha256)
        .await?;
    let download = store.presign_download(&key).await?;
    let downloaded = client
        .get(download.url.as_str())
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    assert_eq!(downloaded.as_ref(), content);
    s3_client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    Ok(())
}

fn required(name: &'static str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    env::var(name).map_err(|_| format!("{name} is required for MinIO integration").into())
}
