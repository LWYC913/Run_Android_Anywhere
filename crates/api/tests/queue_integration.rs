use std::{collections::BTreeMap, env, time::Duration};

use async_nats::jetstream::stream;
use run_anywhere_api::queue::{JetStreamPublisher, JobQueuePublisher};
use run_anywhere_contracts::{
    AndroidAbi, DurationSeconds, HostArch, IsolationTier, JobId, JobQueued, ProjectId, RuntimeKind,
    RuntimeProfile, RuntimeProfileId,
};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn jetstream_publisher_uses_the_job_id_for_deduplication() -> TestResult {
    if env::var("RUN_QUEUE_INTEGRATION").as_deref() != Ok("true") {
        eprintln!("skipping NATS round-trip; RUN_QUEUE_INTEGRATION is not true");
        return Ok(());
    }

    let client = async_nats::connect(env::var("NATS_URL")?).await?;
    let context = async_nats::jetstream::new(client);
    let suffix = Uuid::new_v4().simple().to_string();
    let stream_name = format!("PART3_{suffix}");
    let subject = format!("part3.jobs.{suffix}");
    let mut stream = context
        .create_stream(stream::Config {
            name: stream_name.clone(),
            subjects: vec![subject.clone()],
            duplicate_window: Duration::from_secs(120),
            ..stream::Config::default()
        })
        .await?;
    let publisher = JetStreamPublisher::with_subject(context.clone(), subject)?;
    let message = JobQueued {
        job_id: JobId::new(format!("job_{suffix}"))?,
        project_id: ProjectId::new("proj_queue_integration")?,
        runtime_profile: RuntimeProfile {
            id: RuntimeProfileId::new("rtp_queue_integration")?,
            android_api: 35,
            device_profile: "pixel_6".to_owned(),
            abi: AndroidAbi::X86_64,
            host_arch: HostArch::X86_64,
            runtime_kind: RuntimeKind::AndroidEmulatorContainer,
            image_ref: "registry.example.test/android:35".to_owned(),
            isolation_tier: IsolationTier::VmIsolated,
        },
        min_isolation: IsolationTier::VmIsolated,
        timeout_seconds: DurationSeconds::new(300)?,
    };

    publisher
        .publish_job_queued(&message, &BTreeMap::new())
        .await?;
    publisher
        .publish_job_queued(&message, &BTreeMap::new())
        .await?;
    let message_count = stream.info().await?.state.messages;
    context.delete_stream(&stream_name).await?;
    assert_eq!(message_count, 1);
    Ok(())
}
