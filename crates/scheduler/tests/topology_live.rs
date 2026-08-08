mod common;

use std::{env, error::Error};

use run_anywhere_scheduler::{
    Config, TopologyConfig, TopologyError, connect_nats, desired_job_consumer, desired_job_stream,
    provision_topology,
    subjects::{
        JOB_ADVISORY_CONSUMER, JOB_ADVISORY_STREAM, JOB_DISPATCH_CONSUMER, JOB_DLQ_STREAM,
        JOB_QUEUE_STREAM,
    },
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn integration_enabled() -> bool {
    env::var("RUN_SCHEDULER_INTEGRATION").is_ok_and(|value| value.eq_ignore_ascii_case("true"))
}

#[tokio::test]
async fn live_topology_is_idempotent_and_reconciles_safe_editable_drift() -> TestResult {
    if !integration_enabled() {
        eprintln!("skipping live scheduler topology test; set RUN_SCHEDULER_INTEGRATION=true");
        return Ok(());
    }

    let config = Config::from_env()?;
    let _topology_lock =
        common::acquire_shared_nats_topology_lock(config.database_url.expose_secret()).await?;
    let topology_config = TopologyConfig::try_from(&config)?;
    let client = connect_nats(&config.nats).await?;
    let context = async_nats::jetstream::new(client);

    let first = provision_topology(&context, &topology_config).await?;
    assert_eq!(first.job_stream.cached_info().config.name, JOB_QUEUE_STREAM);
    assert_eq!(first.dlq_stream.cached_info().config.name, JOB_DLQ_STREAM);
    assert_eq!(
        first.advisory_stream.cached_info().config.name,
        JOB_ADVISORY_STREAM
    );
    assert_eq!(first.job_consumer.cached_info().name, JOB_DISPATCH_CONSUMER);
    assert_eq!(
        first.advisory_consumer.cached_info().name,
        JOB_ADVISORY_CONSUMER
    );

    // A second pass must discover the same durable resources without conflict.
    let second = provision_topology(&context, &topology_config).await?;
    let expected_stream = desired_job_stream(&topology_config);
    let expected_consumer = desired_job_consumer(&topology_config);
    assert_eq!(
        second.job_stream.cached_info().config.description,
        expected_stream.description
    );
    assert_eq!(
        second.job_stream.cached_info().config.max_bytes,
        expected_stream.max_bytes
    );
    assert_eq!(
        second.job_consumer.cached_info().config.description,
        expected_consumer.description
    );
    assert_eq!(
        second.job_consumer.cached_info().config.ack_wait,
        expected_consumer.ack_wait
    );

    // Description is an intentionally editable field. Drift it on both the
    // stream and consumer without changing message retention or delivery.
    let mut drifted_stream = expected_stream.clone();
    drifted_stream.description = Some("temporary scheduler integration drift".to_owned());
    context.update_stream(drifted_stream).await?;

    let job_stream = context.get_stream(JOB_QUEUE_STREAM).await?;
    let mut drifted_consumer = expected_consumer.clone();
    drifted_consumer.description = Some("temporary scheduler consumer drift".to_owned());
    job_stream.update_consumer(drifted_consumer).await?;

    // Capture the exercise result, then restore deterministically before any
    // error or assertion can leave the shared integration server drifted.
    let exercise: TestResult<(Option<String>, Option<String>)> = async {
        let repaired = provision_topology(&context, &topology_config).await?;
        Ok((
            repaired.job_stream.cached_info().config.description.clone(),
            repaired
                .job_consumer
                .cached_info()
                .config
                .description
                .clone(),
        ))
    }
    .await;

    let stream_restore = context.update_stream(expected_stream.clone()).await;
    let consumer_restore = job_stream.update_consumer(expected_consumer.clone()).await;
    stream_restore?;
    consumer_restore?;

    let (stream_description, consumer_description) = exercise?;
    assert_eq!(stream_description, expected_stream.description);
    assert_eq!(consumer_description, expected_consumer.description);

    // Subject ownership is semantic, not an editable capacity field. Add an
    // otherwise harmless extra subject, prove startup fails clearly, then
    // restore the exact production definition before asserting the result.
    let mut incompatible_stream = expected_stream.clone();
    incompatible_stream
        .subjects
        .push("jobs.incompatible-topology-test".to_owned());
    context.update_stream(incompatible_stream).await?;
    let incompatible = provision_topology(&context, &topology_config).await;
    context.update_stream(expected_stream).await?;
    assert!(matches!(
        incompatible,
        Err(TopologyError::Incompatible {
            field: "subjects",
            ..
        })
    ));

    Ok(())
}
