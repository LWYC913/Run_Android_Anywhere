use std::{env, time::Duration};

use async_nats::{Client, ConnectOptions, jetstream};
use futures_util::StreamExt as _;
use run_anywhere_scheduler::{TopologyConfig, provision_topology};
use tokio::time::{sleep, timeout};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn connect(url: &str, user: &str, password: &str, inbox_prefix: &str) -> TestResult<Client> {
    Ok(ConnectOptions::new()
        .user_and_password(user.to_owned(), password.to_owned())
        .custom_inbox_prefix(inbox_prefix)
        .connect(url)
        .await?)
}

async fn assert_no_message(subscription: &mut async_nats::Subscriber) {
    assert!(
        timeout(Duration::from_millis(350), subscription.next())
            .await
            .is_err(),
        "an identity received a message forbidden by the Part 4 policy"
    );
}

#[tokio::test]
async fn nats_enforces_part_four_identity_permissions() -> TestResult {
    if env::var("RUN_NATS_SECURITY_INTEGRATION").as_deref() != Ok("true") {
        eprintln!("skipping NATS permission probe; RUN_NATS_SECURITY_INTEGRATION is not true");
        return Ok(());
    }

    let url = env::var("NATS_SECURITY_URL")?;
    let api = connect(&url, "api", "api-test-password", "_INBOX.api").await?;
    let scheduler = connect(
        &url,
        "scheduler",
        "scheduler-test-password",
        "_INBOX.scheduler",
    )
    .await?;
    let worker_alpha = connect(
        &url,
        "worker-alpha",
        "worker-alpha-test-password",
        "_INBOX.workers.wrk_alpha",
    )
    .await?;
    let worker_beta = connect(
        &url,
        "worker-beta",
        "worker-beta-test-password",
        "_INBOX.workers.wrk_beta",
    )
    .await?;

    let scheduler_js = jetstream::new(scheduler.clone());
    let topology = provision_topology(
        &scheduler_js,
        &TopologyConfig {
            job_stream_max_bytes: 64 * 1024 * 1024,
            dlq_stream_max_bytes: 32 * 1024 * 1024,
            advisory_stream_max_bytes: 8 * 1024 * 1024,
            ..TopologyConfig::default()
        },
    )
    .await?;
    let mut queue_stream = topology.job_stream;
    let mut dlq_stream = topology.dlq_stream;
    let mut consumer = topology.job_consumer;

    // The scheduler's management grant is confined to Part 4 resources.
    // Provisioning above proves its request/reply inbox works, so any failure
    // here is an authorization boundary rather than a broken client.
    let unrelated_management = timeout(
        Duration::from_millis(500),
        scheduler_js.create_stream(jetstream::stream::Config {
            name: "UNRELATED_SECURITY_STREAM".to_owned(),
            subjects: vec!["unrelated.security".to_owned()],
            ..Default::default()
        }),
    )
    .await;
    assert!(!matches!(unrelated_management, Ok(Ok(_))));

    // Only the scheduler may manage JetStream resources. The API and workers
    // can create response inboxes, but their $JS.API publishes are denied.
    let api_management = timeout(
        Duration::from_millis(500),
        jetstream::new(api.clone()).get_stream("JOB_QUEUE"),
    )
    .await;
    assert!(!matches!(api_management, Ok(Ok(_))));
    let worker_management = timeout(
        Duration::from_millis(500),
        jetstream::new(worker_alpha.clone()).get_stream("JOB_QUEUE"),
    )
    .await;
    assert!(!matches!(worker_management, Ok(Ok(_))));

    // The API can produce work, while workers cannot publish queue or DLQ
    // records. The scheduler retains its explicit DLQ publish grant.
    jetstream::new(api.clone())
        .publish("jobs.queued", "api-work".into())
        .await?
        .await?;
    worker_alpha
        .publish("jobs.queued", "forbidden-worker-work".into())
        .await?;
    worker_alpha
        .publish("jobs.dead", "forbidden-worker-dlq".into())
        .await?;
    worker_alpha.flush().await?;
    scheduler_js
        .publish("jobs.dead", "scheduler-dlq".into())
        .await?
        .await?;
    assert_eq!(queue_stream.info().await?.state.messages, 1);
    assert_eq!(dlq_stream.info().await?.state.messages, 1);

    // The API cannot consume work, and a worker cannot consume another
    // worker's dispatch subject.
    let mut api_queue = api.subscribe("jobs.queued").await?;
    let mut alpha_dispatch = worker_alpha.subscribe("workers.wrk_alpha.dispatch").await?;
    let mut beta_on_alpha = worker_beta.subscribe("workers.wrk_alpha.dispatch").await?;
    api.flush().await?;
    worker_alpha.flush().await?;
    worker_beta.flush().await?;
    scheduler_js
        .publish("jobs.queued", "scheduler-work".into())
        .await?
        .await?;
    scheduler
        .publish("workers.wrk_alpha.dispatch", "dispatch".into())
        .await?;
    scheduler.flush().await?;
    assert_eq!(
        timeout(Duration::from_secs(1), alpha_dispatch.next())
            .await?
            .expect("worker alpha should receive its dispatch")
            .payload,
        "dispatch"
    );
    assert_no_message(&mut api_queue).await;
    assert_no_message(&mut beta_on_alpha).await;

    // A worker can publish only its own control messages. The scheduler can
    // subscribe to the control wildcard and receives no cross-worker forgery.
    let mut registrations = scheduler.subscribe("control.workers.*.register").await?;
    scheduler.flush().await?;
    worker_alpha
        .publish("control.workers.wrk_alpha.register", "own".into())
        .await?;
    worker_alpha
        .publish("control.workers.wrk_beta.register", "forged".into())
        .await?;
    worker_alpha.flush().await?;
    assert_eq!(
        timeout(Duration::from_secs(1), registrations.next())
            .await?
            .expect("scheduler should receive the authorized registration")
            .payload,
        "own"
    );
    assert_no_message(&mut registrations).await;

    // Scheduler control replies are statically confined to the subject
    // worker's private inbox; the scheduler has no dynamic response grant.
    let safe_reply = "_INBOX.workers.wrk_alpha.control.1";
    let mut safe_replies = worker_alpha.subscribe(safe_reply).await?;
    worker_alpha.flush().await?;
    worker_alpha
        .publish_with_reply(
            "control.workers.wrk_alpha.register",
            safe_reply,
            "safe-request".into(),
        )
        .await?;
    worker_alpha.flush().await?;
    let safe_request = timeout(Duration::from_secs(1), registrations.next())
        .await?
        .expect("scheduler should receive the safe control request");
    scheduler
        .publish(
            safe_request.reply.expect("safe request has a reply"),
            "safe-response".into(),
        )
        .await?;
    scheduler.flush().await?;
    assert_eq!(
        timeout(Duration::from_secs(1), safe_replies.next())
            .await?
            .expect("worker should receive its scoped response")
            .payload,
        "safe-response"
    );

    let malicious_reply = "_INBOX.api.confused-deputy";
    let mut malicious_replies = api.subscribe(malicious_reply).await?;
    api.flush().await?;
    worker_alpha
        .publish_with_reply(
            "control.workers.wrk_alpha.register",
            malicious_reply,
            "malicious-request".into(),
        )
        .await?;
    worker_alpha.flush().await?;
    let malicious_request = timeout(Duration::from_secs(1), registrations.next())
        .await?
        .expect("scheduler should receive the malicious control request");
    scheduler
        .publish(
            malicious_request
                .reply
                .expect("malicious request has a reply"),
            "must-not-be-relayed".into(),
        )
        .await?;
    scheduler.flush().await?;
    assert_no_message(&mut malicious_replies).await;

    // A worker cannot issue a raw JetStream acknowledgement. Ack-pending
    // remains set after its attempt and clears only after the scheduler sends
    // the exact same protocol acknowledgement.
    let mut messages = consumer.messages().await?;
    let message = timeout(Duration::from_secs(1), messages.next())
        .await?
        .expect("the secured queue should contain work")?;
    let ack_subject = message
        .reply
        .clone()
        .expect("JetStream delivery must carry an ack subject");
    worker_alpha
        .publish(ack_subject.clone(), "+ACK".into())
        .await?;
    worker_alpha.flush().await?;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(consumer.info().await?.num_ack_pending, 1);

    scheduler.publish(ack_subject, "+ACK".into()).await?;
    scheduler.flush().await?;
    timeout(Duration::from_secs(1), async {
        loop {
            if consumer
                .info()
                .await
                .expect("consumer info")
                .num_ack_pending
                == 0
            {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;

    Ok(())
}
