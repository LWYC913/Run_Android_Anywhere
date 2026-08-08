//! Least-privilege NATS authorization policy for Part 4 identities.
//!
//! These values are intended to be rendered into NATS account/user
//! permissions by deployment tooling. Workers use NATS' `allow_responses`
//! facility only for dispatch replies. Scheduler control replies use an
//! explicit worker-inbox allowlist plus application-level worker scoping.

use run_anywhere_contracts::WorkerId;

use crate::subjects::{
    JOBS_DEAD_SUBJECT, JOBS_QUEUED_SUBJECT, MAX_DELIVERIES_ADVISORY_SUBJECT,
    TERMINATED_ADVISORY_SUBJECT, job_result_subject, job_transition_subject,
    worker_dispatch_subject, worker_heartbeat_subject, worker_registration_subject,
};

pub const API_INBOX_PREFIX: &str = "_INBOX.api";
pub const SCHEDULER_INBOX_PREFIX: &str = "_INBOX.scheduler";
pub const JETSTREAM_API_WILDCARD: &str = "$JS.API.>";
pub const JETSTREAM_ACK_WILDCARD: &str = "$JS.ACK.>";
pub const JOB_QUEUE_ACK_SUBJECTS: &str = "$JS.ACK.JOB_QUEUE.job-dispatch-v1.>";
pub const JOB_ADVISORY_ACK_SUBJECTS: &str = "$JS.ACK.JOB_ADVISORIES.job-advisory-recovery-v1.>";

/// JetStream API calls used by topology provisioning, both pull consumers,
/// advisory recovery, queue metrics, and source-message retirement. Keeping
/// this list explicit prevents the scheduler identity from administering
/// unrelated streams in the same account.
pub const SCHEDULER_JETSTREAM_API_SUBJECTS: [&str; 17] = [
    "$JS.API.STREAM.INFO.JOB_QUEUE",
    "$JS.API.STREAM.CREATE.JOB_QUEUE",
    "$JS.API.STREAM.UPDATE.JOB_QUEUE",
    "$JS.API.STREAM.MSG.GET.JOB_QUEUE",
    "$JS.API.STREAM.MSG.DELETE.JOB_QUEUE",
    "$JS.API.STREAM.INFO.JOB_DLQ",
    "$JS.API.STREAM.CREATE.JOB_DLQ",
    "$JS.API.STREAM.UPDATE.JOB_DLQ",
    "$JS.API.STREAM.INFO.JOB_ADVISORIES",
    "$JS.API.STREAM.CREATE.JOB_ADVISORIES",
    "$JS.API.STREAM.UPDATE.JOB_ADVISORIES",
    "$JS.API.CONSUMER.INFO.JOB_QUEUE.job-dispatch-v1",
    "$JS.API.CONSUMER.CREATE.JOB_QUEUE.job-dispatch-v1.jobs.queued",
    "$JS.API.CONSUMER.MSG.NEXT.JOB_QUEUE.job-dispatch-v1",
    "$JS.API.CONSUMER.INFO.JOB_ADVISORIES.job-advisory-recovery-v1",
    "$JS.API.CONSUMER.CREATE.JOB_ADVISORIES.job-advisory-recovery-v1",
    "$JS.API.CONSUMER.MSG.NEXT.JOB_ADVISORIES.job-advisory-recovery-v1",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NatsPrincipal {
    Api,
    Scheduler,
    Worker(WorkerId),
}

/// A declarative server-side permission set for one authenticated identity.
///
/// Publish checks accept only concrete subjects because NATS publishers may
/// not publish to wildcard subjects. Subscribe checks also accept the exact
/// wildcard subscriptions declared by this policy; broader wildcard requests
/// are denied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NatsPermissionPolicy {
    principal: NatsPrincipal,
    publish: Vec<String>,
    subscribe: Vec<String>,
    allow_responses: bool,
    manage_jetstream: bool,
    consume_job_queue: bool,
    raw_jetstream_ack: bool,
    inbox_prefix: String,
}

impl NatsPermissionPolicy {
    #[must_use]
    pub fn api() -> Self {
        Self {
            principal: NatsPrincipal::Api,
            publish: vec![JOBS_QUEUED_SUBJECT.to_owned()],
            subscribe: vec![format!("{API_INBOX_PREFIX}.>")],
            allow_responses: false,
            manage_jetstream: false,
            consume_job_queue: false,
            raw_jetstream_ack: false,
            inbox_prefix: API_INBOX_PREFIX.to_owned(),
        }
    }

    #[must_use]
    pub fn scheduler() -> Self {
        let mut publish = vec![
            JOBS_QUEUED_SUBJECT.to_owned(),
            JOBS_DEAD_SUBJECT.to_owned(),
            "workers.*.dispatch".to_owned(),
            "_INBOX.workers.*.>".to_owned(),
            JOB_QUEUE_ACK_SUBJECTS.to_owned(),
            JOB_ADVISORY_ACK_SUBJECTS.to_owned(),
        ];
        publish.extend(
            SCHEDULER_JETSTREAM_API_SUBJECTS
                .iter()
                .map(|subject| (*subject).to_owned()),
        );
        Self {
            principal: NatsPrincipal::Scheduler,
            publish,
            subscribe: vec![
                "control.workers.*.register".to_owned(),
                "control.workers.*.heartbeat".to_owned(),
                "control.jobs.*.transition".to_owned(),
                "control.jobs.*.result".to_owned(),
                MAX_DELIVERIES_ADVISORY_SUBJECT.to_owned(),
                TERMINATED_ADVISORY_SUBJECT.to_owned(),
                format!("{SCHEDULER_INBOX_PREFIX}.>"),
            ],
            allow_responses: false,
            manage_jetstream: true,
            consume_job_queue: true,
            raw_jetstream_ack: true,
            inbox_prefix: SCHEDULER_INBOX_PREFIX.to_owned(),
        }
    }

    #[must_use]
    pub fn worker(worker_id: &WorkerId) -> Self {
        let inbox_prefix = worker_inbox_prefix(worker_id);
        Self {
            principal: NatsPrincipal::Worker(worker_id.clone()),
            publish: vec![
                worker_registration_subject(worker_id),
                worker_heartbeat_subject(worker_id),
                job_transition_subject(worker_id),
                job_result_subject(worker_id),
            ],
            subscribe: vec![
                worker_dispatch_subject(worker_id),
                format!("{inbox_prefix}.>"),
            ],
            // A worker may answer only a request it actually received. NATS
            // grants that reply subject temporarily through allow_responses.
            allow_responses: true,
            manage_jetstream: false,
            consume_job_queue: false,
            raw_jetstream_ack: false,
            inbox_prefix,
        }
    }

    #[must_use]
    pub const fn principal(&self) -> &NatsPrincipal {
        &self.principal
    }

    #[must_use]
    pub fn publish_allowlist(&self) -> &[String] {
        &self.publish
    }

    #[must_use]
    pub fn subscribe_allowlist(&self) -> &[String] {
        &self.subscribe
    }

    #[must_use]
    pub const fn allows_dynamic_responses(&self) -> bool {
        self.allow_responses
    }

    #[must_use]
    pub const fn can_manage_jetstream(&self) -> bool {
        self.manage_jetstream
    }

    #[must_use]
    pub const fn can_consume_job_queue(&self) -> bool {
        self.consume_job_queue
    }

    #[must_use]
    pub const fn can_issue_raw_jetstream_ack(&self) -> bool {
        self.raw_jetstream_ack
    }

    /// Prefix that the matching client must pass to async-nats'
    /// `custom_inbox_prefix` option.
    #[must_use]
    pub fn inbox_prefix(&self) -> &str {
        &self.inbox_prefix
    }

    #[must_use]
    pub fn allows_publish(&self, subject: &str) -> bool {
        if contains_wildcard(subject) {
            return false;
        }
        self.publish
            .iter()
            .any(|permission| subject_matches(permission, subject))
    }

    #[must_use]
    pub fn allows_subscribe(&self, subject: &str) -> bool {
        self.subscribe.iter().any(|permission| {
            permission == subject
                || (!contains_wildcard(subject) && subject_matches(permission, subject))
        })
    }
}

#[must_use]
pub fn worker_inbox_prefix(worker_id: &WorkerId) -> String {
    format!("_INBOX.workers.{worker_id}")
}

fn contains_wildcard(subject: &str) -> bool {
    subject.split('.').any(|token| matches!(token, "*" | ">"))
}

fn subject_matches(permission: &str, subject: &str) -> bool {
    let mut permission_tokens = permission.split('.');
    let mut subject_tokens = subject.split('.');

    loop {
        match (permission_tokens.next(), subject_tokens.next()) {
            (Some(">"), Some(_)) => return permission_tokens.next().is_none(),
            (Some("*"), Some(_)) => {}
            (Some(expected), Some(actual)) if expected == actual => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_is_confined_to_its_own_control_and_dispatch_subjects() {
        let worker = WorkerId::new("wrk_alpha").unwrap();
        let other = WorkerId::new("wrk_beta").unwrap();
        let policy = NatsPermissionPolicy::worker(&worker);

        assert!(policy.allows_publish(&worker_registration_subject(&worker)));
        assert!(policy.allows_publish(&worker_heartbeat_subject(&worker)));
        assert!(policy.allows_publish(&job_transition_subject(&worker)));
        assert!(policy.allows_publish(&job_result_subject(&worker)));
        assert!(!policy.allows_publish(&worker_registration_subject(&other)));

        assert!(policy.allows_subscribe(&worker_dispatch_subject(&worker)));
        assert!(!policy.allows_subscribe(&worker_dispatch_subject(&other)));
        assert!(!policy.allows_subscribe("workers.>"));
        assert!(policy.allows_subscribe("_INBOX.workers.wrk_alpha.reply"));
    }

    #[test]
    fn worker_has_no_queue_dlq_management_or_ack_authority() {
        let worker = WorkerId::new("wrk_alpha").unwrap();
        let policy = NatsPermissionPolicy::worker(&worker);

        assert!(!policy.allows_publish(JOBS_QUEUED_SUBJECT));
        assert!(!policy.allows_publish(JOBS_DEAD_SUBJECT));
        assert!(!policy.allows_publish("$JS.API.STREAM.INFO.JOB_QUEUE"));
        assert!(!policy.allows_publish("$JS.ACK.JOB_QUEUE.consumer.1.2"));
        assert!(!policy.can_manage_jetstream());
        assert!(!policy.can_consume_job_queue());
        assert!(!policy.can_issue_raw_jetstream_ack());
    }

    #[test]
    fn api_can_produce_but_cannot_consume_or_manage_work() {
        let policy = NatsPermissionPolicy::api();

        assert!(policy.allows_publish(JOBS_QUEUED_SUBJECT));
        assert!(!policy.allows_publish(JOBS_DEAD_SUBJECT));
        assert!(!policy.allows_subscribe(JOBS_QUEUED_SUBJECT));
        assert!(!policy.allows_subscribe("control.jobs.wrk_alpha.result"));
        assert!(!policy.can_consume_job_queue());
        assert!(!policy.can_manage_jetstream());
        assert!(!policy.can_issue_raw_jetstream_ack());
    }

    #[test]
    fn scheduler_has_every_required_part_four_privilege() {
        let policy = NatsPermissionPolicy::scheduler();

        assert!(policy.allows_publish(JOBS_QUEUED_SUBJECT));
        assert!(policy.allows_publish(JOBS_DEAD_SUBJECT));
        assert!(policy.allows_publish("workers.wrk_alpha.dispatch"));
        assert!(policy.allows_publish("_INBOX.workers.wrk_alpha.request.1"));
        assert!(policy.allows_publish("$JS.API.STREAM.INFO.JOB_QUEUE"));
        assert!(policy.allows_publish("$JS.ACK.JOB_QUEUE.job-dispatch-v1.1.2"));
        assert!(!policy.allows_publish("$JS.API.STREAM.INFO.UNRELATED"));
        assert!(!policy.allows_publish("$JS.ACK.UNRELATED.consumer.1.2"));
        assert!(policy.allows_subscribe("control.workers.*.register"));
        assert!(policy.allows_subscribe("control.jobs.wrk_alpha.result"));
        assert!(policy.allows_subscribe(MAX_DELIVERIES_ADVISORY_SUBJECT));
        assert!(policy.allows_subscribe("_INBOX.scheduler.dispatch-reply"));
        assert!(policy.can_consume_job_queue());
        assert!(policy.can_manage_jetstream());
        assert!(policy.can_issue_raw_jetstream_ack());
        assert!(!policy.allows_dynamic_responses());
    }

    #[test]
    fn wildcard_publish_and_broader_subscribe_requests_are_never_authorized() {
        let scheduler = NatsPermissionPolicy::scheduler();
        let worker = NatsPermissionPolicy::worker(&WorkerId::new("wrk_alpha").unwrap());

        assert!(!scheduler.allows_publish("workers.*.dispatch"));
        assert!(!scheduler.allows_subscribe("control.>"));
        assert!(!worker.allows_subscribe("workers.*.dispatch"));
        assert!(!worker.allows_subscribe("_INBOX.workers.>"));
    }

    #[test]
    fn policies_expose_distinct_client_inbox_prefixes() {
        let worker = WorkerId::new("wrk_alpha").unwrap();

        assert_eq!(NatsPermissionPolicy::api().inbox_prefix(), "_INBOX.api");
        assert_eq!(
            NatsPermissionPolicy::scheduler().inbox_prefix(),
            "_INBOX.scheduler"
        );
        assert_eq!(
            NatsPermissionPolicy::worker(&worker).inbox_prefix(),
            "_INBOX.workers.wrk_alpha"
        );
    }

    #[test]
    fn checked_in_nats_configs_track_the_scoped_scheduler_api_grants() {
        let test_config = include_str!("../tests/fixtures/nats-security.conf");
        let production_config =
            include_str!("../../../deploy/compose/nats-server.production.conf.example");

        for subject in SCHEDULER_JETSTREAM_API_SUBJECTS
            .iter()
            .chain([JOB_QUEUE_ACK_SUBJECTS, JOB_ADVISORY_ACK_SUBJECTS].iter())
        {
            assert!(test_config.contains(subject), "test config lacks {subject}");
            assert!(
                production_config.contains(subject),
                "production config lacks {subject}"
            );
        }
        assert!(!test_config.contains("\"$JS.API.>\""));
        assert!(!production_config.contains("\"$JS.API.>\""));
    }
}
