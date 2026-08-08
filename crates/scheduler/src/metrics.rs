//! Minimal dependency-free Prometheus exposition for the scheduler.

use std::{
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{Router, extract::State, http::header, response::IntoResponse, routing::get};

use crate::fairness::PriorityLane;

const CLAIM_LATENCY_BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum DlqMetricReason {
    Malformed = 0,
    DatabaseInconsistent = 1,
    AttemptsExhausted = 2,
    Cancelled = 3,
    Other = 4,
}

impl DlqMetricReason {
    const ALL: [Self; 5] = [
        Self::Malformed,
        Self::DatabaseInconsistent,
        Self::AttemptsExhausted,
        Self::Cancelled,
        Self::Other,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::DatabaseInconsistent => "database_inconsistent",
            Self::AttemptsExhausted => "attempts_exhausted",
            Self::Cancelled => "cancelled",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum RecoveryMetricResult {
    Requeued = 0,
    InfraFailed = 1,
    Cancelled = 2,
    StaleLease = 3,
    Error = 4,
}

impl RecoveryMetricResult {
    const ALL: [Self; 5] = [
        Self::Requeued,
        Self::InfraFailed,
        Self::Cancelled,
        Self::StaleLease,
        Self::Error,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::Requeued => "requeued",
            Self::InfraFailed => "infra_failed",
            Self::Cancelled => "cancelled",
            Self::StaleLease => "stale_lease",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum ReaperMetricResult {
    Reaped = 0,
    AlreadyAbsent = 1,
    Protected = 2,
    InGrace = 3,
    Error = 4,
}

impl ReaperMetricResult {
    const ALL: [Self; 5] = [
        Self::Reaped,
        Self::AlreadyAbsent,
        Self::Protected,
        Self::InGrace,
        Self::Error,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::Reaped => "reaped",
            Self::AlreadyAbsent => "already_absent",
            Self::Protected => "protected",
            Self::InGrace => "in_grace",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Default)]
pub struct SchedulerMetrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    queue_pending: AtomicU64,
    queue_ack_pending: AtomicU64,
    pending_interactive: AtomicU64,
    pending_batch: AtomicU64,
    claim_latency_buckets: [AtomicU64; CLAIM_LATENCY_BUCKETS.len()],
    claim_latency_count: AtomicU64,
    claim_latency_sum_bits: AtomicU64,
    redeliveries: AtomicU64,
    quota_blocked: AtomicU64,
    dlq: [AtomicU64; DlqMetricReason::ALL.len()],
    recoveries: [AtomicU64; RecoveryMetricResult::ALL.len()],
    reaper: [AtomicU64; ReaperMetricResult::ALL.len()],
}

impl Default for MetricsInner {
    fn default() -> Self {
        Self {
            queue_pending: AtomicU64::new(0),
            queue_ack_pending: AtomicU64::new(0),
            pending_interactive: AtomicU64::new(0),
            pending_batch: AtomicU64::new(0),
            claim_latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            claim_latency_count: AtomicU64::new(0),
            claim_latency_sum_bits: AtomicU64::new(0.0_f64.to_bits()),
            redeliveries: AtomicU64::new(0),
            quota_blocked: AtomicU64::new(0),
            dlq: std::array::from_fn(|_| AtomicU64::new(0)),
            recoveries: std::array::from_fn(|_| AtomicU64::new(0)),
            reaper: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl SchedulerMetrics {
    pub fn set_queue_depth(&self, pending: u64, ack_pending: u64) {
        self.inner.queue_pending.store(pending, Ordering::Relaxed);
        self.inner
            .queue_ack_pending
            .store(ack_pending, Ordering::Relaxed);
    }

    pub fn set_pending_dispatches(&self, lane: PriorityLane, count: u64) {
        match lane {
            PriorityLane::Interactive => self
                .inner
                .pending_interactive
                .store(count, Ordering::Relaxed),
            PriorityLane::Batch => self.inner.pending_batch.store(count, Ordering::Relaxed),
        }
    }

    pub fn observe_claim_latency(&self, latency: Duration) {
        let seconds = latency.as_secs_f64();
        for (bound, bucket) in CLAIM_LATENCY_BUCKETS
            .iter()
            .zip(&self.inner.claim_latency_buckets)
        {
            if seconds <= *bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.inner
            .claim_latency_count
            .fetch_add(1, Ordering::Relaxed);
        atomic_add_f64(&self.inner.claim_latency_sum_bits, seconds);
    }

    pub fn record_redelivery(&self) {
        self.inner.redeliveries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dlq(&self, reason: DlqMetricReason) {
        self.inner.dlq[reason as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_quota_blocked(&self) {
        self.inner.quota_blocked.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_recovery(&self, result: RecoveryMetricResult) {
        self.inner.recoveries[result as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_reaper(&self, result: ReaperMetricResult) {
        self.inner.reaper[result as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn render(&self) -> String {
        let mut output = String::with_capacity(4096);
        output.push_str(
            "# HELP raa_scheduler_queue_depth JetStream job messages by consumer state.\n",
        );
        output.push_str("# TYPE raa_scheduler_queue_depth gauge\n");
        writeln!(
            output,
            "raa_scheduler_queue_depth{{state=\"pending\"}} {}",
            self.inner.queue_pending.load(Ordering::Relaxed)
        )
        .expect("writing to String cannot fail");
        writeln!(
            output,
            "raa_scheduler_queue_depth{{state=\"ack_pending\"}} {}",
            self.inner.queue_ack_pending.load(Ordering::Relaxed)
        )
        .expect("writing to String cannot fail");

        output.push_str("# HELP raa_scheduler_claim_latency_seconds Time spent obtaining a fenced database claim.\n");
        output.push_str("# TYPE raa_scheduler_claim_latency_seconds histogram\n");
        for (bound, bucket) in CLAIM_LATENCY_BUCKETS
            .iter()
            .zip(&self.inner.claim_latency_buckets)
        {
            writeln!(
                output,
                "raa_scheduler_claim_latency_seconds_bucket{{le=\"{bound}\"}} {}",
                bucket.load(Ordering::Relaxed)
            )
            .expect("writing to String cannot fail");
        }
        let claim_count = self.inner.claim_latency_count.load(Ordering::Relaxed);
        writeln!(
            output,
            "raa_scheduler_claim_latency_seconds_bucket{{le=\"+Inf\"}} {claim_count}"
        )
        .expect("writing to String cannot fail");
        writeln!(
            output,
            "raa_scheduler_claim_latency_seconds_sum {}",
            f64::from_bits(self.inner.claim_latency_sum_bits.load(Ordering::Relaxed))
        )
        .expect("writing to String cannot fail");
        writeln!(
            output,
            "raa_scheduler_claim_latency_seconds_count {claim_count}"
        )
        .expect("writing to String cannot fail");

        render_counter(
            &mut output,
            "raa_scheduler_redeliveries_total",
            "JetStream job message redeliveries observed.",
            self.inner.redeliveries.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "raa_scheduler_quota_blocked_total",
            "Claims held back by project concurrency quotas.",
            self.inner.quota_blocked.load(Ordering::Relaxed),
        );

        output.push_str(
            "# HELP raa_scheduler_dlq_total Jobs durably sent to the dead-letter stream.\n",
        );
        output.push_str("# TYPE raa_scheduler_dlq_total counter\n");
        for reason in DlqMetricReason::ALL {
            writeln!(
                output,
                "raa_scheduler_dlq_total{{reason=\"{}\"}} {}",
                reason.label(),
                self.inner.dlq[reason as usize].load(Ordering::Relaxed)
            )
            .expect("writing to String cannot fail");
        }

        output.push_str(
            "# HELP raa_scheduler_reconciler_recoveries_total Jobs handled by reconciliation.\n",
        );
        output.push_str("# TYPE raa_scheduler_reconciler_recoveries_total counter\n");
        for result in RecoveryMetricResult::ALL {
            writeln!(
                output,
                "raa_scheduler_reconciler_recoveries_total{{result=\"{}\"}} {}",
                result.label(),
                self.inner.recoveries[result as usize].load(Ordering::Relaxed)
            )
            .expect("writing to String cannot fail");
        }

        output.push_str("# HELP raa_scheduler_reaper_total Runtime cleanup decisions.\n");
        output.push_str("# TYPE raa_scheduler_reaper_total counter\n");
        for result in ReaperMetricResult::ALL {
            writeln!(
                output,
                "raa_scheduler_reaper_total{{result=\"{}\"}} {}",
                result.label(),
                self.inner.reaper[result as usize].load(Ordering::Relaxed)
            )
            .expect("writing to String cannot fail");
        }

        output.push_str(
            "# HELP raa_scheduler_pending_dispatches Work held in the fairness buffer.\n",
        );
        output.push_str("# TYPE raa_scheduler_pending_dispatches gauge\n");
        writeln!(
            output,
            "raa_scheduler_pending_dispatches{{lane=\"interactive\"}} {}",
            self.inner.pending_interactive.load(Ordering::Relaxed)
        )
        .expect("writing to String cannot fail");
        writeln!(
            output,
            "raa_scheduler_pending_dispatches{{lane=\"batch\"}} {}",
            self.inner.pending_batch.load(Ordering::Relaxed)
        )
        .expect("writing to String cannot fail");
        output
    }
}

fn atomic_add_f64(value: &AtomicU64, delta: f64) {
    let mut current = value.load(Ordering::Relaxed);
    loop {
        let next = (f64::from_bits(current) + delta).to_bits();
        match value.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn render_counter(output: &mut String, name: &str, help: &str, value: u64) {
    writeln!(output, "# HELP {name} {help}").expect("writing to String cannot fail");
    writeln!(output, "# TYPE {name} counter").expect("writing to String cannot fail");
    writeln!(output, "{name} {value}").expect("writing to String cannot fail");
}

pub fn metrics_router(metrics: SchedulerMetrics) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(metrics)
}

async fn metrics_handler(State(metrics): State<SchedulerMetrics>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        metrics.render(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    #[test]
    fn renders_all_required_metric_families_and_labels() {
        let metrics = SchedulerMetrics::default();
        metrics.set_queue_depth(7, 2);
        metrics.set_pending_dispatches(PriorityLane::Interactive, 3);
        metrics.observe_claim_latency(Duration::from_millis(25));
        metrics.record_redelivery();
        metrics.record_dlq(DlqMetricReason::AttemptsExhausted);
        metrics.record_quota_blocked();
        metrics.record_recovery(RecoveryMetricResult::Requeued);
        metrics.record_reaper(ReaperMetricResult::Reaped);

        let rendered = metrics.render();
        assert!(rendered.contains("raa_scheduler_queue_depth{state=\"pending\"} 7"));
        assert!(rendered.contains("raa_scheduler_queue_depth{state=\"ack_pending\"} 2"));
        assert!(rendered.contains("raa_scheduler_claim_latency_seconds_count 1"));
        assert!(rendered.contains("raa_scheduler_redeliveries_total 1"));
        assert!(rendered.contains("raa_scheduler_dlq_total{reason=\"attempts_exhausted\"} 1"));
        assert!(rendered.contains("raa_scheduler_quota_blocked_total 1"));
        assert!(
            rendered.contains("raa_scheduler_reconciler_recoveries_total{result=\"requeued\"} 1")
        );
        assert!(rendered.contains("raa_scheduler_reaper_total{result=\"reaped\"} 1"));
        assert!(rendered.contains("raa_scheduler_pending_dispatches{lane=\"interactive\"} 3"));
    }

    #[test]
    fn cloned_handles_share_atomic_state() {
        let first = SchedulerMetrics::default();
        let second = first.clone();
        first.record_redelivery();
        second.record_redelivery();
        assert!(
            first
                .render()
                .contains("raa_scheduler_redeliveries_total 2")
        );
    }

    #[tokio::test]
    async fn router_serves_prometheus_text_on_the_dedicated_path() {
        let response = metrics_router(SchedulerMetrics::default())
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(
            String::from_utf8(body.to_vec())
                .unwrap()
                .contains("raa_scheduler_queue_depth")
        );
    }
}
