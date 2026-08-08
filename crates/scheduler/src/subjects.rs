//! Stable NATS subjects and bounded W3C trace-context propagation.

use std::{collections::BTreeMap, str::FromStr};

use async_nats::{HeaderMap, HeaderName, HeaderValue};
use run_anywhere_contracts::WorkerId;
use thiserror::Error;

pub const JOBS_QUEUED_SUBJECT: &str = "jobs.queued";
pub const JOBS_DEAD_SUBJECT: &str = "jobs.dead";

pub const JOB_QUEUE_STREAM: &str = "JOB_QUEUE";
pub const JOB_DISPATCH_CONSUMER: &str = "job-dispatch-v1";
pub const JOB_DLQ_STREAM: &str = "JOB_DLQ";
pub const JOB_ADVISORY_STREAM: &str = "JOB_ADVISORIES";
pub const JOB_ADVISORY_CONSUMER: &str = "job-advisory-recovery-v1";

pub const MAX_DELIVERIES_ADVISORY_SUBJECT: &str =
    "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.JOB_QUEUE.job-dispatch-v1";
pub const TERMINATED_ADVISORY_SUBJECT: &str =
    "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.JOB_QUEUE.job-dispatch-v1";

pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const TRACESTATE_HEADER: &str = "tracestate";
pub const BAGGAGE_HEADER: &str = "baggage";
pub const TRACE_HEADER_NAMES: [&str; 3] = [TRACEPARENT_HEADER, TRACESTATE_HEADER, BAGGAGE_HEADER];
pub const MAX_TRACE_HEADER_BYTES: usize = 4096;

pub fn worker_registration_subject(worker_id: &WorkerId) -> String {
    format!("control.workers.{worker_id}.register")
}

pub fn worker_heartbeat_subject(worker_id: &WorkerId) -> String {
    format!("control.workers.{worker_id}.heartbeat")
}

pub fn job_transition_subject(worker_id: &WorkerId) -> String {
    format!("control.jobs.{worker_id}.transition")
}

pub fn job_result_subject(worker_id: &WorkerId) -> String {
    format!("control.jobs.{worker_id}.result")
}

pub fn worker_dispatch_subject(worker_id: &WorkerId) -> String {
    format!("workers.{worker_id}.dispatch")
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TraceHeaderError {
    #[error("unsupported trace header `{0}`")]
    Unsupported(String),
    #[error("trace header `{0}` exceeds 4096 bytes")]
    TooLarge(String),
    #[error("invalid trace header name `{0}`")]
    InvalidName(String),
    #[error("invalid value for trace header `{0}`")]
    InvalidValue(String),
}

/// Extract only W3C propagation fields. Queue metadata and credentials are
/// deliberately never copied into worker dispatch requests.
pub fn extract_trace_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    let mut trace = BTreeMap::new();
    for name in TRACE_HEADER_NAMES {
        if let Some(value) = headers.get(name) {
            let value = value.as_str();
            if value.len() <= MAX_TRACE_HEADER_BYTES {
                trace.insert(name.to_owned(), value.to_owned());
            }
        }
    }
    trace
}

/// Add a previously extracted trace context to a NATS request. Unknown fields
/// are rejected to keep this boundary from becoming a general header tunnel.
pub fn inject_trace_headers(
    headers: &mut HeaderMap,
    trace: &BTreeMap<String, String>,
) -> Result<(), TraceHeaderError> {
    for (name, value) in trace {
        let canonical = TRACE_HEADER_NAMES
            .iter()
            .copied()
            .find(|candidate| candidate.eq_ignore_ascii_case(name))
            .ok_or_else(|| TraceHeaderError::Unsupported(name.clone()))?;
        if value.len() > MAX_TRACE_HEADER_BYTES {
            return Err(TraceHeaderError::TooLarge(canonical.to_owned()));
        }
        let parsed_name = HeaderName::from_str(canonical)
            .map_err(|_| TraceHeaderError::InvalidName(canonical.to_owned()))?;
        let parsed_value = HeaderValue::from_str(value)
            .map_err(|_| TraceHeaderError::InvalidValue(canonical.to_owned()))?;
        headers.insert(parsed_name, parsed_value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_subjects_are_scoped_to_the_validated_worker() {
        let worker = WorkerId::new("wrk_scheduler-a").unwrap();
        assert_eq!(
            worker_registration_subject(&worker),
            "control.workers.wrk_scheduler-a.register"
        );
        assert_eq!(
            worker_heartbeat_subject(&worker),
            "control.workers.wrk_scheduler-a.heartbeat"
        );
        assert_eq!(
            job_transition_subject(&worker),
            "control.jobs.wrk_scheduler-a.transition"
        );
        assert_eq!(
            job_result_subject(&worker),
            "control.jobs.wrk_scheduler-a.result"
        );
        assert_eq!(
            worker_dispatch_subject(&worker),
            "workers.wrk_scheduler-a.dispatch"
        );
    }

    #[test]
    fn trace_propagation_has_a_strict_allowlist() {
        let mut headers = HeaderMap::new();
        headers.insert(TRACEPARENT_HEADER, "00-abc-def-01");
        headers.insert("authorization", "must-not-propagate");
        let trace = extract_trace_headers(&headers);
        assert_eq!(trace.len(), 1);
        assert_eq!(trace[TRACEPARENT_HEADER], "00-abc-def-01");

        let unsupported = BTreeMap::from([("authorization".to_owned(), "secret".to_owned())]);
        assert!(matches!(
            inject_trace_headers(&mut HeaderMap::new(), &unsupported),
            Err(TraceHeaderError::Unsupported(_))
        ));
    }
}
