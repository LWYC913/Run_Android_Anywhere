//! Queue payload validation and canonical PostgreSQL decision rules.

use chrono::{DateTime, Utc};
use run_anywhere_contracts::{JobDeadLetterReason, JobOutcome, JobQueued, JobState};
use run_anywhere_repository::{JobSchedulingSnapshot, LeaseGuard};
use thiserror::Error;

use crate::fairness::PriorityLane;

pub const MAX_QUEUE_PAYLOAD_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum QueuePayloadError {
    #[error("queue payload exceeds the 256 KiB scheduler limit")]
    TooLarge,
    #[error("queue payload is not a valid JobQueued message")]
    Malformed,
}

pub fn decode_queue_payload(bytes: &[u8]) -> Result<JobQueued, QueuePayloadError> {
    if bytes.len() > MAX_QUEUE_PAYLOAD_BYTES {
        return Err(QueuePayloadError::TooLarge);
    }
    serde_json::from_slice(bytes).map_err(|_| QueuePayloadError::Malformed)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CanonicalDeliveryDecision {
    /// The database is already terminal; the stale queue record is disposable.
    AckTerminal,
    /// A cancellation moved queued work onto the finalizer path without a lease.
    FinalizeCancellation,
    /// Valid, unowned work can enter the bounded fairness buffer.
    Buffer(PriorityLane),
    /// A scheduler restart/redelivery must attach the new ack handle to this
    /// exact still-live database lease without a new claim or attempt.
    Rebind(LeaseGuard),
    /// A stale or finalizing database record must be left to the reconciler.
    HoldForReconciliation,
    DeadLetter(JobDeadLetterReason),
}

pub fn classify_canonical_delivery(
    queued: &JobQueued,
    snapshot: &JobSchedulingSnapshot,
    now: DateTime<Utc>,
) -> CanonicalDeliveryDecision {
    if snapshot.job.state.is_terminal() {
        return CanonicalDeliveryDecision::AckTerminal;
    }
    if snapshot.pending_outcome == Some(JobOutcome::Cancelled)
        && snapshot.lease.is_none()
        && snapshot.delivery_attempts == 0
    {
        return CanonicalDeliveryDecision::FinalizeCancellation;
    }
    if queued != &snapshot.queued {
        return CanonicalDeliveryDecision::DeadLetter(
            JobDeadLetterReason::CanonicalJobInconsistent,
        );
    }
    if let Some(lease) = &snapshot.lease {
        return if snapshot
            .lease_expires_at
            .is_some_and(|expires_at| expires_at > now)
        {
            CanonicalDeliveryDecision::Rebind(lease.clone())
        } else {
            CanonicalDeliveryDecision::HoldForReconciliation
        };
    }
    if snapshot.job.state == JobState::Queued
        && snapshot.pending_outcome.is_none()
        && snapshot.lease_expires_at.is_none()
    {
        CanonicalDeliveryDecision::Buffer(snapshot.job.mode.into())
    } else {
        CanonicalDeliveryDecision::HoldForReconciliation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_and_malformed_payloads_are_rejected_without_echoing_content() {
        assert_eq!(
            decode_queue_payload(&vec![b'x'; MAX_QUEUE_PAYLOAD_BYTES + 1]),
            Err(QueuePayloadError::TooLarge)
        );
        assert_eq!(
            decode_queue_payload(b"this-is-not-json"),
            Err(QueuePayloadError::Malformed)
        );
    }
}
