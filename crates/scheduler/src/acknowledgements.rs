//! Scheduler-owned JetStream acknowledgement bindings.
//!
//! Workers only ever identify the fenced database lease.  The opaque NATS
//! acknowledgement subject remains inside this process.

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_nats::jetstream::{AckKind, Message};
use run_anywhere_contracts::{JobId, LeaseId, WorkerId};
use thiserror::Error;
use tokio::sync::RwLock;

/// One delivered queue message bound to the canonical PostgreSQL lease.
#[derive(Clone)]
pub struct AckBinding {
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub stream_sequence: u64,
    pub message: Message,
}

impl std::fmt::Debug for AckBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AckBinding")
            .field("worker_id", &self.worker_id)
            .field("lease_id", &self.lease_id)
            .field("stream_sequence", &self.stream_sequence)
            .field("message", &"[redacted]")
            .finish()
    }
}

impl AckBinding {
    pub fn matches(&self, worker_id: &WorkerId, lease_id: &LeaseId) -> bool {
        self.worker_id == *worker_id && self.lease_id == *lease_id
    }
}

#[derive(Clone, Debug, Default)]
pub struct AckRegistry {
    inner: Arc<RwLock<HashMap<JobId, AckBinding>>>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AckRegistryError {
    #[error("there is no active queue acknowledgement for job `{0}`")]
    Missing(JobId),
    #[error("the queue acknowledgement for job `{0}` belongs to another lease")]
    StaleLease(JobId),
    #[error("JetStream acknowledgement failed: {0}")]
    JetStream(String),
}

impl AckRegistry {
    /// Bind a delivery after PostgreSQL has proved which exact lease owns it.
    /// A redelivery for that same live lease replaces the obsolete ack handle
    /// without changing the database attempt counter.
    pub async fn bind(&self, job_id: JobId, binding: AckBinding) {
        self.inner.write().await.insert(job_id, binding);
    }

    pub async fn binding(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        lease_id: &LeaseId,
    ) -> Result<AckBinding, AckRegistryError> {
        let bindings = self.inner.read().await;
        let binding = bindings
            .get(job_id)
            .ok_or_else(|| AckRegistryError::Missing(job_id.clone()))?;
        if !binding.matches(worker_id, lease_id) {
            return Err(AckRegistryError::StaleLease(job_id.clone()));
        }
        Ok(binding.clone())
    }

    pub async fn progress(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        lease_id: &LeaseId,
    ) -> Result<(), AckRegistryError> {
        let binding = self.binding(job_id, worker_id, lease_id).await?;
        binding
            .message
            .ack_with(AckKind::Progress)
            .await
            .map_err(|error| AckRegistryError::JetStream(error.to_string()))
    }

    /// Confirm an acknowledgement with the server before dropping the binding.
    pub async fn confirm_ack(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        lease_id: &LeaseId,
    ) -> Result<(), AckRegistryError> {
        let mut binding = self.binding(job_id, worker_id, lease_id).await?;
        loop {
            binding
                .message
                .double_ack()
                .await
                .map_err(|error| AckRegistryError::JetStream(error.to_string()))?;

            let mut bindings = self.inner.write().await;
            let Some(current) = bindings.get(job_id) else {
                return Ok(());
            };
            if !current.matches(worker_id, lease_id) {
                return Err(AckRegistryError::StaleLease(job_id.clone()));
            }
            if current.stream_sequence == binding.stream_sequence {
                bindings.remove(job_id);
                return Ok(());
            }
            // A canonical redrive was rebound while the previous server
            // confirmation was in flight. Confirm that replacement too before
            // reporting semantic completion to the worker.
            binding = current.clone();
        }
    }

    /// Release a recovered lease for redelivery. A failed NAK is safe: the
    /// message remains unacknowledged and becomes visible after `AckWait`.
    pub async fn nak(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        lease_id: &LeaseId,
        delay: Option<Duration>,
    ) -> Result<(), AckRegistryError> {
        let binding = self.take(job_id, worker_id, lease_id).await?;
        binding
            .message
            .ack_with(AckKind::Nak(delay))
            .await
            .map_err(|error| AckRegistryError::JetStream(error.to_string()))
    }

    pub async fn take(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        lease_id: &LeaseId,
    ) -> Result<AckBinding, AckRegistryError> {
        let mut bindings = self.inner.write().await;
        let binding = bindings
            .get(job_id)
            .ok_or_else(|| AckRegistryError::Missing(job_id.clone()))?;
        if !binding.matches(worker_id, lease_id) {
            return Err(AckRegistryError::StaleLease(job_id.clone()));
        }
        bindings
            .remove(job_id)
            .ok_or_else(|| AckRegistryError::Missing(job_id.clone()))
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }
}
