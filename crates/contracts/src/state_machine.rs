use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Claimed,
    ProvisioningRuntime,
    Booting,
    InstallingApk,
    RunningTests,
    DebugAvailable,
    CollectingArtifacts,
    CleaningUp,
    Passed,
    Failed,
    Cancelled,
    TimedOut,
    InfraFailed,
}

impl JobState {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Passed | Self::Failed | Self::Cancelled | Self::TimedOut | Self::InfraFailed
        )
    }

    pub const fn terminal_outcome(self) -> Option<JobOutcome> {
        match self {
            Self::Passed => Some(JobOutcome::Passed),
            Self::Failed => Some(JobOutcome::Failed),
            Self::Cancelled => Some(JobOutcome::Cancelled),
            Self::TimedOut => Some(JobOutcome::TimedOut),
            Self::InfraFailed => Some(JobOutcome::InfraFailed),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobOutcome {
    Passed,
    Failed,
    Cancelled,
    TimedOut,
    InfraFailed,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TransitionEvidence {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_outcome: Option<JobOutcome>,
    pub artifacts_finalized: bool,
    pub cleanup_completed: bool,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum TransitionError {
    #[error("transition from {from:?} to {to:?} is not allowed")]
    InvalidTransition { from: JobState, to: JobState },
    #[error("transition from {from:?} to {to:?} requires a pending outcome")]
    MissingPendingOutcome { from: JobState, to: JobState },
    #[error("outcome {outcome:?} is not allowed while leaving {from:?}")]
    OutcomeNotAllowed { from: JobState, outcome: JobOutcome },
    #[error("artifacts must be finalized before cleanup begins")]
    ArtifactsNotFinalized,
    #[error("cleanup must complete before a terminal state is recorded")]
    CleanupNotCompleted,
    #[error("terminal state {to:?} does not match pending outcome {pending:?}")]
    TerminalOutcomeMismatch { pending: JobOutcome, to: JobState },
}

pub fn can_transition(from: JobState, to: JobState, evidence: &TransitionEvidence) -> bool {
    validate_transition(from, to, evidence).is_ok()
}

pub fn validate_transition(
    from: JobState,
    to: JobState,
    evidence: &TransitionEvidence,
) -> Result<(), TransitionError> {
    if from.is_terminal() || (to.is_terminal() && from != JobState::CleaningUp) {
        return Err(TransitionError::InvalidTransition { from, to });
    }

    match (from, to) {
        (JobState::Queued, JobState::Claimed)
        | (JobState::Claimed, JobState::ProvisioningRuntime)
        | (JobState::ProvisioningRuntime, JobState::Booting)
        | (JobState::Booting, JobState::InstallingApk)
        | (JobState::InstallingApk, JobState::RunningTests) => Ok(()),

        (JobState::RunningTests, JobState::DebugAvailable) => {
            require_pending_outcome(from, to, evidence).map(|_| ())
        }

        (JobState::RunningTests | JobState::DebugAvailable, JobState::CollectingArtifacts) => {
            require_pending_outcome(from, to, evidence).map(|_| ())
        }

        (
            JobState::Queued
            | JobState::Claimed
            | JobState::ProvisioningRuntime
            | JobState::Booting
            | JobState::InstallingApk,
            JobState::CollectingArtifacts,
        ) => {
            let outcome = require_pending_outcome(from, to, evidence)?;
            if early_outcome_allowed(from, outcome) {
                Ok(())
            } else {
                Err(TransitionError::OutcomeNotAllowed { from, outcome })
            }
        }

        (JobState::CollectingArtifacts, JobState::CleaningUp) => {
            if evidence.artifacts_finalized {
                Ok(())
            } else {
                Err(TransitionError::ArtifactsNotFinalized)
            }
        }

        (JobState::CleaningUp, terminal) if terminal.is_terminal() => {
            if !evidence.cleanup_completed {
                return Err(TransitionError::CleanupNotCompleted);
            }
            let pending = require_pending_outcome(from, to, evidence)?;
            if terminal.terminal_outcome() == Some(pending) {
                Ok(())
            } else {
                Err(TransitionError::TerminalOutcomeMismatch {
                    pending,
                    to: terminal,
                })
            }
        }

        _ => Err(TransitionError::InvalidTransition { from, to }),
    }
}

fn require_pending_outcome(
    from: JobState,
    to: JobState,
    evidence: &TransitionEvidence,
) -> Result<JobOutcome, TransitionError> {
    evidence
        .pending_outcome
        .ok_or(TransitionError::MissingPendingOutcome { from, to })
}

const fn early_outcome_allowed(from: JobState, outcome: JobOutcome) -> bool {
    match from {
        JobState::Queued => matches!(outcome, JobOutcome::Cancelled | JobOutcome::TimedOut),
        JobState::Claimed | JobState::ProvisioningRuntime | JobState::Booting => matches!(
            outcome,
            JobOutcome::Cancelled | JobOutcome::TimedOut | JobOutcome::InfraFailed
        ),
        JobState::InstallingApk => matches!(
            outcome,
            JobOutcome::Failed
                | JobOutcome::Cancelled
                | JobOutcome::TimedOut
                | JobOutcome::InfraFailed
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(outcome: JobOutcome) -> TransitionEvidence {
        TransitionEvidence {
            pending_outcome: Some(outcome),
            artifacts_finalized: false,
            cleanup_completed: false,
        }
    }

    #[test]
    fn normal_spine_transitions_are_allowed() {
        let empty = TransitionEvidence::default();
        for (from, to) in [
            (JobState::Queued, JobState::Claimed),
            (JobState::Claimed, JobState::ProvisioningRuntime),
            (JobState::ProvisioningRuntime, JobState::Booting),
            (JobState::Booting, JobState::InstallingApk),
            (JobState::InstallingApk, JobState::RunningTests),
        ] {
            assert!(can_transition(from, to, &empty), "{from:?} -> {to:?}");
        }
    }

    #[test]
    fn debug_is_a_hold_after_an_outcome_is_known() {
        assert!(can_transition(
            JobState::RunningTests,
            JobState::DebugAvailable,
            &evidence(JobOutcome::Failed)
        ));
        assert_eq!(
            validate_transition(
                JobState::RunningTests,
                JobState::DebugAvailable,
                &TransitionEvidence::default()
            ),
            Err(TransitionError::MissingPendingOutcome {
                from: JobState::RunningTests,
                to: JobState::DebugAvailable
            })
        );
    }

    #[test]
    fn test_and_debug_states_flow_through_artifact_collection() {
        for state in [JobState::RunningTests, JobState::DebugAvailable] {
            assert!(can_transition(
                state,
                JobState::CollectingArtifacts,
                &evidence(JobOutcome::Passed)
            ));
        }
    }

    #[test]
    fn early_outcomes_are_source_specific() {
        assert!(can_transition(
            JobState::Queued,
            JobState::CollectingArtifacts,
            &evidence(JobOutcome::Cancelled)
        ));
        assert!(!can_transition(
            JobState::Queued,
            JobState::CollectingArtifacts,
            &evidence(JobOutcome::InfraFailed)
        ));
        assert!(can_transition(
            JobState::Claimed,
            JobState::CollectingArtifacts,
            &evidence(JobOutcome::InfraFailed)
        ));
        assert!(!can_transition(
            JobState::Booting,
            JobState::CollectingArtifacts,
            &evidence(JobOutcome::Failed)
        ));
        assert!(can_transition(
            JobState::InstallingApk,
            JobState::CollectingArtifacts,
            &evidence(JobOutcome::Failed)
        ));
    }

    #[test]
    fn artifacts_must_be_finalized_before_cleanup() {
        assert_eq!(
            validate_transition(
                JobState::CollectingArtifacts,
                JobState::CleaningUp,
                &evidence(JobOutcome::Passed)
            ),
            Err(TransitionError::ArtifactsNotFinalized)
        );
        let mut finalized = evidence(JobOutcome::Passed);
        finalized.artifacts_finalized = true;
        assert!(can_transition(
            JobState::CollectingArtifacts,
            JobState::CleaningUp,
            &finalized
        ));
    }

    #[test]
    fn confirmed_empty_artifact_manifest_can_be_finalized() {
        let finalized_empty_manifest = TransitionEvidence {
            pending_outcome: Some(JobOutcome::Cancelled),
            artifacts_finalized: true,
            cleanup_completed: false,
        };
        assert!(can_transition(
            JobState::CollectingArtifacts,
            JobState::CleaningUp,
            &finalized_empty_manifest
        ));
    }

    #[test]
    fn cleanup_and_matching_outcome_gate_terminal_state() {
        let not_clean = evidence(JobOutcome::Failed);
        assert_eq!(
            validate_transition(JobState::CleaningUp, JobState::Failed, &not_clean),
            Err(TransitionError::CleanupNotCompleted)
        );

        let completed = TransitionEvidence {
            pending_outcome: Some(JobOutcome::Failed),
            artifacts_finalized: true,
            cleanup_completed: true,
        };
        assert!(can_transition(
            JobState::CleaningUp,
            JobState::Failed,
            &completed
        ));
        assert_eq!(
            validate_transition(JobState::CleaningUp, JobState::Passed, &completed),
            Err(TransitionError::TerminalOutcomeMismatch {
                pending: JobOutcome::Failed,
                to: JobState::Passed
            })
        );
    }

    #[test]
    fn terminal_states_are_immutable_and_cannot_be_skipped_to() {
        let completed = TransitionEvidence {
            pending_outcome: Some(JobOutcome::Passed),
            artifacts_finalized: true,
            cleanup_completed: true,
        };
        assert!(!can_transition(
            JobState::RunningTests,
            JobState::Passed,
            &completed
        ));
        assert!(!can_transition(
            JobState::Passed,
            JobState::CollectingArtifacts,
            &completed
        ));
    }

    #[test]
    fn invalid_skips_are_rejected() {
        assert!(!can_transition(
            JobState::Queued,
            JobState::Booting,
            &TransitionEvidence::default()
        ));
        assert!(!can_transition(
            JobState::DebugAvailable,
            JobState::RunningTests,
            &evidence(JobOutcome::Failed)
        ));
    }
}
