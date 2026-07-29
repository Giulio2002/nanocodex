use std::{path::PathBuf, sync::Arc};

use nanocodex_agent::events::AgentEvent;
use serde::Serialize;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{EvalFailure, EvalResult, VerifierResult};

/// One event from a possibly concurrent Evaluator attempt.
#[derive(Clone, Debug, Serialize)]
pub struct EvalEvent {
    /// Evaluator job identity.
    pub run_id: Uuid,
    /// `UUIDv7` identity for the emitting attempt.
    pub attempt_id: Uuid,
    /// Stable task name.
    pub task_name: String,
    /// Filesystem-safe unique trial name.
    pub trial_name: String,
    /// One-based monotonic sequence within this attempt.
    pub sequence: u64,
    /// Typed event payload.
    #[serde(flatten)]
    pub kind: EvalEventKind,
}

/// Agent and verifier activity exposed independently from [`EvalResult`].
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum EvalEventKind {
    /// The disposable environment is ready and agent setup is beginning.
    AttemptStarted {
        /// Complete task prompt.
        prompt: String,
        /// Workspace presented to the agent.
        workspace: PathBuf,
    },
    /// One unmodified event from the owned agent lifecycle.
    Agent(AgentEvent),
    /// Verification is beginning.
    VerifierStarted,
    /// Complete verifier output became available.
    VerifierOutput {
        /// Captured standard output.
        stdout: String,
        /// Captured standard error.
        stderr: String,
    },
    /// Verification produced its exit code and rewards.
    VerifierCompleted(VerifierResult),
    /// The scored result became terminal.
    Completed(Box<EvalResult>),
    /// The attempt failed without a score.
    Failed(Box<EvalFailure>),
}

/// Cloneable source of independent subscriptions to one evaluation job.
#[derive(Clone)]
pub struct EvalEvents {
    sender: broadcast::Sender<Arc<EvalEvent>>,
}

/// One independent, ordered subscription to an evaluation job.
pub struct EvalEventStream {
    receiver: broadcast::Receiver<Arc<EvalEvent>>,
}

/// Failure while consuming a bounded evaluation event subscription.
#[derive(Debug, thiserror::Error)]
pub enum EvalEventStreamError {
    /// The subscriber did not keep pace with producers.
    #[error("event subscriber fell behind and missed {missed} events")]
    Lagged {
        /// Number of events dropped for this subscription.
        missed: u64,
    },
}

impl EvalEvents {
    pub(crate) const fn new(sender: broadcast::Sender<Arc<EvalEvent>>) -> Self {
        Self { sender }
    }

    /// Subscribes before attempts start. Each subscription receives the same
    /// subsequent events independently.
    #[must_use]
    pub fn subscribe(&self) -> EvalEventStream {
        EvalEventStream {
            receiver: self.sender.subscribe(),
        }
    }
}

impl EvalEventStream {
    /// Receives the next event, `None` after the run closes, or an explicit
    /// lag error rather than silently skipping events.
    ///
    /// # Errors
    ///
    /// Returns [`EvalEventStreamError::Lagged`] when this subscriber did not
    /// keep up with the bounded event journal.
    pub async fn recv(&mut self) -> Result<Option<Arc<EvalEvent>>, EvalEventStreamError> {
        match self.receiver.recv().await {
            Ok(event) => Ok(Some(event)),
            Err(broadcast::error::RecvError::Closed) => Ok(None),
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                Err(EvalEventStreamError::Lagged { missed })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::broadcast;
    use uuid::Uuid;

    use super::{EvalEvent, EvalEventKind, EvalEventStreamError, EvalEvents};

    #[tokio::test]
    async fn subscriptions_receive_the_same_event_independently() {
        let (sender, _) = broadcast::channel(4);
        let events = EvalEvents::new(sender.clone());
        let mut first = events.subscribe();
        let mut second = events.subscribe();
        let event = Arc::new(event(1));

        sender.send(Arc::clone(&event)).unwrap();

        let first = first.recv().await.unwrap().unwrap();
        let second = second.recv().await.unwrap().unwrap();
        assert!(Arc::ptr_eq(&first, &event));
        assert!(Arc::ptr_eq(&second, &event));
    }

    #[tokio::test]
    async fn lag_is_reported_instead_of_silently_skipping_events() {
        let (sender, _) = broadcast::channel(1);
        let events = EvalEvents::new(sender.clone());
        let mut subscriber = events.subscribe();

        sender.send(Arc::new(event(1))).unwrap();
        sender.send(Arc::new(event(2))).unwrap();

        assert!(matches!(
            subscriber.recv().await,
            Err(EvalEventStreamError::Lagged { missed: 1 })
        ));
        assert_eq!(subscriber.recv().await.unwrap().unwrap().sequence, 2);
    }

    fn event(sequence: u64) -> EvalEvent {
        EvalEvent {
            run_id: Uuid::nil(),
            attempt_id: Uuid::nil(),
            task_name: "task".to_owned(),
            trial_name: "task__attempt".to_owned(),
            sequence,
            kind: EvalEventKind::VerifierStarted,
        }
    }
}
