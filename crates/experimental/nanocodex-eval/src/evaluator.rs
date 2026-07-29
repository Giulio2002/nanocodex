use std::{
    error::Error,
    ffi::OsString,
    fmt, fs,
    future::Future,
    io,
    num::ParseFloatError,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use nanocodex_agent::{
    Nanocodex, NanocodexBuilder, NanocodexError,
    events::{
        AgentEvent, AgentEventKind, AgentEvents, CompactionCompleted, CompactionFailed,
        ModelCallCompleted, ModelCallFailed, ModelWarmupCompleted, ModelWarmupFailed, RunStarted,
        ToolResultEvent,
    },
    session::SessionId,
    transport::ResponsesError,
};
use nanocodex_oai_api::{MODEL, pricing::CostStatus, responses::Usage};
use serde::Deserialize;
use tokio::{
    sync::{Notify, broadcast},
    time::timeout,
};
use tracing::{Instrument, Span, info, info_span};
use uuid::Uuid;

use crate::{
    AgentId, AgentMetadata, AgentResult, AgentStatus, BillingCompleteness, CleanupPhase,
    EvalArtifacts, EvalAttemptOutcome, EvalCleanup, EvalEnvironment, EvalEvent, EvalEventKind,
    EvalEvents, EvalException, EvalExceptionKind, EvalFailure, EvalFailureTiming, EvalOutcome,
    EvalResult, EvalStatus, EvalTiming, PhaseTiming, Sweep, SweepAttemptResult, SweepResults, Task,
    TaskLoadError, UsageTotals, VerifierResult,
    codex::{CodexExec, CodexRunError},
    job::EvalJob,
    native::{NativeAttempt, VerifierExecution},
};

const EVENT_CAPACITY: usize = 16_384;
// A healthy driver normally acknowledges shutdown and emits its retained
// terminal event immediately. Ten seconds bounds how long the evaluator waits
// for that optional terminal snapshot. Resource shutdown remains a mandatory
// join after this deadline: a broken driver quarantines its admission lane
// instead of racing a verifier against live agent work.
const AGENT_CANCELLATION_GRACE: Duration = Duration::from_secs(10);
// One warmup plus three typical four-call attempts stays below the provider's
// approximate 15-request-per-minute routing guidance for a cache key.
const PROMPT_CACHE_COHORT_SIZE: u64 = 3;
const ESTIMATED_LOWER_BOUND_COST_STATUS: &str = "estimated_lower_bound";

/// A reusable evaluation recipe. Every task call creates an independent agent
/// session and disposable workspace.
#[derive(Clone)]
pub struct Evaluator {
    inner: Arc<EvaluatorInner>,
}

/// Deliberate evaluator policy configured before running tasks.
pub struct EvaluatorBuilder {
    nanocodex: NanocodexBuilder,
    output_directory: PathBuf,
    max_concurrency: usize,
    max_memory_mb: Option<u64>,
    attempt_environment: EvalEnvironment,
    attempt_agent: Option<AttemptAgentFactory>,
    finite_run: Option<FiniteRun>,
    #[cfg(test)]
    malformed_terminal_metrics: bool,
}

struct EvaluatorInner {
    nanocodex: NanocodexBuilder,
    job: EvalJob,
    planned_attempts: Option<usize>,
    admission: Arc<AdmissionController>,
    max_concurrency: usize,
    max_memory_mb: Option<u64>,
    attempt_environment: EvalEnvironment,
    next_prompt_cache_attempt: AtomicU64,
    events: broadcast::Sender<Arc<EvalEvent>>,
    attempt_agent: Option<AttemptAgentFactory>,
    #[cfg(test)]
    malformed_terminal_metrics: bool,
}

struct AdmissionController {
    max_concurrency: usize,
    max_memory_mb: Option<u64>,
    state: Mutex<AdmissionState>,
    changed: Notify,
}

#[derive(Default)]
struct AdmissionState {
    running: usize,
    memory_mb: u64,
    admitted: usize,
    draining: bool,
}

struct AdmissionPermit {
    controller: Arc<AdmissionController>,
    memory_mb: u64,
}

struct FiniteRun {
    manifest: crate::sweep::RunManifest,
    mode: FiniteRunMode,
}

#[derive(Clone, Copy)]
enum FiniteRunMode {
    Fresh,
    Resume,
}

type AttemptError = Box<dyn Error + Send + Sync + 'static>;
type AttemptAgentFactory = Arc<
    dyn for<'a> Fn(EvalAttempt<'a>, NanocodexBuilder) -> Result<AttemptAgent, AttemptError>
        + Send
        + Sync
        + 'static,
>;

type AttemptVerifierFuture<'a> = Pin<
    Box<dyn Future<Output = Result<AttemptVerification, AttemptVerificationFailure>> + Send + 'a>,
>;
type AttemptVerifierCleanupFuture<'a> = Pin<Box<dyn Future<Output = CleanupPhase> + Send + 'a>>;
type AttemptReadinessFuture =
    Pin<Box<dyn Future<Output = Result<(), AttemptError>> + Send + 'static>>;

/// The Nanocodex configuration and resources owned by one attempt.
pub struct AttemptAgent {
    driver: AttemptDriver,
    readiness: Option<AttemptReadinessFuture>,
    verifier: Option<Box<dyn AttemptVerifier>>,
}

enum AttemptDriver {
    Nanocodex(NanocodexBuilder),
    Codex(CodexExec),
}

/// A verifier that runs against the same retained environment as the agent.
pub trait AttemptVerifier: Send {
    /// Verifies one completed agent attempt.
    ///
    /// The returned future may borrow the verifier, task, and attempt for its
    /// complete execution. Failures are retained as typed evaluation errors.
    fn verify<'a>(
        &'a mut self,
        task: &'a Task,
        attempt: EvalAttempt<'a>,
    ) -> AttemptVerifierFuture<'a>;

    /// Explicitly joins verifier-owned resources when verification will not run.
    ///
    /// Implementations that own processes, VMs, mounts, or other asynchronous
    /// resources must override this method. The evaluator awaits it on every
    /// post-construction abort path.
    fn shutdown(&mut self) -> AttemptVerifierCleanupFuture<'_> {
        Box::pin(async { CleanupPhase::not_required() })
    }
}

/// A verifier's primary semantic error plus independently retained cleanup.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct AttemptVerificationFailure {
    #[source]
    error: AttemptError,
    occurred_at: DateTime<Utc>,
    /// Cleanup health observed after the primary verification failure.
    pub cleanup: CleanupPhase,
}

impl AttemptVerificationFailure {
    /// Retains a verifier error and the cleanup attempted after it.
    pub fn new(error: impl Error + Send + Sync + 'static, cleanup: CleanupPhase) -> Self {
        let occurred_at = cleanup
            .timing
            .as_ref()
            .map_or_else(Utc::now, |timing| timing.started_at);
        Self {
            error: Box::new(error),
            occurred_at,
            cleanup,
        }
    }

    /// Retains an error timestamp captured before asynchronous cleanup began.
    pub fn observed_at(
        error: impl Error + Send + Sync + 'static,
        occurred_at: DateTime<Utc>,
        cleanup: CleanupPhase,
    ) -> Self {
        Self {
            error: Box::new(error),
            occurred_at,
            cleanup,
        }
    }

    fn into_parts(self) -> (AttemptError, DateTime<Utc>, CleanupPhase) {
        (self.error, self.occurred_at, self.cleanup)
    }
}

/// Complete typed output returned by an attempt-owned verifier.
pub struct AttemptVerification {
    /// Process-equivalent exit status and named rewards.
    pub result: VerifierResult,
    /// Complete captured verifier standard output.
    pub stdout: String,
    /// Complete captured verifier standard error.
    pub stderr: String,
    /// Attempt-owned verifier cleanup health and timing.
    pub cleanup: CleanupPhase,
}

struct AttemptInput {
    task: Task,
    nanocodex: NanocodexBuilder,
    coordinate: Option<SweepCoordinate>,
    queued_at: DateTime<Utc>,
}

struct AttemptOutput {
    outcome: EvalAttemptOutcome,
    coordinate: Option<SweepCoordinate>,
}

#[derive(Clone)]
struct SweepCoordinate {
    agent: AgentId,
    trial: u16,
}

/// Immutable paths and task metadata available while configuring one attempt.
#[derive(Clone, Copy)]
pub struct EvalAttempt<'a> {
    task: &'a Task,
    directory: &'a Path,
    workspace: &'a Path,
}

/// Failure to configure, execute, verify, or durably retain an attempt.
#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    /// Configured concurrency was zero.
    #[error("maximum concurrency must be greater than zero")]
    InvalidConcurrency,

    /// Configured aggregate memory was zero.
    #[error("maximum task memory must be greater than zero")]
    InvalidMemory,

    /// The evaluator stopped admitting new attempts while draining.
    #[error("evaluation is draining and no longer admits new attempts")]
    Draining,

    /// A task requires behavior unavailable in the native backend.
    #[error("task {task} cannot run with the native backend: {reason}")]
    UnsupportedNativeTask {
        /// Stable task name.
        task: String,
        /// Unsupported task requirement.
        reason: &'static str,
    },

    /// Filesystem or process I/O failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A loaded task package changed before an attempt could use it.
    #[error(transparent)]
    TaskPackage(#[from] TaskLoadError),

    /// Retained evaluation output would mutate a hashed task package.
    #[error("evaluation output {output} must not be nested in task package {task}")]
    OutputOverlapsTask {
        /// Prospective canonical output parent.
        output: PathBuf,
        /// Canonical task package root.
        task: PathBuf,
    },

    /// Agent setup or execution failed.
    #[error("Nanocodex failed: {0}")]
    Nanocodex(#[from] NanocodexError),

    /// Agent execution completed but explicit resource cleanup failed.
    #[error("Nanocodex cleanup failed: {0}")]
    AgentCleanup(#[source] NanocodexError),

    /// A pinned stock-Codex child process failed.
    #[error("Codex failed: {0}")]
    Codex(#[source] crate::CodexExecError),

    /// The attempt backend factory failed.
    #[error("failed to configure attempt agent: {0}")]
    AttemptAgent(#[source] AttemptError),

    /// An attempt-owned verifier failed.
    #[error("attempt verifier failed: {0}")]
    AttemptVerifier(#[source] AttemptError),

    /// Agent execution exceeded the task deadline.
    #[error("agent exceeded its {0:?} timeout")]
    AgentTimeout(Duration),

    /// Verifier execution exceeded the task deadline.
    #[error("verifier exceeded its {0:?} timeout")]
    VerifierTimeout(Duration),

    /// The agent firehose ended without a terminal event.
    #[error("agent event stream closed before a terminal event")]
    AgentEventsClosed,

    /// The agent emitted a terminal event whose typed metrics were invalid.
    #[error("failed to decode agent terminal metrics: {0}")]
    AgentTerminal(#[source] serde_json::Error),

    /// Typed artifact JSON could not be encoded or decoded.
    #[error("failed to encode or decode JSON: {0}")]
    Json(#[from] serde_json::Error),

    /// An existing job is bound to a different sweep manifest.
    #[error("evaluation job is already bound to a different run: {0}")]
    RunConflict(PathBuf),

    /// A resumable job uses an incompatible task digest algorithm.
    #[error(
        "evaluation job {path} uses task digest schema `{found}`; expected `{expected}`; \
         start an explicit fresh run to cross this recovery boundary"
    )]
    RunDigestSchemaIncompatible {
        /// Retained job whose task identity cannot be compared safely.
        path: PathBuf,
        /// Retained schema label.
        found: String,
        /// Current schema label.
        expected: String,
    },

    /// A retained terminal result did not belong to its finite run.
    #[error("invalid durable evaluation trial: {0}")]
    InvalidDurableTrial(String),

    /// Another process still owns the matching resumable job.
    #[error("matching incomplete evaluation job is already active: {0}")]
    RunActive(PathBuf),

    /// A verifier emitted an invalid numeric reward.
    #[error("invalid verifier reward: {0}")]
    ParseReward(#[from] ParseFloatError),

    /// Internal sweep execution lost its stable coordinate.
    #[error("sweep execution lost its task-agent-trial coordinates")]
    MissingSweepCoordinate,
}

impl Evaluator {
    /// Starts an evaluator builder from a reusable Nanocodex recipe.
    ///
    /// Every attempt receives an independent session and workspace. The recipe
    /// automatically shares only its immutable prompt-cache warmup.
    #[must_use]
    pub fn builder(nanocodex: NanocodexBuilder) -> EvaluatorBuilder {
        EvaluatorBuilder {
            nanocodex: nanocodex.shared_prompt_cache(),
            output_directory: PathBuf::from(".nanocodex/evals"),
            max_concurrency: 1,
            max_memory_mb: None,
            attempt_environment: EvalEnvironment::Native,
            attempt_agent: None,
            finite_run: None,
            #[cfg(test)]
            malformed_terminal_metrics: false,
        }
    }

    /// Runs one independent attempt.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the attempt cannot be admitted.
    /// Accepted setup, agent, and verifier failures are returned as typed
    /// [`EvalAttemptOutcome::Unscored`] values.
    pub async fn task(&self, task: Task) -> Result<EvalAttemptOutcome, EvalError> {
        let queued_at = Utc::now();
        let _permit = self
            .inner
            .admission
            .acquire(task.resources().memory_mb)
            .await
            .ok_or(EvalError::Draining)?;
        self.run_task(AttemptInput {
            task,
            nanocodex: self.inner.nanocodex.clone(),
            coordinate: None,
            queued_at,
        })
        .await
        .map(|output| output.outcome)
    }

    /// Runs `count` fresh attempts of the same immutable task.
    ///
    /// Results preserve attempt order even when work completes out of order.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the batch cannot be scheduled or
    /// retained. Attempt failures remain in their original positions.
    pub async fn task_n(
        &self,
        task: Task,
        count: usize,
    ) -> Result<Vec<EvalAttemptOutcome>, EvalError> {
        self.tasks(std::iter::repeat_n(task, count).collect()).await
    }

    /// Runs one independent attempt for every task in `tasks`.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the batch cannot be scheduled or
    /// retained. Attempt failures remain in their original positions.
    pub async fn tasks(&self, tasks: Vec<Task>) -> Result<Vec<EvalAttemptOutcome>, EvalError> {
        let inputs = tasks
            .into_iter()
            .map(|task| AttemptInput {
                task,
                nanocodex: self.inner.nanocodex.clone(),
                coordinate: None,
                queued_at: Utc::now(),
            })
            .collect();
        Ok(self
            .run_tasks(inputs)
            .await?
            .into_iter()
            .map(|output| output.outcome)
            .collect())
    }

    /// Runs `count` fresh attempts for every task in `tasks`.
    ///
    /// Results are grouped in input task order and then trial order.
    ///
    /// # Errors
    ///
    /// Returns an operational error when the batch cannot be scheduled or
    /// retained. Attempt failures remain in their original positions.
    pub async fn tasks_n(
        &self,
        tasks: Vec<Task>,
        count: usize,
    ) -> Result<Vec<EvalAttemptOutcome>, EvalError> {
        self.tasks(
            tasks
                .into_iter()
                .flat_map(|task| std::iter::repeat_n(task, count))
                .collect(),
        )
        .await
    }

    /// Runs an advanced finite task-by-agent-by-trial sweep.
    ///
    /// # Errors
    ///
    /// Returns an operational error when run binding or durable recovery fails.
    /// Every accepted task × agent × trial coordinate is returned, including
    /// unscored attempts.
    pub async fn sweep(&self, sweep: Sweep) -> Result<SweepResults, EvalError> {
        let manifest = sweep.manifest();
        self.inner.job.bind_run(&manifest)?;
        let completed = self.inner.job.completed_coordinates(&manifest)?;
        let mut skipped = 0;
        let mut inputs = Vec::new();
        for attempt in sweep.attempts() {
            if completed.contains(&attempt.coordinate()) {
                skipped += 1;
                continue;
            }
            inputs.push(AttemptInput {
                task: attempt.task().clone(),
                nanocodex: attempt.nanocodex().clone(),
                coordinate: Some(SweepCoordinate {
                    agent: attempt.agent_id().clone(),
                    trial: attempt.trial(),
                }),
                queued_at: Utc::now(),
            });
        }
        let attempts = self
            .run_tasks(inputs)
            .await?
            .into_iter()
            .map(|output| {
                let coordinate = output.coordinate.ok_or(EvalError::MissingSweepCoordinate)?;
                Ok(SweepAttemptResult::new(
                    coordinate.agent,
                    coordinate.trial,
                    output.outcome,
                ))
            })
            .collect::<Result<Vec<_>, EvalError>>()?;
        Ok(SweepResults::new(attempts, skipped))
    }

    /// Returns how many attempts in `sweep` do not yet have a durable terminal
    /// result in this evaluator's job directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the job is bound to another sweep or its retained
    /// artifacts cannot be inspected.
    pub fn remaining_attempts(&self, sweep: &Sweep) -> Result<usize, EvalError> {
        let manifest = sweep.manifest();
        self.inner.job.bind_run(&manifest)?;
        let completed = self.inner.job.completed_coordinates(&manifest)?;
        let mut remaining = 0;
        for attempt in sweep.attempts() {
            if !completed.contains(&attempt.coordinate()) {
                remaining += 1;
            }
        }
        Ok(remaining)
    }

    async fn run_tasks(&self, tasks: Vec<AttemptInput>) -> Result<Vec<AttemptOutput>, EvalError> {
        let scheduling_window = tasks
            .len()
            .min(self.inner.max_concurrency.saturating_mul(4))
            .max(1);
        let evaluator = self.clone();
        let mut completed = stream::iter(tasks.into_iter().enumerate())
            .map(move |(index, input)| {
                let evaluator = evaluator.clone();
                async move {
                    let _permit = evaluator
                        .inner
                        .admission
                        .acquire(input.task.resources().memory_mb)
                        .await?;
                    let result = evaluator.run_task(input).await;
                    Some((index, result))
                }
            })
            .buffer_unordered(scheduling_window);
        let mut results = Vec::new();
        while let Some(output) = completed.next().await {
            let Some((index, result)) = output else {
                continue;
            };
            results.push((index, result?));
        }
        results.sort_unstable_by_key(|(index, _)| *index);
        Ok(results.into_iter().map(|(_, result)| result).collect())
    }

    /// Returns the stable identifier shared by this evaluator's attempts.
    #[must_use]
    pub fn id(&self) -> Uuid {
        self.inner.job.id()
    }

    /// Returns when this evaluator was built.
    #[must_use]
    pub fn started_at(&self) -> DateTime<Utc> {
        self.inner.job.started_at()
    }

    /// Returns the directory containing this evaluator's attempt artifacts.
    #[must_use]
    pub fn directory(&self) -> &std::path::Path {
        self.inner.job.directory()
    }

    /// Returns the parent directory containing evaluation jobs.
    #[must_use]
    pub fn parent_directory(&self) -> &std::path::Path {
        self.inner.job.parent_directory()
    }

    /// Returns whether this evaluator reopened an incomplete matching job.
    #[must_use]
    pub fn resumed(&self) -> bool {
        self.inner.job.resumed()
    }

    /// Returns the finite attempt count fixed by `resume_incomplete`, when set.
    #[must_use]
    pub fn planned_attempts(&self) -> Option<usize> {
        self.inner.planned_attempts
    }

    /// Returns the maximum number of concurrently executing attempts.
    #[must_use]
    pub fn max_concurrency(&self) -> usize {
        self.inner.max_concurrency
    }

    /// Returns the optional ceiling on concurrently admitted task-declared
    /// memory.
    #[must_use]
    pub fn max_memory_mb(&self) -> Option<u64> {
        self.inner.max_memory_mb
    }

    /// Stops admission of attempts that have not started and returns the number
    /// admitted since this evaluator was built.
    ///
    /// Attempts that already hold admission continue normally. Repeated calls
    /// are idempotent and return the same final admitted count.
    pub fn begin_drain(&self) -> usize {
        self.inner.admission.begin_drain()
    }

    /// Returns the execution environment selected for every attempt.
    #[must_use]
    pub fn attempt_environment(&self) -> EvalEnvironment {
        self.inner.attempt_environment
    }

    async fn run_task(&self, input: AttemptInput) -> Result<AttemptOutput, EvalError> {
        let AttemptInput {
            task,
            nanocodex,
            coordinate,
            queued_at,
        } = input;
        let session_id = SessionId::new();
        let attempt_id = session_id.as_uuid();
        let prompt_cache_cohort = self
            .inner
            .next_prompt_cache_attempt
            .fetch_add(1, Ordering::Relaxed)
            / PROMPT_CACHE_COHORT_SIZE;
        let trial_name = trial_name(&task, attempt_id, coordinate.as_ref());
        let admitted_at = Utc::now();
        let queue_wait = PhaseTiming {
            started_at: queued_at,
            finished_at: admitted_at,
        };
        let started_at = queued_at;
        let mut emitter =
            AttemptEmitter::new(self, session_id, prompt_cache_cohort, &task, &trial_name);
        let span = attempt_span(
            self,
            &task,
            attempt_id,
            &trial_name,
            prompt_cache_cohort,
            coordinate.as_ref(),
        );
        record_content(&span, "task.prompt", task.prompt());
        let trace_started = Instant::now();
        let result = self
            .run_task_inner(
                task.clone(),
                nanocodex,
                attempt_id,
                trial_name.clone(),
                queue_wait.clone(),
                &mut emitter,
            )
            .instrument(span.clone())
            .await;
        record_attempt_result(&span, trace_started, &result);
        let outcome = match result {
            Ok(result) => EvalAttemptOutcome::Scored(result),
            Err(failure) => {
                let failure = attempt_failure(
                    self, attempt_id, task, trial_name, started_at, queue_wait, &failure,
                );
                emitter.emit(EvalEventKind::Failed(Box::new(failure.clone())));
                EvalAttemptOutcome::Unscored(failure)
            }
        };
        Ok(AttemptOutput {
            outcome,
            coordinate,
        })
    }

    async fn run_task_inner(
        &self,
        task: Task,
        nanocodex: NanocodexBuilder,
        attempt_id: Uuid,
        trial_name: String,
        queue_wait: PhaseTiming,
        emitter: &mut AttemptEmitter<'_>,
    ) -> Result<EvalResult, AttemptRunFailure> {
        reject_output_overlap(self.inner.job.parent_directory(), task.root())
            .map_err(AttemptRunFailure::new)?;
        task.validate_package()
            .map_err(|error| AttemptRunFailure::new(EvalError::TaskPackage(error)))?;
        let attempt = {
            let span = info_span!(
                target: "nanocodex_eval",
                "eval.environment.setup",
                otel.kind = "internal",
                otel.status_code = tracing::field::Empty,
                eval.task.name = task.name(),
                eval.trial.name = trial_name.as_str(),
                output.directory = %self.inner.job.directory().display(),
                status = tracing::field::Empty,
                error.message = tracing::field::Empty,
                duration_ns = tracing::field::Empty,
            );
            let trace_started = Instant::now();
            let result = span.in_scope(|| {
                validate_attempt_environment(&task, self.inner.attempt_agent.is_some())?;
                NativeAttempt::prepare(self.inner.job.directory(), &trial_name, &task)
            });
            record_span_result(&span, trace_started, &result);
            result.map_err(AttemptRunFailure::new)?
        };
        emitter.emit(EvalEventKind::AttemptStarted {
            prompt: task.prompt().to_owned(),
            workspace: attempt.paths.workspace.clone(),
        });
        let mut agent = self
            .execute_agent(emitter, &task, &attempt, nanocodex)
            .await
            .map_err(|failure| AttemptRunFailure::from_agent(&attempt, failure))?;

        if let Err(error) = task.validate_package() {
            let error = RecordedEvalError::now(EvalError::TaskPackage(error));
            let verifier_cleanup = shutdown_attempt_verifier(&mut agent.verifier).await;
            return Err(AttemptRunFailure::after_agent(
                &attempt,
                &agent,
                error,
                verifier_cleanup,
            ));
        }
        if agent
            .error
            .as_ref()
            .is_some_and(|error| !verifier_workspace_usable_after_agent_error(&error.error))
        {
            let verifier_cleanup = shutdown_attempt_verifier(&mut agent.verifier).await;
            let error = agent
                .error
                .take()
                .unwrap_or_else(|| RecordedEvalError::now(EvalError::AgentEventsClosed));
            return Err(AttemptRunFailure::after_agent(
                &attempt,
                &agent,
                error,
                verifier_cleanup,
            ));
        }
        emitter.emit(EvalEventKind::VerifierStarted);
        let verifier = match self
            .execute_verifier(&task, &attempt, agent.verifier.take())
            .await
        {
            Ok(verifier) => verifier,
            Err(failure) => {
                let primary = agent.error.take();
                return Err(AttemptRunFailure::after_verifier_failure(
                    &attempt, &agent, primary, failure,
                ));
            }
        };
        if let Err(error) = task.validate_package() {
            return Err(AttemptRunFailure::after_verifier(
                &attempt,
                &agent,
                &verifier,
                RecordedEvalError::now(EvalError::TaskPackage(error)),
            ));
        }
        emitter.emit(EvalEventKind::VerifierOutput {
            stdout: verifier.stdout.clone(),
            stderr: verifier.stderr.clone(),
        });
        emitter.emit(EvalEventKind::VerifierCompleted(verifier.result.clone()));

        let status = verifier_status(&verifier.result);
        let score_outcome = match status {
            EvalStatus::Passed => EvalOutcome::Passed,
            EvalStatus::Failed => EvalOutcome::VerifierFailed,
        };
        let exception = agent
            .error
            .as_ref()
            .map(|error| eval_exception(&error.error, error.occurred_at));
        let result = EvalResult {
            attempt_id,
            task_name: task.name().to_owned(),
            trial_name,
            status,
            outcome: exception
                .as_ref()
                .map_or(score_outcome, |exception| exception.outcome),
            environment: self.inner.attempt_environment,
            agent: agent.result,
            verifier: verifier.result,
            exception,
            timing: EvalTiming {
                started_at: queue_wait.started_at,
                finished_at: Utc::now(),
                queue_wait,
                environment_setup: attempt.setup_timing.clone(),
                environment_readiness: agent.readiness_timing,
                agent_setup: agent.setup_timing,
                agent_execution: agent.execution_timing,
                verifier: verifier.timing,
            },
            cleanup: EvalCleanup {
                agent: agent.cleanup,
                verifier: verifier.cleanup,
            },
            artifacts: EvalArtifacts {
                directory: attempt.paths.root.clone(),
                workspace: attempt.paths.workspace.clone(),
                verifier_output: attempt.paths.verifier_output.clone(),
            },
            task,
        };
        emitter.emit(EvalEventKind::Completed(Box::new(result.clone())));
        Ok(result)
    }

    async fn execute_verifier(
        &self,
        task: &Task,
        attempt: &NativeAttempt,
        verifier: Option<Box<dyn AttemptVerifier>>,
    ) -> Result<VerifierExecution, VerifierExecutionFailure> {
        let span = info_span!(
            target: "nanocodex_eval",
            "eval.verifier",
            otel.kind = "internal",
            otel.status_code = tracing::field::Empty,
            eval.task.name = task.name(),
            verifier.script = %task.verifier().script().display(),
            verifier.timeout_ms = duration_ms(task.verifier().timeout()),
            process.exit.code = tracing::field::Empty,
            verifier.reward.total = tracing::field::Empty,
            verifier.passed = tracing::field::Empty,
            verifier.stdout.bytes = tracing::field::Empty,
            verifier.stderr.bytes = tracing::field::Empty,
            status = tracing::field::Empty,
            error.message = tracing::field::Empty,
            duration_ns = tracing::field::Empty,
        );
        let trace_started = Instant::now();
        let result = async {
            if let Some(mut verifier) = verifier {
                let started_at = Utc::now();
                let execution = match verifier
                    .verify(
                        task,
                        EvalAttempt {
                            task,
                            directory: &attempt.paths.root,
                            workspace: &attempt.paths.workspace,
                        },
                    )
                    .await
                {
                    Ok(execution) => execution,
                    Err(failure) => {
                        let (error, occurred_at, cleanup) = failure.into_parts();
                        let finished_at = cleanup
                            .timing
                            .as_ref()
                            .map_or_else(Utc::now, |timing| timing.started_at);
                        return Err(VerifierExecutionFailure {
                            error: RecordedEvalError {
                                error: EvalError::AttemptVerifier(error),
                                occurred_at,
                            },
                            cleanup,
                            timing: Some(PhaseTiming {
                                started_at,
                                finished_at,
                            }),
                        });
                    }
                };
                Ok(VerifierExecution {
                    result: execution.result,
                    timing: PhaseTiming {
                        started_at,
                        finished_at: execution
                            .cleanup
                            .timing
                            .as_ref()
                            .map_or_else(Utc::now, |timing| timing.started_at),
                    },
                    stdout: execution.stdout,
                    stderr: execution.stderr,
                    cleanup: execution.cleanup,
                })
            } else {
                let started_at = Utc::now();
                attempt
                    .verify(task)
                    .await
                    .map_err(|error| VerifierExecutionFailure {
                        error: RecordedEvalError::now(error),
                        cleanup: CleanupPhase::not_required(),
                        timing: Some(PhaseTiming::finished(started_at)),
                    })
            }
        }
        .instrument(span.clone())
        .await;
        if let Ok(verifier) = &result {
            let passed = verifier.result.rewards.values().all(|reward| *reward > 0.0);
            span.record("process.exit.code", verifier.result.exit_code);
            span.record(
                "verifier.reward.total",
                verifier.result.rewards.values().sum::<f64>(),
            );
            span.record("verifier.passed", passed);
            span.record("verifier.stdout.bytes", verifier.stdout.len());
            span.record("verifier.stderr.bytes", verifier.stderr.len());
            record_content(&span, "verifier.stdout", &verifier.stdout);
            record_content(&span, "verifier.stderr", &verifier.stderr);
        }
        record_span_result(&span, trace_started, &result);
        result
    }

    async fn execute_agent(
        &self,
        emitter: &mut AttemptEmitter<'_>,
        task: &Task,
        attempt: &NativeAttempt,
        nanocodex: NanocodexBuilder,
    ) -> Result<AgentExecution, AgentExecutionFailure> {
        let AgentSetup {
            agent,
            verifier,
            readiness_timing,
            timing: setup_timing,
        } = self.setup_agent(emitter, task, attempt, nanocodex).await?;
        match agent {
            PreparedAgent::Nanocodex { agent, events } => {
                self.execute_nanocodex_agent(
                    emitter,
                    task,
                    agent,
                    events,
                    verifier,
                    readiness_timing,
                    setup_timing,
                )
                .await
            }
            PreparedAgent::Codex(codex) => {
                self.execute_codex_agent(
                    emitter,
                    task,
                    attempt,
                    codex,
                    verifier,
                    readiness_timing,
                    setup_timing,
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_nanocodex_agent(
        &self,
        emitter: &mut AttemptEmitter<'_>,
        task: &Task,
        agent: Nanocodex,
        mut events: AgentEvents,
        verifier: Option<Box<dyn AttemptVerifier>>,
        readiness_timing: PhaseTiming,
        setup_timing: PhaseTiming,
    ) -> Result<AgentExecution, AgentExecutionFailure> {
        let execution_started = Utc::now();
        let span = info_span!(
            target: "nanocodex_eval",
            "eval.agent.execution",
            otel.kind = "internal",
            otel.status_code = tracing::field::Empty,
            eval.task.name = task.name(),
            eval.attempt.id = %emitter.attempt_id,
            agent.timeout_ms = duration_ms(task.agent_timeout()),
            status = tracing::field::Empty,
            error.message = tracing::field::Empty,
            duration_ns = tracing::field::Empty,
        );
        let trace_started = Instant::now();
        let result = async {
            let turn = agent.prompt(task.prompt()).await?;
            let mut observation = AgentObservation::default();
            let event_result = timeout(
                task.agent_timeout(),
                receive_agent_terminal(&mut events, emitter, &mut observation),
            )
            .await;
            match event_result {
                Ok(Ok(terminal)) => {
                    #[cfg(test)]
                    let terminal = {
                        let mut terminal = terminal;
                        if self.inner.malformed_terminal_metrics {
                            terminal.payload = Arc::from(
                                serde_json::value::to_raw_value(
                                    &serde_json::json!({"malformed": true}),
                                )
                                .map_err(EvalError::Json)?,
                            );
                        }
                        terminal
                    };
                    let (primary, final_message) = match turn.result().await {
                        Ok(result) => (None, result.into_final_message()),
                        Err(error) => (
                            Some(EvalError::Nanocodex(error)),
                            observation.final_message.clone(),
                        ),
                    };
                    let completeness = observation.billing_completeness();
                    observation.final_message = final_message;
                    let selection = observation.select_result(Some(&terminal), completeness);
                    if let Some(error) = &selection.terminal_error {
                        tracing::warn!(
                            target: "nanocodex_eval",
                            error = %error,
                            "failed to decode terminal agent metrics; retaining \
                             completed-operation lower bound"
                        );
                    }
                    let primary = primary
                        .map(RecordedEvalError::now)
                        .or_else(|| selection.terminal_error.map(RecordedEvalError::now));
                    Ok(AgentRunState::Finished(AgentTurnOutcome {
                        primary,
                        result: selection.result,
                        result_is_lower_bound: selection.used_lower_bound,
                    }))
                }
                Ok(Err(error)) => {
                    let selection = observation.select_result(None, BillingCompleteness::Unknown);
                    Ok(AgentRunState::Finished(AgentTurnOutcome {
                        primary: Some(RecordedEvalError::now(error)),
                        result: selection.result,
                        result_is_lower_bound: selection.used_lower_bound,
                    }))
                }
                Err(_) => {
                    let primary =
                        RecordedEvalError::now(EvalError::AgentTimeout(task.agent_timeout()));
                    Ok(AgentRunState::TimedOut {
                        primary,
                        observation,
                    })
                }
            }
        };
        let result = result.instrument(span.clone()).await;
        record_span_result(&span, trace_started, &result);
        let mut result = result.map_err(RecordedEvalError::now);
        let execution_timing = PhaseTiming::finished(execution_started);
        if let Ok(AgentRunState::Finished(outcome)) = &mut result {
            outcome.apply_lower_bound_duration(phase_timing_ns(&execution_timing));
        }
        let cleanup_started = Utc::now();
        let (outcome, cleanup) = match result {
            Ok(AgentRunState::Finished(outcome)) => {
                let shutdown = agent.shutdown().await;
                let cleanup = match shutdown {
                    Ok(()) => CleanupPhase::completed(cleanup_started),
                    Err(error) => CleanupPhase::failed(cleanup_started, &error),
                };
                (outcome, cleanup)
            }
            Ok(AgentRunState::TimedOut {
                primary,
                mut observation,
            }) => {
                let recovery = recover_timed_out_agent(
                    AGENT_CANCELLATION_GRACE,
                    agent.shutdown(),
                    receive_agent_terminal(&mut events, emitter, &mut observation),
                )
                .await;
                let cleanup = match recovery.shutdown {
                    Ok(()) => CleanupPhase::completed(cleanup_started),
                    Err(error) => CleanupPhase::failed(cleanup_started, &error),
                };
                if recovery.grace_elapsed && recovery.terminal.is_none() {
                    tracing::warn!(
                        target: "nanocodex_eval",
                        grace_ms = duration_ms(AGENT_CANCELLATION_GRACE),
                        primary_error = %primary.error,
                        "agent terminal recovery exceeded its private grace; \
                         resource shutdown remained joined"
                    );
                }
                let completeness = observation.billing_completeness();
                let terminal = match recovery.terminal {
                    Some(Ok(terminal)) => Some(terminal),
                    Some(Err(error)) => {
                        tracing::warn!(
                            target: "nanocodex_eval",
                            error = %error,
                            primary_error = %primary.error,
                            "agent events closed without a terminal snapshot after timeout; \
                             retaining completed-operation lower bound"
                        );
                        None
                    }
                    None => None,
                };
                let selection = observation.select_result(terminal.as_ref(), completeness);
                if let Some(error) = &selection.terminal_error {
                    tracing::warn!(
                        target: "nanocodex_eval",
                        error = %error,
                        primary_error = %primary.error,
                        "failed to decode terminal metrics after agent timeout; retaining \
                         completed-operation lower bound"
                    );
                }
                let outcome = AgentTurnOutcome {
                    primary: Some(primary),
                    result: selection.result,
                    result_is_lower_bound: selection.used_lower_bound,
                };
                let mut outcome = outcome;
                outcome.apply_lower_bound_duration(phase_timing_ns(&execution_timing));
                (outcome, cleanup)
            }
            Err(error) => {
                let shutdown = agent.shutdown().await;
                let cleanup = match shutdown {
                    Ok(()) => CleanupPhase::completed(cleanup_started),
                    Err(error) => CleanupPhase::failed(cleanup_started, &error),
                };
                (
                    AgentTurnOutcome {
                        primary: Some(error),
                        result: None,
                        result_is_lower_bound: false,
                    },
                    cleanup,
                )
            }
        };
        let error = outcome.primary.or_else(|| {
            outcome
                .result
                .is_none()
                .then(|| RecordedEvalError::now(EvalError::AgentEventsClosed))
        });
        Ok(AgentExecution {
            result: outcome.result,
            error,
            verifier,
            readiness_timing,
            setup_timing,
            execution_timing,
            cleanup,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_codex_agent(
        &self,
        emitter: &AttemptEmitter<'_>,
        task: &Task,
        attempt: &NativeAttempt,
        codex: CodexExec,
        verifier: Option<Box<dyn AttemptVerifier>>,
        readiness_timing: PhaseTiming,
        setup_timing: PhaseTiming,
    ) -> Result<AgentExecution, AgentExecutionFailure> {
        let execution_started = Utc::now();
        let span = info_span!(
            target: "nanocodex_eval",
            "eval.agent.execution",
            otel.kind = "internal",
            otel.status_code = tracing::field::Empty,
            eval.task.name = task.name(),
            eval.attempt.id = %emitter.attempt_id,
            agent.kind = "stock_codex_cli",
            agent.timeout_ms = duration_ms(task.agent_timeout()),
            status = tracing::field::Empty,
            error.message = tracing::field::Empty,
            duration_ns = tracing::field::Empty,
        );
        let trace_started = Instant::now();
        let execution = codex
            .run(
                &attempt.paths.workspace,
                &attempt.paths.root,
                task.prompt(),
                task.agent_timeout(),
            )
            .instrument(span.clone())
            .await;
        let execution_timing = PhaseTiming::finished(execution_started);
        let error = execution.error.map(|error| {
            RecordedEvalError::now(match error {
                CodexRunError::Timeout(timeout) => EvalError::AgentTimeout(timeout),
                CodexRunError::Execution(error) => EvalError::Codex(error),
            })
        });
        if let Some(error) = &error {
            span.record("status", "failed");
            span.record("otel.status_code", "ERROR");
            span.record("error.message", error.error.to_string());
        } else {
            span.record("status", "completed");
            span.record("otel.status_code", "OK");
        }
        span.record("duration_ns", elapsed_ns(trace_started));
        Ok(AgentExecution {
            result: execution.result,
            error,
            verifier,
            readiness_timing,
            setup_timing,
            execution_timing,
            cleanup: execution.cleanup,
        })
    }

    async fn setup_agent(
        &self,
        emitter: &AttemptEmitter<'_>,
        task: &Task,
        attempt: &NativeAttempt,
        nanocodex: NanocodexBuilder,
    ) -> Result<AgentSetup, AgentExecutionFailure> {
        let readiness_started = Utc::now();
        let span = info_span!(
            target: "nanocodex_eval",
            "eval.agent.setup",
            otel.kind = "internal",
            otel.status_code = tracing::field::Empty,
            eval.task.name = task.name(),
            eval.attempt.id = %emitter.attempt_id,
            workspace = %attempt.paths.workspace.display(),
            status = tracing::field::Empty,
            error.message = tracing::field::Empty,
            duration_ns = tracing::field::Empty,
        );
        let trace_started = Instant::now();
        let result = async {
            let builder = nanocodex
                .workspace(&attempt.paths.workspace)
                .session_id(emitter.session_id)
                .prompt_cache_key(format!(
                    "nanoeval:{}:{:x}",
                    self.id().simple(),
                    emitter.prompt_cache_cohort
                ));
            let configured = if let Some(factory) = &self.inner.attempt_agent {
                match factory(
                    EvalAttempt {
                        task,
                        directory: &attempt.paths.root,
                        workspace: &attempt.paths.workspace,
                    },
                    builder,
                ) {
                    Ok(configured) => configured,
                    Err(error) => {
                        let error = RecordedEvalError::now(EvalError::AttemptAgent(error));
                        return Err(AgentExecutionFailure::setup(
                            error,
                            CleanupPhase::not_required(),
                            None,
                        ));
                    }
                }
            } else {
                AttemptAgent::new(builder)
            };
            let (driver, readiness, mut verifier) = configured.into_parts();
            if let Some(readiness) = readiness
                && let Err(error) = readiness.await
            {
                let error = RecordedEvalError::now(EvalError::AttemptAgent(error));
                let verifier_cleanup = shutdown_attempt_verifier(&mut verifier).await;
                return Err(AgentExecutionFailure::setup(error, verifier_cleanup, None));
            }
            let readiness_timing = PhaseTiming::finished(readiness_started);
            let setup_started = Utc::now();
            match driver {
                AttemptDriver::Nanocodex(builder) => match builder.build() {
                    Ok((agent, events)) => Ok(AgentSetup {
                        agent: PreparedAgent::Nanocodex { agent, events },
                        verifier,
                        readiness_timing,
                        timing: PhaseTiming::finished(setup_started),
                    }),
                    Err(error) => {
                        let error = RecordedEvalError::now(EvalError::Nanocodex(error));
                        let verifier_cleanup = shutdown_attempt_verifier(&mut verifier).await;
                        Err(AgentExecutionFailure::setup(
                            error,
                            verifier_cleanup,
                            Some(readiness_timing),
                        ))
                    }
                },
                AttemptDriver::Codex(codex) => Ok(AgentSetup {
                    agent: PreparedAgent::Codex(codex),
                    verifier,
                    readiness_timing,
                    timing: PhaseTiming::finished(setup_started),
                }),
            }
        }
        .instrument(span.clone())
        .await;
        record_span_result(&span, trace_started, &result);
        result
    }
}

async fn shutdown_attempt_verifier(
    verifier: &mut Option<Box<dyn AttemptVerifier>>,
) -> CleanupPhase {
    let Some(mut verifier) = verifier.take() else {
        return CleanupPhase::not_required();
    };
    verifier.shutdown().await
}

struct AgentExecution {
    result: Option<AgentResult>,
    error: Option<RecordedEvalError>,
    verifier: Option<Box<dyn AttemptVerifier>>,
    readiness_timing: PhaseTiming,
    setup_timing: PhaseTiming,
    execution_timing: PhaseTiming,
    cleanup: CleanupPhase,
}

#[derive(Debug)]
struct AgentExecutionFailure {
    error: RecordedEvalError,
    result: Option<AgentResult>,
    cleanup: CleanupPhase,
    verifier_cleanup: CleanupPhase,
    readiness_timing: Option<PhaseTiming>,
    setup_timing: Option<PhaseTiming>,
    execution_timing: Option<PhaseTiming>,
}

impl AgentExecutionFailure {
    const fn setup(
        error: RecordedEvalError,
        verifier_cleanup: CleanupPhase,
        readiness_timing: Option<PhaseTiming>,
    ) -> Self {
        Self {
            error,
            result: None,
            cleanup: CleanupPhase::not_required(),
            verifier_cleanup,
            readiness_timing,
            setup_timing: None,
            execution_timing: None,
        }
    }
}

impl fmt::Display for AgentExecutionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.error.fmt(formatter)
    }
}

impl Error for AgentExecutionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error.error)
    }
}

#[derive(Debug)]
struct VerifierExecutionFailure {
    error: RecordedEvalError,
    cleanup: CleanupPhase,
    timing: Option<PhaseTiming>,
}

impl fmt::Display for VerifierExecutionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.error.fmt(formatter)
    }
}

impl Error for VerifierExecutionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error.error)
    }
}

struct AgentTurnOutcome {
    primary: Option<RecordedEvalError>,
    result: Option<AgentResult>,
    result_is_lower_bound: bool,
}

impl AgentTurnOutcome {
    const fn apply_lower_bound_duration(&mut self, duration_ns: u64) {
        if !self.result_is_lower_bound {
            return;
        }
        if let Some(result) = &mut self.result {
            result.metadata.duration_ns = duration_ns;
            result.metadata.duration_ms = duration_ns / 1_000_000;
        }
    }
}

enum AgentRunState {
    Finished(AgentTurnOutcome),
    TimedOut {
        primary: RecordedEvalError,
        observation: AgentObservation,
    },
}

struct TimedOutAgentRecovery<T, S> {
    terminal: Option<T>,
    shutdown: S,
    grace_elapsed: bool,
}

#[derive(Debug)]
struct RecordedEvalError {
    error: EvalError,
    occurred_at: DateTime<Utc>,
}

impl RecordedEvalError {
    fn now(error: EvalError) -> Self {
        Self {
            error,
            occurred_at: Utc::now(),
        }
    }
}

#[derive(Default)]
struct AgentObservation {
    billable_in_flight: u32,
    billing_unknown: bool,
    billing_uncertain_response_attempts: u32,
    final_message: String,
    run: Option<ObservedRun>,
    steers: u32,
    model_calls_started: u32,
    compactions_started: u32,
    tool_calls_started: u32,
    tool_work_duration_ns: u64,
    connection_attempts: u32,
    websocket_reconnects: u32,
    response_attempts: u32,
    response_retries: u32,
    connection_duration_ns: u64,
    retry_backoff_duration_ns: u64,
    pending_retry_delay_ns: Option<u64>,
    completed: CompletedBillableOperations,
}

struct ObservedRun {
    model: String,
    effort: String,
    transport: String,
    orchestration: String,
}

#[derive(Deserialize)]
struct AttemptFailureObservation {
    #[serde(default)]
    billing_uncertain: bool,
}

#[derive(Deserialize)]
struct AttemptRetryObservation {
    delay_ns: u64,
}

#[derive(Deserialize)]
struct ConnectionCompletedObservation {
    purpose: String,
    duration_ns: u64,
}

#[derive(Deserialize)]
struct ConnectionFailedObservation {
    duration_ns: u64,
}

struct AgentResultSelection {
    result: Option<AgentResult>,
    terminal_error: Option<EvalError>,
    used_lower_bound: bool,
}

#[derive(Default)]
struct CompletedBillableOperations {
    usage: UsageTotals,
    warmup_usage: UsageTotals,
    model_calls: u32,
    compactions: u32,
    tool_calls: u32,
    response_attempts: u32,
    response_retries: u32,
    model_duration_ns: u64,
    warmup_duration_ns: u64,
    completed_responses: u32,
}

struct AttemptRunFailure {
    error: EvalError,
    occurred_at: DateTime<Utc>,
    agent: Option<AgentResult>,
    verifier: Option<VerifierResult>,
    cleanup: EvalCleanup,
    environment_setup: Option<PhaseTiming>,
    environment_readiness: Option<PhaseTiming>,
    agent_setup: Option<PhaseTiming>,
    agent_execution: Option<PhaseTiming>,
    verifier_timing: Option<PhaseTiming>,
}

impl AttemptRunFailure {
    fn new(error: EvalError) -> Self {
        Self {
            error,
            occurred_at: Utc::now(),
            agent: None,
            verifier: None,
            cleanup: EvalCleanup::default(),
            environment_setup: None,
            environment_readiness: None,
            agent_setup: None,
            agent_execution: None,
            verifier_timing: None,
        }
    }

    fn from_agent(attempt: &NativeAttempt, failure: AgentExecutionFailure) -> Self {
        let RecordedEvalError { error, occurred_at } = failure.error;
        Self {
            error,
            occurred_at,
            agent: failure.result,
            verifier: None,
            cleanup: EvalCleanup {
                agent: failure.cleanup,
                verifier: failure.verifier_cleanup,
            },
            environment_setup: Some(attempt.setup_timing.clone()),
            environment_readiness: failure.readiness_timing,
            agent_setup: failure.setup_timing,
            agent_execution: failure.execution_timing,
            verifier_timing: None,
        }
    }

    fn after_agent(
        attempt: &NativeAttempt,
        agent: &AgentExecution,
        error: RecordedEvalError,
        verifier_cleanup: CleanupPhase,
    ) -> Self {
        Self {
            error: error.error,
            occurred_at: error.occurred_at,
            agent: agent.result.clone(),
            verifier: None,
            cleanup: EvalCleanup {
                agent: agent.cleanup.clone(),
                verifier: verifier_cleanup,
            },
            environment_setup: Some(attempt.setup_timing.clone()),
            environment_readiness: Some(agent.readiness_timing.clone()),
            agent_setup: Some(agent.setup_timing.clone()),
            agent_execution: Some(agent.execution_timing.clone()),
            verifier_timing: None,
        }
    }

    fn after_verifier_failure(
        attempt: &NativeAttempt,
        agent: &AgentExecution,
        primary: Option<RecordedEvalError>,
        failure: VerifierExecutionFailure,
    ) -> Self {
        let VerifierExecutionFailure {
            error: verifier_error,
            cleanup,
            timing,
        } = failure;
        if let Some(primary) = &primary {
            tracing::warn!(
                target: "nanocodex_eval",
                primary_error = %primary.error,
                verifier_error = %verifier_error.error,
                "verifier failed after an earlier agent exception"
            );
        }
        let error = primary.unwrap_or(verifier_error);
        Self {
            error: error.error,
            occurred_at: error.occurred_at,
            agent: agent.result.clone(),
            verifier: None,
            cleanup: EvalCleanup {
                agent: agent.cleanup.clone(),
                verifier: cleanup,
            },
            environment_setup: Some(attempt.setup_timing.clone()),
            environment_readiness: Some(agent.readiness_timing.clone()),
            agent_setup: Some(agent.setup_timing.clone()),
            agent_execution: Some(agent.execution_timing.clone()),
            verifier_timing: timing,
        }
    }

    fn after_verifier(
        attempt: &NativeAttempt,
        agent: &AgentExecution,
        verifier: &VerifierExecution,
        error: RecordedEvalError,
    ) -> Self {
        Self {
            error: error.error,
            occurred_at: error.occurred_at,
            agent: agent.result.clone(),
            verifier: Some(verifier.result.clone()),
            cleanup: EvalCleanup {
                agent: agent.cleanup.clone(),
                verifier: verifier.cleanup.clone(),
            },
            environment_setup: Some(attempt.setup_timing.clone()),
            environment_readiness: Some(agent.readiness_timing.clone()),
            agent_setup: Some(agent.setup_timing.clone()),
            agent_execution: Some(agent.execution_timing.clone()),
            verifier_timing: Some(verifier.timing.clone()),
        }
    }
}

async fn receive_agent_terminal(
    events: &mut AgentEvents,
    emitter: &mut AttemptEmitter<'_>,
    observation: &mut AgentObservation,
) -> Result<AgentEvent, EvalError> {
    loop {
        let event = events.recv().await.ok_or(EvalError::AgentEventsClosed)?;
        observation.observe(&event)?;
        let terminal = event.kind.is_terminal();
        emitter.emit(EvalEventKind::Agent(event.clone()));
        if terminal {
            return Ok(event);
        }
    }
}

async fn recover_timed_out_agent<S, R>(
    grace: Duration,
    shutdown: S,
    terminal: R,
) -> TimedOutAgentRecovery<R::Output, S::Output>
where
    S: Future,
    R: Future,
{
    tokio::pin!(shutdown);
    tokio::pin!(terminal);
    let deadline = tokio::time::sleep(grace);
    tokio::pin!(deadline);
    let mut shutdown_output = None;
    let mut terminal_output = None;
    let grace_elapsed = loop {
        if shutdown_output.is_some() && terminal_output.is_some() {
            break false;
        }
        tokio::select! {
            biased;
            output = &mut shutdown, if shutdown_output.is_none() => {
                shutdown_output = Some(output);
            }
            output = &mut terminal, if terminal_output.is_none() => {
                terminal_output = Some(output);
            }
            () = &mut deadline => break true,
        }
    };
    let shutdown = match shutdown_output {
        Some(output) => output,
        None => shutdown.await,
    };
    TimedOutAgentRecovery {
        terminal: terminal_output,
        shutdown,
        grace_elapsed,
    }
}

impl AgentObservation {
    const fn billing_completeness(&self) -> BillingCompleteness {
        if self.billable_in_flight == 0 && !self.billing_unknown {
            BillingCompleteness::Complete
        } else {
            BillingCompleteness::Unknown
        }
    }

    fn observe(&mut self, event: &AgentEvent) -> Result<(), EvalError> {
        self.observe_lifecycle(event.kind);
        match event.kind {
            AgentEventKind::RunStarted => {
                let run: RunStarted = event.decode_payload()?;
                self.run = Some(ObservedRun {
                    model: run.model,
                    effort: run.effort,
                    transport: run.transport,
                    orchestration: run.orchestration,
                });
            }
            AgentEventKind::AssistantMessage => {
                let message: nanocodex_agent::events::AssistantMessage = event.decode_payload()?;
                self.final_message = message.text;
            }
            AgentEventKind::RunSteered => {
                self.steers = self.steers.saturating_add(1);
            }
            AgentEventKind::ModelCallStarted => {
                self.model_calls_started = self.model_calls_started.saturating_add(1);
            }
            AgentEventKind::ModelCompactionStarted => {
                self.compactions_started = self.compactions_started.saturating_add(1);
            }
            AgentEventKind::ToolCall => {
                self.tool_calls_started = self.tool_calls_started.saturating_add(1);
            }
            AgentEventKind::ToolResult => {
                let result: ToolResultEvent = event.decode_payload()?;
                self.tool_work_duration_ns = self
                    .tool_work_duration_ns
                    .saturating_add(result.duration_ns);
            }
            AgentEventKind::ModelAttemptStarted => {
                self.response_attempts = self.response_attempts.saturating_add(1);
                if let Some(delay_ns) = self.pending_retry_delay_ns.take() {
                    self.retry_backoff_duration_ns =
                        self.retry_backoff_duration_ns.saturating_add(delay_ns);
                }
            }
            AgentEventKind::ModelAttemptFailed => {
                let failure: AttemptFailureObservation = event.decode_payload()?;
                if failure.billing_uncertain {
                    self.billing_unknown = true;
                    self.billing_uncertain_response_attempts =
                        self.billing_uncertain_response_attempts.saturating_add(1);
                }
            }
            AgentEventKind::ModelAttemptRetrying => {
                let retry: AttemptRetryObservation = event.decode_payload()?;
                self.response_retries = self.response_retries.saturating_add(1);
                self.pending_retry_delay_ns = Some(
                    self.pending_retry_delay_ns
                        .unwrap_or_default()
                        .saturating_add(retry.delay_ns),
                );
            }
            AgentEventKind::ModelConnectionStarted => {
                self.connection_attempts = self.connection_attempts.saturating_add(1);
            }
            AgentEventKind::ModelConnectionCompleted => {
                let connection: ConnectionCompletedObservation = event.decode_payload()?;
                self.connection_duration_ns = self
                    .connection_duration_ns
                    .saturating_add(connection.duration_ns);
                if connection.purpose != "initial" {
                    self.websocket_reconnects = self.websocket_reconnects.saturating_add(1);
                }
            }
            AgentEventKind::ModelConnectionFailed => {
                let connection: ConnectionFailedObservation = event.decode_payload()?;
                self.connection_duration_ns = self
                    .connection_duration_ns
                    .saturating_add(connection.duration_ns);
            }
            AgentEventKind::ModelWarmupCompleted => {
                let completed: ModelWarmupCompleted = event.decode_payload()?;
                self.completed.warmup_duration_ns = self
                    .completed
                    .warmup_duration_ns
                    .saturating_add(completed.duration_ns);
                if completed.source == "response" {
                    if completed.usage.is_none() {
                        self.billing_unknown = true;
                    }
                    self.completed
                        .observe(completed.usage.as_ref(), true, completed.attempt);
                }
            }
            AgentEventKind::ModelWarmupFailed => {
                let failed: ModelWarmupFailed = event.decode_payload()?;
                self.completed.warmup_duration_ns = self
                    .completed
                    .warmup_duration_ns
                    .saturating_add(failed.duration_ns);
            }
            AgentEventKind::ModelCallCompleted => {
                let completed: ModelCallCompleted = event.decode_payload()?;
                if completed.usage.is_none() {
                    self.billing_unknown = true;
                }
                self.completed.model_calls = self.completed.model_calls.saturating_add(1);
                self.completed.tool_calls = self
                    .completed
                    .tool_calls
                    .saturating_add(u32::try_from(completed.tool_calls).unwrap_or(u32::MAX));
                self.completed.model_duration_ns = self
                    .completed
                    .model_duration_ns
                    .saturating_add(completed.duration_ns);
                self.completed
                    .observe(completed.usage.as_ref(), false, Some(completed.attempt));
            }
            AgentEventKind::ModelCallFailed => {
                let failed: ModelCallFailed = event.decode_payload()?;
                self.completed.model_duration_ns = self
                    .completed
                    .model_duration_ns
                    .saturating_add(failed.duration_ns);
            }
            AgentEventKind::ModelCompactionCompleted => {
                let completed: CompactionCompleted = event.decode_payload()?;
                if completed.usage.is_none() {
                    self.billing_unknown = true;
                }
                self.completed.compactions = self.completed.compactions.saturating_add(1);
                self.completed.model_duration_ns = self
                    .completed
                    .model_duration_ns
                    .saturating_add(completed.duration_ns);
                self.completed
                    .observe(completed.usage.as_ref(), false, Some(completed.attempt));
            }
            AgentEventKind::ModelCompactionFailed => {
                let failed: CompactionFailed = event.decode_payload()?;
                self.completed.model_duration_ns = self
                    .completed
                    .model_duration_ns
                    .saturating_add(failed.duration_ns);
            }
            _ => {}
        }
        Ok(())
    }

    fn select_result(
        &self,
        terminal: Option<&AgentEvent>,
        billing_completeness: BillingCompleteness,
    ) -> AgentResultSelection {
        let Some(terminal) = terminal else {
            let result = self.lower_bound_result(None);
            return AgentResultSelection {
                used_lower_bound: result.is_some(),
                result,
                terminal_error: None,
            };
        };
        match AgentResult::from_terminal(self.final_message.clone(), terminal, billing_completeness)
        {
            Ok(result) => AgentResultSelection {
                result: Some(result),
                terminal_error: None,
                used_lower_bound: false,
            },
            Err(error) => {
                let result = self.lower_bound_result(Some(terminal.kind));
                AgentResultSelection {
                    used_lower_bound: result.is_some(),
                    result,
                    terminal_error: Some(error),
                }
            }
        }
    }

    fn lower_bound_result(&self, terminal_kind: Option<AgentEventKind>) -> Option<AgentResult> {
        if self.run.is_none()
            && self.completed.completed_responses == 0
            && self.model_calls_started == 0
            && self.compactions_started == 0
            && self.tool_calls_started == 0
            && self.connection_attempts == 0
            && self.response_attempts == 0
        {
            return None;
        }
        let run = self.run.as_ref();
        let model = run.map_or_else(|| MODEL.to_owned(), |run| run.model.clone());
        let effort = run.map_or_else(String::new, |run| run.effort.clone());
        let cost_usd = None;
        let model_calls = self.model_calls_started.max(self.completed.model_calls);
        let compactions = self.compactions_started.max(self.completed.compactions);
        let tool_calls = self.tool_calls_started.max(self.completed.tool_calls);
        let response_attempts = self.response_attempts.max(self.completed.response_attempts);
        let response_retries = self.response_retries.max(self.completed.response_retries);
        let metadata = AgentMetadata {
            status: match terminal_kind {
                Some(AgentEventKind::RunCompleted) => AgentStatus::Completed,
                Some(AgentEventKind::RunFailed) => AgentStatus::Failed,
                _ => AgentStatus::Cancelled,
            },
            model: model.clone(),
            effort: effort.clone(),
            reasoning_mode: None,
            transport: run.map_or_else(String::new, |run| run.transport.clone()),
            orchestration: run.map_or_else(String::new, |run| run.orchestration.clone()),
            runtime_completeness: crate::MeasurementCompleteness::ObservedLowerBound,
            duration_ms: 0,
            duration_ns: 0,
            model_calls,
            steers: self.steers,
            compactions,
            tool_calls,
            connection_attempts: self.connection_attempts,
            websocket_reconnects: self.websocket_reconnects,
            response_attempts,
            response_retries,
            billing_uncertain_response_attempts: self.billing_uncertain_response_attempts,
            connection_duration_ns: self.connection_duration_ns,
            retry_backoff_duration_ns: self.retry_backoff_duration_ns,
            model_duration_ns: self.completed.model_duration_ns,
            warmup_duration_ns: self.completed.warmup_duration_ns,
            tool_work_duration_ns: self.tool_work_duration_ns,
            tool_wall_duration_ns: 0,
            usage: self.completed.usage.clone(),
            warmup_usage: self.completed.warmup_usage.clone(),
            _last_response_id: None,
            cost_usd,
            cost_status: CostStatus::UsageNotReported.as_str().to_owned(),
            estimated_cost: None,
        };
        Some(AgentResult {
            final_message: self.final_message.clone(),
            model,
            effort,
            model_calls,
            tool_calls,
            usage: self.completed.usage.clone(),
            cost_usd,
            billing_completeness: BillingCompleteness::Unknown,
            metadata,
        })
    }

    const fn observe_lifecycle(&mut self, kind: AgentEventKind) {
        match kind {
            AgentEventKind::ModelWarmupStarted
            | AgentEventKind::ModelCallStarted
            | AgentEventKind::ModelCompactionStarted => {
                self.billable_in_flight = self.billable_in_flight.saturating_add(1);
            }
            AgentEventKind::ModelWarmupCompleted
            | AgentEventKind::ModelCallCompleted
            | AgentEventKind::ModelCompactionCompleted => {
                self.billable_in_flight = self.billable_in_flight.saturating_sub(1);
            }
            AgentEventKind::ModelWarmupFailed
            | AgentEventKind::ModelCallFailed
            | AgentEventKind::ModelCompactionFailed => {
                self.billable_in_flight = self.billable_in_flight.saturating_sub(1);
            }
            _ => {}
        }
    }
}

impl CompletedBillableOperations {
    fn observe(&mut self, usage: Option<&Usage>, warmup: bool, attempt: Option<u32>) {
        self.completed_responses = self.completed_responses.saturating_add(1);
        if let Some(attempt) = attempt {
            self.response_attempts = self.response_attempts.saturating_add(attempt);
            self.response_retries = self
                .response_retries
                .saturating_add(attempt.saturating_sub(1));
        }
        if let Some(usage) = usage {
            if warmup {
                self.warmup_usage.add(usage);
            } else {
                self.usage.add(usage);
            }
        }
    }
}

impl UsageTotals {
    fn add(&mut self, usage: &Usage) {
        self.input_tokens = self.input_tokens.saturating_add(usage.input_tokens);
        self.cached_input_tokens = self.cached_input_tokens.saturating_add(
            usage
                .input_tokens_details
                .as_ref()
                .map_or(0, |details| details.cached_tokens),
        );
        self.cache_write_input_tokens = self.cache_write_input_tokens.saturating_add(
            usage
                .input_tokens_details
                .as_ref()
                .map_or(0, |details| details.cache_write_tokens),
        );
        self.output_tokens = self.output_tokens.saturating_add(usage.output_tokens);
        self.reasoning_output_tokens = self.reasoning_output_tokens.saturating_add(
            usage
                .output_tokens_details
                .as_ref()
                .map_or(0, |details| details.reasoning_tokens),
        );
        self.total_tokens = self.total_tokens.saturating_add(usage.total_tokens);
    }
}

struct AgentSetup {
    agent: PreparedAgent,
    verifier: Option<Box<dyn AttemptVerifier>>,
    readiness_timing: PhaseTiming,
    timing: PhaseTiming,
}

enum PreparedAgent {
    Nanocodex {
        agent: Nanocodex,
        events: AgentEvents,
    },
    Codex(CodexExec),
}

impl EvaluatorBuilder {
    #[cfg(test)]
    const fn with_malformed_terminal_metrics(mut self) -> Self {
        self.malformed_terminal_metrics = true;
        self
    }

    /// Sets the parent under which this evaluator creates one UUID-named
    /// artifact directory.
    #[must_use]
    pub fn output_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.output_directory = directory.into();
        self
    }

    /// Reopens the newest incomplete job whose durable run manifest matches
    /// `sweep`, or creates a new job when none exists.
    #[must_use]
    pub fn resume_incomplete(mut self, sweep: &Sweep) -> Self {
        self.finite_run = Some(FiniteRun {
            manifest: sweep.manifest(),
            mode: FiniteRunMode::Resume,
        });
        self
    }

    /// Creates a new job already bound to `sweep`, even when a matching
    /// incomplete job exists.
    #[must_use]
    pub fn fresh_run(mut self, sweep: &Sweep) -> Self {
        self.finite_run = Some(FiniteRun {
            manifest: sweep.manifest(),
            mode: FiniteRunMode::Fresh,
        });
        self
    }

    /// Sets the maximum number of attempts allowed to execute concurrently.
    ///
    /// The default is one. [`Self::build`] rejects zero.
    #[must_use]
    pub const fn max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency;
        self
    }

    /// Bounds the sum of task-declared memory for concurrently running
    /// attempts. A task whose own declaration exceeds the ceiling runs alone.
    #[must_use]
    pub const fn max_memory_mb(mut self, max_memory_mb: u64) -> Self {
        self.max_memory_mb = Some(max_memory_mb);
        self
    }

    /// Records the execution environment used by the configured attempt
    /// backend in results and durable Harbor artifacts.
    #[must_use]
    pub const fn attempt_environment(mut self, environment: EvalEnvironment) -> Self {
        self.attempt_environment = environment;
        self
    }

    /// Configures the fresh Nanocodex builder for each attempt.
    ///
    /// The factory runs after the disposable workspace is populated and before
    /// the agent is built. This is the boundary for attempt-owned resources
    /// such as a retained VM tool session and its guest-visible workspace.
    #[must_use]
    pub fn attempt_agent<F, E>(mut self, factory: F) -> Self
    where
        F: for<'a> Fn(EvalAttempt<'a>, NanocodexBuilder) -> Result<AttemptAgent, E>
            + Send
            + Sync
            + 'static,
        E: Error + Send + Sync + 'static,
    {
        self.attempt_agent = Some(Arc::new(move |attempt, builder| {
            factory(attempt, builder).map_err(|error| Box::new(error) as AttemptError)
        }));
        self
    }

    /// Builds a reusable evaluator and a source of independent event streams.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid concurrency or an unavailable output path.
    pub fn build(self) -> Result<(Evaluator, EvalEvents), EvalError> {
        if self.max_concurrency == 0 {
            return Err(EvalError::InvalidConcurrency);
        }
        if self.max_memory_mb == Some(0) {
            return Err(EvalError::InvalidMemory);
        }
        if let Some(run) = &self.finite_run {
            let output = prospective_canonical_directory(&self.output_directory)?;
            for task in run.manifest.task_roots() {
                reject_output_overlap(&output, task)?;
            }
        }
        let planned_attempts = self
            .finite_run
            .as_ref()
            .map(|run| run.manifest.attempt_count());
        let job = match &self.finite_run {
            Some(run) => {
                let job = match run.mode {
                    FiniteRunMode::Fresh => EvalJob::create(&self.output_directory)?,
                    FiniteRunMode::Resume => {
                        EvalJob::resume_or_create(&self.output_directory, &run.manifest)?
                    }
                };
                job.bind_run(&run.manifest)?;
                job
            }
            None => EvalJob::create(&self.output_directory)?,
        };
        let (event_sender, _) = broadcast::channel(EVENT_CAPACITY);
        Ok((
            Evaluator {
                inner: Arc::new(EvaluatorInner {
                    nanocodex: self.nanocodex,
                    job,
                    planned_attempts,
                    admission: Arc::new(AdmissionController::new(
                        self.max_concurrency,
                        self.max_memory_mb,
                    )),
                    max_concurrency: self.max_concurrency,
                    max_memory_mb: self.max_memory_mb,
                    attempt_environment: self.attempt_environment,
                    next_prompt_cache_attempt: AtomicU64::new(0),
                    events: event_sender.clone(),
                    attempt_agent: self.attempt_agent,
                    #[cfg(test)]
                    malformed_terminal_metrics: self.malformed_terminal_metrics,
                }),
            },
            EvalEvents::new(event_sender),
        ))
    }
}

impl AdmissionController {
    fn new(max_concurrency: usize, max_memory_mb: Option<u64>) -> Self {
        Self {
            max_concurrency,
            max_memory_mb,
            state: Mutex::new(AdmissionState::default()),
            changed: Notify::new(),
        }
    }

    async fn acquire(self: &Arc<Self>, requested_memory_mb: u64) -> Option<AdmissionPermit> {
        let memory_mb = self
            .max_memory_mb
            .map_or(0, |limit| requested_memory_mb.min(limit));
        loop {
            let changed = self.changed.notified();
            {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                if state.draining {
                    return None;
                }
                let concurrency_available = state.running < self.max_concurrency;
                let memory_available = self.max_memory_mb.is_none_or(|limit| {
                    state
                        .memory_mb
                        .checked_add(memory_mb)
                        .is_some_and(|total| total <= limit)
                });
                if concurrency_available && memory_available {
                    state.running += 1;
                    state.memory_mb += memory_mb;
                    state.admitted = state.admitted.saturating_add(1);
                    return Some(AdmissionPermit {
                        controller: Arc::clone(self),
                        memory_mb,
                    });
                }
            }
            changed.await;
        }
    }

    fn begin_drain(&self) -> usize {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.draining = true;
        let admitted = state.admitted;
        drop(state);
        self.changed.notify_waiters();
        admitted
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let mut state = self
            .controller
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.running = state.running.saturating_sub(1);
        state.memory_mb = state.memory_mb.saturating_sub(self.memory_mb);
        drop(state);
        self.controller.changed.notify_waiters();
    }
}

impl AttemptAgent {
    /// Uses `nanocodex` for one attempt with the default native verifier.
    #[must_use]
    pub fn new(nanocodex: NanocodexBuilder) -> Self {
        Self {
            driver: AttemptDriver::Nanocodex(nanocodex),
            readiness: None,
            verifier: None,
        }
    }

    /// Uses one pinned stock-Codex CLI process for an evaluator attempt.
    ///
    /// This concrete adapter preserves the evaluator's workspace, timeout,
    /// verifier, cleanup, and retention lifecycle.
    #[doc(hidden)]
    #[must_use]
    pub fn codex(codex: CodexExec) -> Self {
        Self {
            driver: AttemptDriver::Codex(codex),
            readiness: None,
            verifier: None,
        }
    }

    /// Installs asynchronous environment readiness work that must complete
    /// before the agent is built or any model request is sent.
    ///
    /// VM adapters use this to wait for a typed guest handshake. A readiness
    /// failure aborts the attempt as an environment error without spending a
    /// model request.
    #[must_use]
    pub fn ready<F, E>(mut self, readiness: F) -> Self
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        self.readiness = Some(Box::pin(async move {
            readiness
                .await
                .map_err(|error| Box::new(error) as AttemptError)
        }));
        self
    }

    /// Installs the verifier that owns this attempt's environment backend.
    #[must_use]
    pub fn verifier(mut self, verifier: impl AttemptVerifier + 'static) -> Self {
        self.verifier = Some(Box::new(verifier));
        self
    }

    fn into_parts(
        self,
    ) -> (
        AttemptDriver,
        Option<AttemptReadinessFuture>,
        Option<Box<dyn AttemptVerifier>>,
    ) {
        (self.driver, self.readiness, self.verifier)
    }
}

impl EvalAttempt<'_> {
    /// Returns the immutable task definition.
    #[must_use]
    pub const fn task(&self) -> &Task {
        self.task
    }

    /// Returns the retained attempt root.
    #[must_use]
    pub const fn directory(&self) -> &Path {
        self.directory
    }

    /// Returns the workspace path presented to the agent.
    #[must_use]
    pub const fn workspace(&self) -> &Path {
        self.workspace
    }
}

struct AttemptEmitter<'a> {
    eval: &'a Evaluator,
    attempt_id: Uuid,
    session_id: SessionId,
    prompt_cache_cohort: u64,
    task_name: String,
    trial_name: String,
    sequence: u64,
}

impl<'a> AttemptEmitter<'a> {
    fn new(
        eval: &'a Evaluator,
        session_id: SessionId,
        prompt_cache_cohort: u64,
        task: &Task,
        trial_name: &str,
    ) -> Self {
        Self {
            eval,
            attempt_id: session_id.as_uuid(),
            session_id,
            prompt_cache_cohort,
            task_name: task.name().to_owned(),
            trial_name: trial_name.to_owned(),
            sequence: 0,
        }
    }

    fn emit(&mut self, kind: EvalEventKind) {
        self.sequence += 1;
        let _ = self.eval.inner.events.send(Arc::new(EvalEvent {
            run_id: self.eval.inner.job.id(),
            attempt_id: self.attempt_id,
            task_name: self.task_name.clone(),
            trial_name: self.trial_name.clone(),
            sequence: self.sequence,
            kind,
        }));
    }
}

#[derive(Deserialize)]
struct ResponsesApiErrorEnvelope {
    error: Option<ResponsesApiError>,
    response: Option<ResponsesApiErrorResponse>,
}

#[derive(Deserialize)]
struct ResponsesApiErrorResponse {
    error: Option<ResponsesApiError>,
}

#[derive(Deserialize)]
struct ResponsesApiError {
    code: Option<String>,
}

fn attempt_failure(
    eval: &Evaluator,
    attempt_id: Uuid,
    task: Task,
    trial_name: String,
    started_at: DateTime<Utc>,
    queue_wait: PhaseTiming,
    failure: &AttemptRunFailure,
) -> EvalFailure {
    let root = eval.directory().join(&trial_name);
    let model = failure
        .agent
        .as_ref()
        .map_or_else(|| MODEL.to_owned(), |agent| agent.model.clone());
    let effort = failure
        .agent
        .as_ref()
        .map_or_else(|| "unknown".to_owned(), |agent| agent.effort.clone());
    let exception = eval_exception(&failure.error, failure.occurred_at);
    EvalFailure {
        attempt_id,
        task_name: task.name().to_owned(),
        trial_name,
        exception,
        model,
        effort,
        environment: eval.attempt_environment(),
        started_at,
        finished_at: Utc::now(),
        timing: EvalFailureTiming {
            queue_wait,
            environment_setup: failure.environment_setup.clone(),
            environment_readiness: failure.environment_readiness.clone(),
            agent_setup: failure.agent_setup.clone(),
            agent_execution: failure.agent_execution.clone(),
            verifier: failure.verifier_timing.clone(),
        },
        agent: failure.agent.clone(),
        verifier: failure.verifier.clone(),
        cleanup: failure.cleanup.clone(),
        artifacts: EvalArtifacts {
            workspace: root.join("workspace"),
            verifier_output: root.join("verifier/test-stdout.txt"),
            directory: root,
        },
        task,
    }
}

fn eval_exception(error: &EvalError, occurred_at: DateTime<Utc>) -> EvalException {
    EvalException {
        kind: failure_kind(error),
        outcome: failure_outcome(error),
        message: error.to_string(),
        traceback: error_traceback(error),
        occurred_at,
    }
}

fn failure_outcome(error: &EvalError) -> EvalOutcome {
    match error {
        EvalError::Nanocodex(error) if is_safety_refusal(error) => EvalOutcome::SafetyRefusal,
        EvalError::AgentTimeout(_) => EvalOutcome::AgentTimeout,
        _ => EvalOutcome::InfrastructureError,
    }
}

fn failure_kind(error: &EvalError) -> EvalExceptionKind {
    match error {
        EvalError::Nanocodex(error) if is_safety_refusal(error) => {
            EvalExceptionKind::AgentSafetyRefusal
        }
        EvalError::Nanocodex(error)
            if error
                .responses_error()
                .is_some_and(|error| error.class() == "authorization") =>
        {
            EvalExceptionKind::AgentAuthentication
        }
        EvalError::AgentTimeout(_) => EvalExceptionKind::AgentTimeout,
        EvalError::VerifierTimeout(_) => EvalExceptionKind::VerifierTimeout,
        EvalError::AgentCleanup(_) => EvalExceptionKind::Cleanup,
        EvalError::Nanocodex(_)
        | EvalError::Codex(_)
        | EvalError::AgentEventsClosed
        | EvalError::AgentTerminal(_) => EvalExceptionKind::Agent,
        EvalError::AttemptVerifier(_) | EvalError::ParseReward(_) => EvalExceptionKind::Verifier,
        EvalError::UnsupportedNativeTask { .. }
        | EvalError::TaskPackage(_)
        | EvalError::OutputOverlapsTask { .. }
        | EvalError::AttemptAgent(_) => EvalExceptionKind::Environment,
        EvalError::InvalidConcurrency
        | EvalError::InvalidMemory
        | EvalError::Draining
        | EvalError::InvalidDurableTrial(_)
        | EvalError::Io(_)
        | EvalError::Json(_)
        | EvalError::RunConflict(_)
        | EvalError::RunDigestSchemaIncompatible { .. }
        | EvalError::RunActive(_)
        | EvalError::MissingSweepCoordinate => EvalExceptionKind::Internal,
    }
}

const fn verifier_workspace_usable_after_agent_error(error: &EvalError) -> bool {
    matches!(
        error,
        EvalError::Nanocodex(_)
            | EvalError::Codex(_)
            | EvalError::AgentTimeout(_)
            | EvalError::AgentEventsClosed
            | EvalError::AgentTerminal(_)
    )
}

fn reject_output_overlap(output: &Path, task: &Path) -> Result<(), EvalError> {
    if output.starts_with(task) || output_aliases_task_package(output, task)? {
        return Err(EvalError::OutputOverlapsTask {
            output: output.to_path_buf(),
            task: task.to_path_buf(),
        });
    }
    Ok(())
}

#[cfg(unix)]
fn output_aliases_task_package(output: &Path, task: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt as _;

    const PACKAGE_DIRECTORIES: [&str; 4] = ["environment", "tests", "solution", "steps"];

    // The output ancestry is application-owned and non-adversarial. Comparing
    // existing directory identities also catches accidental bind-mount aliases;
    // it is not intended to defend against a concurrent hostile path swap.
    let mut identities = std::collections::HashSet::<(u64, u64)>::new();
    let mut pending = vec![task.to_path_buf()];
    pending.extend(PACKAGE_DIRECTORIES.into_iter().map(|name| task.join(name)));
    while let Some(directory) = pending.pop() {
        let metadata = match fs::symlink_metadata(&directory) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "task package symlinks are unsupported while checking output ancestry: {}",
                    directory.display()
                ),
            ));
        }
        if !metadata.is_dir() || !identities.insert((metadata.dev(), metadata.ino())) {
            continue;
        }
        if directory != task {
            for entry in fs::read_dir(&directory)? {
                let entry = entry?;
                let metadata = fs::symlink_metadata(entry.path())?;
                if metadata.file_type().is_symlink() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "task package symlinks are unsupported while checking output ancestry: {}",
                            entry.path().display()
                        ),
                    ));
                }
                if metadata.is_dir() {
                    pending.push(entry.path());
                }
            }
        }
    }

    for ancestor in output.ancestors() {
        match fs::metadata(ancestor) {
            Ok(metadata)
                if metadata.is_dir() && identities.contains(&(metadata.dev(), metadata.ino())) =>
            {
                return Ok(true);
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

#[cfg(not(unix))]
fn output_aliases_task_package(_output: &Path, _task: &Path) -> io::Result<bool> {
    Ok(false)
}

fn prospective_canonical_directory(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut missing = Vec::<OsString>::new();
    let mut ancestor = absolute.as_path();
    loop {
        match fs::metadata(ancestor) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    format!("path component is not a directory: {}", ancestor.display()),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(
                    ancestor
                        .file_name()
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::NotFound,
                                format!(
                                    "directory has no existing ancestor: {}",
                                    absolute.display()
                                ),
                            )
                        })?
                        .to_os_string(),
                );
                ancestor = ancestor.parent().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("directory has no existing ancestor: {}", absolute.display()),
                    )
                })?;
            }
            Err(error) => return Err(error),
        }
    }
    let mut canonical = fs::canonicalize(ancestor)?;
    canonical.extend(missing.into_iter().rev());
    Ok(canonical)
}

fn is_safety_refusal(error: &NanocodexError) -> bool {
    let Some(ResponsesError::Api { event }) = error.responses_error() else {
        return false;
    };
    serde_json::from_str::<ResponsesApiErrorEnvelope>(event)
        .ok()
        .and_then(|event| {
            event
                .error
                .or_else(|| event.response.and_then(|response| response.error))
                .and_then(|error| error.code)
        })
        .is_some_and(|code| code == "cyber_policy")
}

fn error_traceback(error: &dyn Error) -> String {
    let mut traceback = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        traceback.push_str("\nCaused by: ");
        traceback.push_str(&error.to_string());
        source = error.source();
    }
    traceback
}

fn attempt_span(
    eval: &Evaluator,
    task: &Task,
    attempt_id: Uuid,
    trial_name: &str,
    prompt_cache_cohort: u64,
    coordinate: Option<&SweepCoordinate>,
) -> Span {
    let span = info_span!(
        target: "nanocodex_eval",
        parent: None,
        "eval.attempt",
        otel.kind = "internal",
        otel.status_code = tracing::field::Empty,
        eval.id = %eval.id(),
        eval.attempt.id = %attempt_id,
        eval.task.name = task.name(),
        eval.trial.name = trial_name,
        eval.agent.id = tracing::field::Empty,
        eval.trial.number = tracing::field::Empty,
        eval.task.image = task.image().reference(),
        eval.resource.cpus = task.resources().cpus,
        eval.resource.memory_mib = task.resources().memory_mb,
        eval.resource.storage_mib = task.resources().storage_mb,
        eval.resource.gpus = task.resources().gpus,
        eval.network = task.network().as_str(),
        eval.score.status = tracing::field::Empty,
        eval.reward.total = tracing::field::Empty,
        agent.model_calls = tracing::field::Empty,
        agent.tool_calls = tracing::field::Empty,
        agent.response_attempts = tracing::field::Empty,
        agent.response_retries = tracing::field::Empty,
        agent.runtime.completeness = tracing::field::Empty,
        agent.usage.completeness = tracing::field::Empty,
        agent.usage.missing = tracing::field::Empty,
        agent.billing.completeness = tracing::field::Empty,
        agent.prompt_cache.cohort = prompt_cache_cohort,
        gen_ai.usage.input_tokens = tracing::field::Empty,
        gen_ai.usage.cached_input_tokens = tracing::field::Empty,
        gen_ai.usage.cache_write_input_tokens = tracing::field::Empty,
        gen_ai.usage.output_tokens = tracing::field::Empty,
        gen_ai.usage.total_tokens = tracing::field::Empty,
        agent.warmup.duration_ns = tracing::field::Empty,
        agent.warmup.input_tokens = tracing::field::Empty,
        agent.warmup.cached_input_tokens = tracing::field::Empty,
        agent.warmup.cache_write_input_tokens = tracing::field::Empty,
        agent.warmup.output_tokens = tracing::field::Empty,
        agent.warmup.total_tokens = tracing::field::Empty,
        cost.usd = tracing::field::Empty,
        cost.status = tracing::field::Empty,
        eval.cleanup.failed = tracing::field::Empty,
        agent.cleanup.status = tracing::field::Empty,
        agent.cleanup.duration_ns = tracing::field::Empty,
        verifier.cleanup.status = tracing::field::Empty,
        verifier.cleanup.duration_ns = tracing::field::Empty,
        status = tracing::field::Empty,
        error.message = tracing::field::Empty,
        duration_ns = tracing::field::Empty,
    );
    if let Some(coordinate) = coordinate {
        span.record("eval.agent.id", coordinate.agent.as_str());
        span.record("eval.trial.number", coordinate.trial);
    }
    span
}

fn record_attempt_result(
    span: &Span,
    started_at: Instant,
    result: &Result<EvalResult, AttemptRunFailure>,
) {
    let duration_ns = elapsed_ns(started_at);
    span.record("duration_ns", duration_ns);
    match result {
        Ok(result) => {
            record_attempt_success(span, result);
            span.in_scope(|| {
                info!(
                    target: "nanocodex_eval",
                    duration_ns,
                    score.status = eval_status(result.status),
                    "evaluation attempt completed"
                );
            });
        }
        Err(failure) => {
            record_cleanup(span, &failure.cleanup);
            if let Some(agent) = &failure.agent {
                record_agent_metrics(span, agent);
            }
            span.record("status", "failed");
            span.record("otel.status_code", "ERROR");
            span.record("error.message", tracing::field::display(&failure.error));
            span.in_scope(|| {
                info!(
                    target: "nanocodex_eval",
                    duration_ns,
                    error = %failure.error,
                    "evaluation attempt failed"
                );
            });
        }
    }
}

fn record_attempt_success(span: &Span, result: &EvalResult) {
    record_cleanup(span, &result.cleanup);
    if let Some(agent) = &result.agent {
        record_agent_metrics(span, agent);
    }
    span.record("status", "completed");
    span.record("eval.score.status", eval_status(result.status));
    span.record(
        "eval.reward.total",
        result.verifier.rewards.values().sum::<f64>(),
    );
    if let Some(exception) = &result.exception {
        span.record("otel.status_code", "ERROR");
        span.record("error.message", tracing::field::display(&exception.message));
    } else {
        span.record("otel.status_code", "OK");
    }
}

fn record_agent_metrics(span: &Span, agent: &AgentResult) {
    let usage = &agent.usage;
    let warmup = &agent.metadata.warmup_usage;
    let usage_observed = agent.has_observed_usage();
    span.record("agent.model_calls", agent.model_calls);
    span.record("agent.tool_calls", agent.tool_calls);
    span.record("agent.response_attempts", agent.metadata.response_attempts);
    span.record("agent.response_retries", agent.metadata.response_retries);
    span.record(
        "agent.runtime.completeness",
        measurement_completeness_label(agent.metadata.runtime_completeness),
    );
    span.record(
        "agent.usage.completeness",
        if !usage_observed {
            "missing"
        } else if agent.billing_completeness == BillingCompleteness::Complete {
            "complete"
        } else {
            "observed_lower_bound"
        },
    );
    span.record("agent.usage.missing", !usage_observed);
    span.record(
        "agent.billing.completeness",
        billing_completeness_label(agent.billing_completeness),
    );
    span.record("cost.status", agent.metadata.cost_status.as_str());
    if usage_observed {
        span.record("gen_ai.usage.input_tokens", usage.input_tokens);
        span.record(
            "gen_ai.usage.cached_input_tokens",
            usage.cached_input_tokens,
        );
        span.record(
            "gen_ai.usage.cache_write_input_tokens",
            usage.cache_write_input_tokens,
        );
        span.record("gen_ai.usage.output_tokens", usage.output_tokens);
        span.record("gen_ai.usage.total_tokens", usage.total_tokens);
        span.record("agent.warmup.input_tokens", warmup.input_tokens);
        span.record(
            "agent.warmup.cached_input_tokens",
            warmup.cached_input_tokens,
        );
        span.record(
            "agent.warmup.cache_write_input_tokens",
            warmup.cache_write_input_tokens,
        );
        span.record("agent.warmup.output_tokens", warmup.output_tokens);
        span.record("agent.warmup.total_tokens", warmup.total_tokens);
    }
    span.record(
        "agent.warmup.duration_ns",
        agent.metadata.warmup_duration_ns,
    );
    if let Some(cost_usd) = agent.cost_usd {
        span.record("cost.usd", cost_usd);
    }
}

const fn measurement_completeness_label(
    completeness: crate::MeasurementCompleteness,
) -> &'static str {
    match completeness {
        crate::MeasurementCompleteness::Complete => "complete",
        crate::MeasurementCompleteness::ObservedLowerBound => "observed_lower_bound",
    }
}

const fn billing_completeness_label(completeness: BillingCompleteness) -> &'static str {
    match completeness {
        BillingCompleteness::Complete => "complete",
        BillingCompleteness::Unknown => "unknown",
    }
}

fn record_cleanup(span: &Span, cleanup: &EvalCleanup) {
    span.record("eval.cleanup.failed", cleanup.is_failed());
    span.record("agent.cleanup.status", cleanup_status(&cleanup.agent));
    span.record(
        "agent.cleanup.duration_ns",
        cleanup.agent.timing.as_ref().map_or(0, phase_timing_ns),
    );
    span.record("verifier.cleanup.status", cleanup_status(&cleanup.verifier));
    span.record(
        "verifier.cleanup.duration_ns",
        cleanup.verifier.timing.as_ref().map_or(0, phase_timing_ns),
    );
}

const fn cleanup_status(cleanup: &CleanupPhase) -> &'static str {
    match cleanup.status {
        crate::CleanupStatus::NotRequired => "not_required",
        crate::CleanupStatus::Completed => "completed",
        crate::CleanupStatus::Failed => "failed",
    }
}

fn phase_timing_ns(timing: &PhaseTiming) -> u64 {
    u64::try_from(
        timing
            .finished_at
            .signed_duration_since(timing.started_at)
            .num_nanoseconds()
            .unwrap_or_default()
            .max(0),
    )
    .unwrap_or(u64::MAX)
}

const fn eval_status(status: EvalStatus) -> &'static str {
    match status {
        EvalStatus::Passed => "passed",
        EvalStatus::Failed => "failed",
    }
}

fn validate_attempt_environment(task: &Task, custom_backend: bool) -> Result<(), EvalError> {
    if task.requires_compose() && !custom_backend {
        return Err(EvalError::UnsupportedNativeTask {
            task: task.name().to_owned(),
            reason: "custom Docker Compose environments are not available in native mode",
        });
    }
    Ok(())
}

fn verifier_status(verifier: &crate::VerifierResult) -> EvalStatus {
    if verifier.rewards.values().all(|reward| *reward > 0.0) {
        EvalStatus::Passed
    } else {
        EvalStatus::Failed
    }
}

fn record_span_result<T, E>(span: &tracing::Span, started_at: Instant, result: &Result<T, E>)
where
    E: std::fmt::Display,
{
    let duration_ns = elapsed_ns(started_at);
    span.record("duration_ns", duration_ns);
    match result {
        Ok(_) => {
            span.record("status", "completed");
            span.record("otel.status_code", "OK");
            span.in_scope(|| {
                info!(
                    target: "nanocodex_eval",
                    duration_ns,
                    status = "completed",
                    "evaluation phase completed"
                );
            });
        }
        Err(error) => {
            span.record("status", "failed");
            span.record("otel.status_code", "ERROR");
            span.record("error.message", tracing::field::display(error));
            span.in_scope(|| {
                info!(
                    target: "nanocodex_eval",
                    duration_ns,
                    status = "failed",
                    error = %error,
                    "evaluation phase failed"
                );
            });
        }
    }
}

fn record_content(span: &tracing::Span, kind: &'static str, content: &str) {
    span.in_scope(|| {
        info!(
            target: "nanocodex_eval",
            content_kind = kind,
            content,
            "evaluation content"
        );
    });
}

fn elapsed_ns(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn trial_name(task: &Task, attempt_id: Uuid, coordinate: Option<&SweepCoordinate>) -> String {
    let short_name = task.name().rsplit('/').next().unwrap_or(task.name());
    let compact_id = attempt_id.simple().to_string();
    match coordinate {
        Some(coordinate) => format!(
            "{short_name}__{}__{:03}__{}",
            coordinate.agent, coordinate.trial, compact_id
        ),
        None => format!("{short_name}__{compact_id}"),
    }
}

impl AgentResult {
    fn from_terminal(
        final_message: String,
        event: &AgentEvent,
        billing_completeness: BillingCompleteness,
    ) -> Result<Self, EvalError> {
        if !event.kind.is_terminal() {
            return Err(EvalError::AgentEventsClosed);
        }
        let metadata: AgentMetadata =
            serde_json::from_str(event.payload.get()).map_err(EvalError::AgentTerminal)?;
        let billing_completeness = if metadata.billing_uncertain_response_attempts > 0
            || metadata.cost_status == ESTIMATED_LOWER_BOUND_COST_STATUS
        {
            BillingCompleteness::Unknown
        } else {
            billing_completeness
        };
        Ok(Self {
            final_message,
            model: metadata.model.clone(),
            effort: metadata.effort.clone(),
            model_calls: metadata.model_calls,
            tool_calls: metadata.tool_calls,
            usage: metadata.usage.clone(),
            cost_usd: metadata.cost_usd,
            billing_completeness,
            metadata,
        })
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use std::{
        collections::BTreeMap,
        convert::Infallible,
        fs,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use futures_util::{SinkExt, StreamExt};
    use nanocodex_agent::{Nanocodex, OpenAi, Tools};
    use nanocodex_oai_api::{
        pricing::CostStatus,
        responses::{InputTokenDetails, OutputTokenDetails, Usage},
    };
    use nanocodex_tools::{ToolContext, ToolDefinition, ToolOutput, runtime::DynamicToolProvider};
    use serde_json::{Value, json};
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    use super::{
        AgentEvent, AgentEventKind, AgentObservation, AttemptAgent, AttemptVerification,
        AttemptVerifier, AttemptVerifierCleanupFuture, AttemptVerifierFuture, EvalAttempt,
        Evaluator,
    };
    use crate::{
        AgentStatus, BillingCompleteness, CleanupPhase, CleanupStatus, EvalAttemptOutcome,
        EvalEventKind, EvalExceptionKind, EvalOutcome, EvalStatus, Task, VerifierResult,
        harbor::Harbor,
    };

    struct AttemptResourceProvider {
        live_resources: Arc<AtomicUsize>,
    }

    struct PackageMutatingProvider {
        mutation: PathBuf,
    }

    impl Drop for AttemptResourceProvider {
        fn drop(&mut self) {
            self.live_resources.fetch_sub(1, Ordering::AcqRel);
        }
    }

    impl Drop for PackageMutatingProvider {
        fn drop(&mut self) {
            fs::write(&self.mutation, "changed after agent execution\n").unwrap();
        }
    }

    #[async_trait]
    impl DynamicToolProvider for AttemptResourceProvider {
        fn start(&self) {}

        fn direct_tools(&self) -> Vec<Arc<dyn nanocodex_tools::Tool>> {
            Vec::new()
        }

        fn available_definitions(&self) -> Vec<ToolDefinition> {
            Vec::new()
        }

        async fn execute(
            &self,
            _name: &str,
            _input: Value,
            _context: ToolContext<'_>,
        ) -> Option<ToolOutput> {
            None
        }
    }

    #[async_trait]
    impl DynamicToolProvider for PackageMutatingProvider {
        fn start(&self) {}

        fn direct_tools(&self) -> Vec<Arc<dyn nanocodex_tools::Tool>> {
            Vec::new()
        }

        fn available_definitions(&self) -> Vec<ToolDefinition> {
            Vec::new()
        }

        async fn execute(
            &self,
            _name: &str,
            _input: Value,
            _context: ToolContext<'_>,
        ) -> Option<ToolOutput> {
            None
        }
    }

    struct ResourceProbeVerifier {
        live_resources: Arc<AtomicUsize>,
    }

    struct ResourceCheckedVerifier<V> {
        inner: V,
        live_resources: Arc<AtomicUsize>,
    }

    struct ShutdownProbeVerifier {
        shutdowns: Arc<AtomicUsize>,
    }

    struct FailingCleanupVerifier;

    struct StaticVerifier {
        reward: f64,
    }

    struct TimeoutRun {
        outcome: EvalAttemptOutcome,
        trial: Value,
        trajectory: Value,
        job: Value,
        aggregate: crate::AggregateDataset,
        terminal_events: usize,
        live_resources: usize,
    }

    impl AttemptVerifier for ShutdownProbeVerifier {
        fn verify<'a>(
            &'a mut self,
            _task: &'a Task,
            _attempt: EvalAttempt<'a>,
        ) -> AttemptVerifierFuture<'a> {
            Box::pin(async {
                panic!("shutdown probe verifier must not execute after an earlier failure")
            })
        }

        fn shutdown(&mut self) -> AttemptVerifierCleanupFuture<'_> {
            Box::pin(async move {
                self.shutdowns.fetch_add(1, Ordering::AcqRel);
                CleanupPhase::completed(chrono::Utc::now())
            })
        }
    }

    impl AttemptVerifier for FailingCleanupVerifier {
        fn verify<'a>(
            &'a mut self,
            _task: &'a Task,
            attempt: EvalAttempt<'a>,
        ) -> AttemptVerifierFuture<'a> {
            Box::pin(async move {
                let cleanup_error = std::io::Error::other("deterministic verifier cleanup failure");
                if let Err(error) =
                    fs::write(attempt.directory().join("verifier/test-stdout.txt"), [])
                {
                    return Err(super::AttemptVerificationFailure::new(
                        error,
                        CleanupPhase::not_required(),
                    ));
                }
                let primary = std::io::Error::other("deterministic verifier primary failure");
                let occurred_at = chrono::Utc::now();
                let cleanup_started = chrono::Utc::now();
                Err(super::AttemptVerificationFailure::observed_at(
                    primary,
                    occurred_at,
                    CleanupPhase::failed(cleanup_started, &cleanup_error),
                ))
            })
        }
    }

    impl AttemptVerifier for StaticVerifier {
        fn verify<'a>(
            &'a mut self,
            _task: &'a Task,
            attempt: EvalAttempt<'a>,
        ) -> AttemptVerifierFuture<'a> {
            let reward = self.reward;
            Box::pin(async move {
                fs::write(attempt.directory().join("verifier/test-stdout.txt"), []).map_err(
                    |error| {
                        super::AttemptVerificationFailure::new(error, CleanupPhase::not_required())
                    },
                )?;
                Ok(AttemptVerification {
                    result: VerifierResult {
                        exit_code: i32::from(reward <= 0.0),
                        rewards: BTreeMap::from([("reward".to_owned(), reward)]),
                    },
                    stdout: String::new(),
                    stderr: String::new(),
                    cleanup: CleanupPhase::not_required(),
                })
            })
        }
    }

    impl AttemptVerifier for ResourceProbeVerifier {
        fn verify<'a>(
            &'a mut self,
            _task: &'a Task,
            _attempt: EvalAttempt<'a>,
        ) -> AttemptVerifierFuture<'a> {
            assert_eq!(
                self.live_resources.load(Ordering::Acquire),
                0,
                "attempt-owned agent resources must be joined before verification starts"
            );
            Box::pin(async {
                let cleanup_started = chrono::Utc::now();
                let cleanup_error = std::io::Error::other("deterministic verifier cleanup failure");
                Ok(AttemptVerification {
                    result: VerifierResult {
                        exit_code: 0,
                        rewards: BTreeMap::from([("reward".to_owned(), 1.0)]),
                    },
                    stdout: String::new(),
                    stderr: String::new(),
                    cleanup: CleanupPhase::failed(cleanup_started, &cleanup_error),
                })
            })
        }
    }

    impl<V> AttemptVerifier for ResourceCheckedVerifier<V>
    where
        V: AttemptVerifier,
    {
        fn verify<'a>(
            &'a mut self,
            task: &'a Task,
            attempt: EvalAttempt<'a>,
        ) -> AttemptVerifierFuture<'a> {
            assert_eq!(
                self.live_resources.load(Ordering::Acquire),
                0,
                "timed-out agent resources must be joined before verification starts"
            );
            self.inner.verify(task, attempt)
        }

        fn shutdown(&mut self) -> AttemptVerifierCleanupFuture<'_> {
            self.inner.shutdown()
        }
    }

    #[test]
    fn verifier_failure_default_timestamp_does_not_follow_completed_cleanup() {
        let cleanup_started = chrono::Utc::now();
        let failure = super::AttemptVerificationFailure::new(
            std::io::Error::other("deterministic verifier failure"),
            CleanupPhase::completed(cleanup_started),
        );
        let (_, occurred_at, cleanup) = failure.into_parts();

        assert_eq!(occurred_at, cleanup_started);
        assert!(cleanup.timing.is_some_and(|timing| {
            occurred_at <= timing.started_at && timing.started_at <= timing.finished_at
        }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn agent_resources_are_joined_before_attempt_verifier() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let warmup = socket.next().await.unwrap().unwrap();
            assert!(warmup.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": { "id": "resp-warmup", "usage": null }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let generation = socket.next().await.unwrap().unwrap();
            assert!(generation.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-generation",
                            "status": "completed",
                            "output": [{
                                "type": "message",
                                "role": "assistant",
                                "content": [{ "type": "output_text", "text": "done" }]
                            }],
                            "usage": {
                                "input_tokens": 1,
                                "input_tokens_details": { "cached_tokens": 0 },
                                "output_tokens": 1,
                                "output_tokens_details": { "reasoning_tokens": 0 },
                                "total_tokens": 2
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            while socket.next().await.is_some() {}
        });
        let live_resources = Arc::new(AtomicUsize::new(0));
        let openai = OpenAi::builder("test")
            .websocket_url(endpoint)
            .build()
            .unwrap();
        let tool_resources = Arc::clone(&live_resources);
        let nanocodex = Nanocodex::builder(openai).tools_factory(move |_agent| {
            tool_resources.fetch_add(1, Ordering::AcqRel);
            Tools::builder()
                .without_defaults()
                .provider(AttemptResourceProvider {
                    live_resources: Arc::clone(&tool_resources),
                })
                .build()
        });
        let output = tempdir().unwrap();
        let verifier_resources = Arc::clone(&live_resources);
        let (evaluator, _events) = Evaluator::builder(nanocodex)
            .output_directory(output.path())
            .attempt_agent(move |_attempt, builder| {
                Ok::<_, Infallible>(AttemptAgent::new(builder).verifier(ResourceProbeVerifier {
                    live_resources: Arc::clone(&verifier_resources),
                }))
            })
            .build()
            .unwrap();
        let task = Task::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks/write-greeting"),
        )
        .unwrap();

        let outcome = evaluator.task(task).await.unwrap();
        let result = outcome
            .scored()
            .expect("the successful verifier must return a scored outcome");

        assert_eq!(result.status, EvalStatus::Passed);
        assert_eq!(result.outcome, EvalOutcome::Passed);
        assert!(result.exception.is_none());
        assert_eq!(result.cleanup.agent.status, CleanupStatus::Completed);
        assert_eq!(result.cleanup.verifier.status, CleanupStatus::Failed);
        assert_eq!(live_resources.load(Ordering::Acquire), 0);
        server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn verifier_primary_and_cleanup_failures_are_both_retained() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let warmup = socket.next().await.unwrap().unwrap();
            assert!(warmup.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": { "id": "resp-warmup", "usage": null }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let generation = socket.next().await.unwrap().unwrap();
            assert!(generation.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-generation",
                            "status": "completed",
                            "output": [{
                                "type": "message",
                                "role": "assistant",
                                "content": [{ "type": "output_text", "text": "done" }]
                            }],
                            "usage": {
                                "input_tokens": 1,
                                "input_tokens_details": { "cached_tokens": 0 },
                                "output_tokens": 1,
                                "output_tokens_details": { "reasoning_tokens": 0 },
                                "total_tokens": 2
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            while socket.next().await.is_some() {}
        });
        let openai = OpenAi::builder("test")
            .websocket_url(endpoint)
            .build()
            .unwrap();
        let output = tempdir().unwrap();
        let (evaluator, _events) = Evaluator::builder(Nanocodex::builder(openai))
            .output_directory(output.path())
            .attempt_agent(|_attempt, builder| {
                Ok::<_, Infallible>(AttemptAgent::new(builder).verifier(FailingCleanupVerifier))
            })
            .build()
            .unwrap();
        let task = Task::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks/write-greeting"),
        )
        .unwrap();

        let outcome = evaluator.task(task).await.unwrap();
        let failure = outcome
            .unscored()
            .expect("verifier execution failure must be unscored");

        assert_eq!(failure.exception.kind, crate::EvalExceptionKind::Verifier);
        assert!(
            failure
                .exception
                .message
                .contains("deterministic verifier primary failure")
        );
        assert!(
            failure
                .exception
                .traceback
                .contains("deterministic verifier primary failure")
        );
        assert_eq!(failure.cleanup.verifier.status, CleanupStatus::Failed);
        assert!(
            failure
                .cleanup
                .verifier
                .timing
                .as_ref()
                .is_some_and(|timing| { failure.exception.occurred_at <= timing.started_at })
        );
        assert!(
            failure
                .cleanup
                .verifier
                .diagnostic
                .as_ref()
                .is_some_and(|diagnostic| diagnostic
                    .message
                    .contains("deterministic verifier cleanup failure"))
        );
        assert!(failure.timing.verifier.is_some());
        server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn verifier_is_joined_after_post_agent_package_validation_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let warmup = socket.next().await.unwrap().unwrap();
            assert!(warmup.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": { "id": "resp-warmup", "usage": null }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let generation = socket.next().await.unwrap().unwrap();
            assert!(generation.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-generation",
                            "status": "completed",
                            "output": [{
                                "type": "message",
                                "role": "assistant",
                                "content": [{ "type": "output_text", "text": "done" }]
                            }],
                            "usage": {
                                "input_tokens": 1,
                                "input_tokens_details": { "cached_tokens": 0 },
                                "output_tokens": 1,
                                "output_tokens_details": { "reasoning_tokens": 0 },
                                "total_tokens": 2
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            while socket.next().await.is_some() {}
        });
        let (task_directory, task) = task_with_agent_timeout(5.0);
        let mutation = task_directory.path().join("environment/README.md");
        let openai = OpenAi::builder("test")
            .websocket_url(endpoint)
            .build()
            .unwrap();
        let nanocodex = Nanocodex::builder(openai).tools_factory(move |_agent| {
            Tools::builder()
                .without_defaults()
                .provider(PackageMutatingProvider {
                    mutation: mutation.clone(),
                })
                .build()
        });
        let output = tempdir().unwrap();
        let verifier_shutdowns = Arc::new(AtomicUsize::new(0));
        let verifier_shutdowns_for_attempt = Arc::clone(&verifier_shutdowns);
        let (evaluator, _events) = Evaluator::builder(nanocodex)
            .output_directory(output.path())
            .attempt_agent(move |_attempt, builder| {
                Ok::<_, Infallible>(AttemptAgent::new(builder).verifier(ShutdownProbeVerifier {
                    shutdowns: Arc::clone(&verifier_shutdowns_for_attempt),
                }))
            })
            .build()
            .unwrap();

        let outcome = evaluator.task(task).await.unwrap();
        let failure = outcome
            .unscored()
            .expect("post-agent package mutation must be returned as unscored");

        assert_eq!(
            failure.exception.kind,
            crate::EvalExceptionKind::Environment
        );
        assert_eq!(failure.cleanup.agent.status, CleanupStatus::Completed);
        assert_eq!(failure.cleanup.verifier.status, CleanupStatus::Completed);
        assert_eq!(verifier_shutdowns.load(Ordering::Acquire), 1);
        assert!(
            failure
                .cleanup
                .verifier
                .timing
                .as_ref()
                .is_some_and(|timing| { failure.exception.occurred_at <= timing.started_at })
        );
        server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn safety_refusal_runs_verifier_and_retains_independent_score_axes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let warmup = socket.next().await.unwrap().unwrap();
            assert!(warmup.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": { "id": "resp-warmup", "usage": null }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let generation = socket.next().await.unwrap().unwrap();
            assert!(generation.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.failed",
                        "response": {
                            "id": "resp-failed",
                            "status": "failed",
                            "error": {
                                "code": "cyber_policy",
                                "message": "deterministic safety refusal"
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            while socket.next().await.is_some() {}
        });
        let live_resources = Arc::new(AtomicUsize::new(0));
        let openai = OpenAi::builder("test")
            .websocket_url(endpoint)
            .build()
            .unwrap();
        let tool_resources = Arc::clone(&live_resources);
        let nanocodex = Nanocodex::builder(openai).tools_factory(move |_agent| {
            tool_resources.fetch_add(1, Ordering::AcqRel);
            Tools::builder()
                .without_defaults()
                .provider(AttemptResourceProvider {
                    live_resources: Arc::clone(&tool_resources),
                })
                .build()
        });
        let output = tempdir().unwrap();
        let verifier_resources = Arc::clone(&live_resources);
        let (evaluator, events) = Evaluator::builder(nanocodex)
            .output_directory(output.path())
            .attempt_agent(move |_attempt, builder| {
                Ok::<_, Infallible>(
                    AttemptAgent::new(builder).verifier(ResourceCheckedVerifier {
                        inner: StaticVerifier { reward: 1.0 },
                        live_resources: Arc::clone(&verifier_resources),
                    }),
                )
            })
            .build()
            .unwrap();
        let recorder = crate::harbor::Harbor::new(&evaluator)
            .unwrap()
            .record(events.subscribe())
            .unwrap();
        let task = Task::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks/write-greeting"),
        )
        .unwrap();

        let outcome = evaluator
            .task(task)
            .await
            .expect("an accepted provider failure must return a terminal outcome");

        assert_eq!(live_resources.load(Ordering::Acquire), 0);
        let result = outcome
            .scored()
            .expect("a healthy verifier must score a provider safety refusal");
        assert_eq!(result.status, EvalStatus::Passed);
        assert_eq!(result.outcome, EvalOutcome::SafetyRefusal);
        assert_eq!(
            result.exception.as_ref().map(|exception| exception.kind),
            Some(crate::EvalExceptionKind::AgentSafetyRefusal)
        );
        assert_eq!(result.verifier.rewards["reward"], 1.0);
        assert_eq!(result.cleanup.agent.status, CleanupStatus::Completed);
        assert_eq!(result.cleanup.verifier.status, CleanupStatus::NotRequired);
        assert!(result.cleanup.agent.timing.is_some());
        assert!(result.timing.queue_wait.finished_at >= result.timing.queue_wait.started_at);
        let agent = result
            .agent
            .as_ref()
            .expect("terminal run metrics must survive the provider failure");
        assert_eq!(agent.metadata.status, AgentStatus::Failed);
        assert_eq!(agent.billing_completeness, BillingCompleteness::Unknown);
        let job = recorder.finish(vec![outcome]).await.unwrap();
        let aggregate = job.aggregate_dataset().unwrap();
        let fact = &aggregate.attempts[0];
        assert!(fact.scored);
        assert!(fact.passed);
        assert!(fact.errored);
        assert!(fact.refused);
        assert_eq!(
            fact.exception_kind,
            Some(crate::EvalExceptionKind::AgentSafetyRefusal)
        );
        assert_eq!(aggregate.configurations[0].success.successes, 1);
        assert_eq!(aggregate.configurations[0].errored_attempts, 1);
        assert_eq!(aggregate.configurations[0].refused_attempts, 1);
        let job_result: Value =
            serde_json::from_slice(&fs::read(job.directory().join("result.json")).unwrap())
                .unwrap();
        assert_eq!(job_result["stats"]["n_completed_trials"], 1);
        assert_eq!(job_result["stats"]["n_errored_trials"], 1);
        let eval = job_result["stats"]["evals"]
            .as_object()
            .and_then(|evals| evals.values().next())
            .unwrap();
        assert_eq!(eval["n_trials"], 1);
        assert_eq!(eval["n_errors"], 1);
        server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn verifier_is_joined_after_attempt_readiness_failure() {
        let output = tempdir().unwrap();
        let verifier_shutdowns = Arc::new(AtomicUsize::new(0));
        let verifier_shutdowns_for_attempt = Arc::clone(&verifier_shutdowns);
        let nanocodex = Nanocodex::builder(OpenAi::new("test").unwrap());
        let (evaluator, _events) = Evaluator::builder(nanocodex)
            .output_directory(output.path())
            .attempt_agent(move |_attempt, builder| {
                Ok::<_, Infallible>(
                    AttemptAgent::new(builder)
                        .ready(async {
                            Err(std::io::Error::other(
                                "deterministic attempt readiness failure",
                            ))
                        })
                        .verifier(ShutdownProbeVerifier {
                            shutdowns: Arc::clone(&verifier_shutdowns_for_attempt),
                        }),
                )
            })
            .build()
            .unwrap();
        let task = Task::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks/write-greeting"),
        )
        .unwrap();

        let outcome = evaluator.task(task).await.unwrap();
        let failure = outcome
            .unscored()
            .expect("readiness failure must be returned as unscored");

        assert_eq!(
            failure.exception.kind,
            crate::EvalExceptionKind::Environment
        );
        assert_eq!(failure.cleanup.agent.status, CleanupStatus::NotRequired);
        assert_eq!(failure.cleanup.verifier.status, CleanupStatus::Completed);
        assert_eq!(verifier_shutdowns.load(Ordering::Acquire), 1);
        assert!(
            failure
                .cleanup
                .verifier
                .timing
                .as_ref()
                .is_some_and(|timing| { failure.exception.occurred_at <= timing.started_at })
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn model_in_flight_timeout_can_retain_a_passing_verifier_score() {
        let run = run_timed_out_attempt(|| StaticVerifier { reward: 1.0 }).await;
        let result = run
            .outcome
            .scored()
            .expect("a completed verifier must make the timeout scored");

        assert_eq!(run.live_resources, 0);
        assert_eq!(run.terminal_events, 1);
        assert_eq!(result.status, EvalStatus::Passed);
        assert_eq!(result.outcome, EvalOutcome::AgentTimeout);
        assert_eq!(
            result.exception.as_ref().map(|exception| exception.kind),
            Some(EvalExceptionKind::AgentTimeout)
        );
        assert!(
            result
                .exception
                .as_ref()
                .is_some_and(|exception| exception.occurred_at <= result.timing.verifier.started_at)
        );
        let agent = result
            .agent
            .as_ref()
            .expect("cancellation must retain a partial terminal snapshot");
        assert_eq!(agent.metadata.status, AgentStatus::Cancelled);
        assert_eq!(agent.billing_completeness, BillingCompleteness::Unknown);
        assert_eq!(
            agent.metadata.runtime_completeness,
            crate::MeasurementCompleteness::ObservedLowerBound
        );
        assert_eq!(run.trial["scored"], true);
        assert_eq!(run.trial["outcome"], "agent_timeout");
        assert_eq!(
            run.trial["exception_info"]["exception_type"],
            "AgentTimeoutError"
        );
        assert_eq!(run.trial["verifier_result"]["rewards"]["reward"], 1.0);
        assert_eq!(run.trial["agent_result"]["billing_completeness"], "unknown");
        assert_eq!(
            run.trial["agent_result"]["metadata"]["runtime_completeness"],
            "observed_lower_bound"
        );
        assert_eq!(run.job["stats"]["n_completed_trials"], 1);
        assert_eq!(run.job["stats"]["n_errored_trials"], 1);
        assert_eq!(run.job["stats"]["n_billing_missing_trials"], 1);
        let eval = run.job["stats"]["evals"]
            .as_object()
            .and_then(|evals| evals.values().next())
            .expect("the scored timeout must contribute one eval aggregate");
        assert_eq!(eval["n_trials"], 1);
        assert_eq!(eval["n_errors"], 1);
        assert_eq!(
            run.aggregate.configurations[0].tokens.total_tokens.samples,
            0
        );
        assert_eq!(
            run.aggregate.configurations[0]
                .observed_tokens_lower_bound
                .total_tokens
                .samples,
            0
        );
        assert_eq!(run.aggregate.configurations[0].billing_missing_attempts, 1);
        assert!(run.aggregate.attempts[0].usage.is_none());
        assert_eq!(
            run.aggregate.attempts[0]
                .runtime
                .as_ref()
                .map(|runtime| runtime.completeness),
            Some(crate::MeasurementCompleteness::ObservedLowerBound)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn model_in_flight_timeout_can_retain_a_failing_verifier_score() {
        let run = run_timed_out_attempt(|| StaticVerifier { reward: 0.0 }).await;
        let result = run
            .outcome
            .scored()
            .expect("a completed verifier must make the timeout scored");

        assert_eq!(run.terminal_events, 1);
        assert_eq!(result.status, EvalStatus::Failed);
        assert_eq!(result.outcome, EvalOutcome::AgentTimeout);
        assert_eq!(
            result.exception.as_ref().map(|exception| exception.kind),
            Some(EvalExceptionKind::AgentTimeout)
        );
        assert_eq!(run.trial["verifier_result"]["rewards"]["reward"], 0.0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn verifier_failure_after_timeout_preserves_the_agent_error() {
        let run = run_timed_out_attempt(|| FailingCleanupVerifier).await;
        let failure = run
            .outcome
            .unscored()
            .expect("a verifier execution failure cannot retain a score");

        assert_eq!(run.terminal_events, 1);
        assert_eq!(failure.exception.kind, EvalExceptionKind::AgentTimeout);
        assert_eq!(failure.exception.outcome, EvalOutcome::AgentTimeout);
        assert!(failure.timing.verifier.as_ref().is_some_and(|timing| {
            failure.exception.occurred_at <= timing.started_at
                && timing.finished_at <= failure.finished_at
        }));
        assert_eq!(failure.cleanup.verifier.status, CleanupStatus::Failed);
        assert!(failure.verifier.is_none());
        assert_eq!(
            run.trial["exception_info"]["exception_type"],
            "AgentTimeoutError"
        );
        assert!(run.trial["verifier_result"].is_null());
        assert_eq!(
            run.trajectory["final_metrics"]["extra"]["billing_completeness"],
            "unknown"
        );
        assert!(
            run.trajectory["final_metrics"]["extra"]["response_attempts"]
                .as_u64()
                .is_some_and(|attempts| attempts > 0)
        );
        assert_eq!(run.job["stats"]["n_completed_trials"], 1);
        assert_eq!(run.job["stats"]["n_errored_trials"], 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn post_setup_agent_failure_can_retain_a_verifier_score() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": { "id": "resp-warmup", "usage": null }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.failed",
                        "response": {
                            "id": "resp-failed",
                            "status": "failed",
                            "error": {
                                "code": "invalid_request_error",
                                "message": "deterministic post-setup agent failure"
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            while socket.next().await.is_some() {}
        });
        let openai = OpenAi::builder("test")
            .websocket_url(endpoint)
            .build()
            .unwrap();
        let output = tempdir().unwrap();
        let (evaluator, _events) = Evaluator::builder(Nanocodex::builder(openai))
            .output_directory(output.path())
            .attempt_agent(|_attempt, builder| {
                Ok::<_, Infallible>(
                    AttemptAgent::new(builder).verifier(StaticVerifier { reward: 1.0 }),
                )
            })
            .build()
            .unwrap();
        let task =
            Task::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks/write-greeting"))
                .unwrap();

        let outcome = evaluator.task(task).await.unwrap();
        let result = outcome
            .scored()
            .expect("post-setup agent failure must retain completed verification");
        assert_eq!(result.status, EvalStatus::Passed);
        assert_eq!(result.outcome, EvalOutcome::InfrastructureError);
        assert_eq!(
            result.exception.as_ref().map(|exception| exception.kind),
            Some(EvalExceptionKind::Agent)
        );
        assert_eq!(
            result.agent.as_ref().map(|agent| agent.metadata.status),
            Some(AgentStatus::Failed)
        );
        server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_terminal_metrics_retain_usage_and_verifier_score() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let warmup = socket.next().await.unwrap().unwrap();
            assert!(warmup.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": { "id": "resp-warmup", "usage": null }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let generation = socket.next().await.unwrap().unwrap();
            assert!(generation.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp-generation",
                            "status": "completed",
                            "output": [{
                                "type": "message",
                                "role": "assistant",
                                "content": [{ "type": "output_text", "text": "done" }]
                            }],
                            "usage": {
                                "input_tokens": 1,
                                "input_tokens_details": { "cached_tokens": 0 },
                                "output_tokens": 1,
                                "output_tokens_details": { "reasoning_tokens": 0 },
                                "total_tokens": 2
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            while socket.next().await.is_some() {}
        });
        let openai = OpenAi::builder("test")
            .websocket_url(endpoint)
            .build()
            .unwrap();
        let output = tempdir().unwrap();
        let (evaluator, events) = Evaluator::builder(Nanocodex::builder(openai))
            .with_malformed_terminal_metrics()
            .output_directory(output.path())
            .attempt_agent(|_attempt, builder| {
                Ok::<_, Infallible>(
                    AttemptAgent::new(builder).verifier(StaticVerifier { reward: 1.0 }),
                )
            })
            .build()
            .unwrap();
        let recorder = crate::harbor::Harbor::new(&evaluator)
            .unwrap()
            .record(events.subscribe())
            .unwrap();
        let task =
            Task::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks/write-greeting"))
                .unwrap();

        let outcome = evaluator.task(task).await.unwrap();
        let result = outcome
            .scored()
            .expect("malformed terminal metrics must not prevent verification");

        assert_eq!(result.status, EvalStatus::Passed);
        assert_eq!(result.outcome, EvalOutcome::InfrastructureError);
        assert_eq!(
            result.exception.as_ref().map(|exception| exception.kind),
            Some(EvalExceptionKind::Agent)
        );
        assert!(
            result
                .exception
                .as_ref()
                .is_some_and(|exception| exception.message.contains("terminal metrics"))
        );
        let agent = result
            .agent
            .as_ref()
            .expect("completed operation metrics must remain available");
        assert_eq!(agent.metadata.status, AgentStatus::Completed);
        assert_eq!(agent.billing_completeness, BillingCompleteness::Unknown);
        assert_eq!(agent.cost_usd, None);
        assert_eq!(
            agent.metadata.cost_status,
            CostStatus::UsageNotReported.as_str()
        );
        assert!(agent.metadata.estimated_cost.is_none());
        assert_eq!(result.verifier.rewards.get("reward").copied(), Some(1.0));
        let attempt_directory = result.artifacts.directory.clone();
        let job = recorder.finish(vec![outcome]).await.unwrap();
        let aggregate = job.aggregate_dataset().unwrap();
        assert!(aggregate.attempts[0].estimated_cost.is_none());
        assert_eq!(
            aggregate.configurations[0]
                .observed_cost_components_lower_bound_usd
                .total_usd
                .samples,
            0
        );
        assert_eq!(
            aggregate.configurations[0]
                .cost_components_usd
                .total_usd
                .samples,
            0
        );
        assert_eq!(
            aggregate.configurations[0]
                .observed_tokens_lower_bound
                .total_tokens
                .samples,
            1
        );
        assert_eq!(aggregate.configurations[0].tokens.total_tokens.samples, 0);
        let events = fs::read_to_string(attempt_directory.join("agent/events.jsonl"))
            .expect("retained agent events");
        let terminal_event = events
            .lines()
            .rev()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|event| matches!(event["type"].as_str(), Some("run.completed" | "run.failed")))
            .expect("retained terminal event");
        assert_eq!(terminal_event["type"], "run.completed");
        let retained_result: Value = serde_json::from_slice(
            &fs::read(attempt_directory.join("result.json")).expect("retained Harbor result"),
        )
        .expect("valid retained Harbor result");
        assert_eq!(
            retained_result["agent_result"]["metadata"]["status"],
            "completed"
        );
        let trajectory: Value = serde_json::from_slice(
            &fs::read(attempt_directory.join("agent/trajectory.json"))
                .expect("retained ATIF trajectory"),
        )
        .expect("valid retained ATIF trajectory");
        let terminal_step = trajectory["steps"]
            .as_array()
            .and_then(|steps| steps.iter().rev().find(|step| step["source"] == "agent"))
            .expect("terminal ATIF agent step");
        assert_eq!(
            terminal_step["extra"]["terminal_event_type"],
            "run.completed"
        );
        assert_eq!(
            terminal_step["extra"]["terminal_payload"]["status"],
            "completed"
        );
        server.await.unwrap();
    }

    #[test]
    fn malformed_terminal_metrics_are_a_verifier_usable_agent_failure() {
        let error = super::EvalError::AgentTerminal(
            serde_json::from_str::<Value>("{").expect_err("fixture must be malformed"),
        );

        assert_eq!(super::failure_kind(&error), EvalExceptionKind::Agent);
        assert!(super::verifier_workspace_usable_after_agent_error(&error));
    }

    #[test]
    fn completed_model_call_leaves_idle_tool_timeout_billing_complete() {
        let mut observation = AgentObservation::default();
        observation.observe_lifecycle(AgentEventKind::ModelCallStarted);
        observation.observe_lifecycle(AgentEventKind::ModelCallCompleted);

        assert_eq!(observation.billable_in_flight, 0);
        assert_eq!(
            observation.billing_completeness(),
            BillingCompleteness::Complete
        );
    }

    #[test]
    fn shared_prefix_warmup_is_not_a_billable_completed_response() {
        let mut observation = AgentObservation::default();
        observation
            .observe(&agent_event(
                1,
                AgentEventKind::ModelWarmupStarted,
                json!({"model": "gpt-5.6-sol", "prompt_cache_key": "shared"}),
            ))
            .unwrap();
        observation
            .observe(&agent_event(
                2,
                AgentEventKind::ModelWarmupCompleted,
                json!({
                    "source": "shared_prefix",
                    "attempt": null,
                    "connection_generation": null,
                    "duration_ns": 10,
                    "usage": null,
                }),
            ))
            .unwrap();

        assert_eq!(
            observation.billing_completeness(),
            BillingCompleteness::Complete
        );
        assert!(observation.lower_bound_result(None).is_none());
    }

    #[test]
    fn completed_operations_form_an_exact_timeout_lower_bound() {
        let warmup_usage = provider_usage(100, 40, 10, 20, 5);
        let generation_usage = provider_usage(200, 100, 20, 30, 8);
        let compaction_usage = provider_usage(50, 10, 5, 6, 0);
        let events = [
            agent_event(
                1,
                AgentEventKind::RunStarted,
                json!({
                    "mode": "openai_model",
                    "model": "gpt-5.6-sol",
                    "reasoning_mode": "summary",
                    "effort": "high",
                    "transport": "websocket",
                    "orchestration": "agent",
                    "websocket_url": "wss://example.invalid",
                    "workspace": null,
                    "instruction_bytes": 4,
                }),
            ),
            agent_event(
                2,
                AgentEventKind::ModelWarmupStarted,
                json!({"model": "gpt-5.6-sol", "prompt_cache_key": "cache"}),
            ),
            agent_event(
                3,
                AgentEventKind::ModelWarmupCompleted,
                json!({
                    "source": "response",
                    "attempt": 1,
                    "connection_generation": 1,
                    "duration_ns": 11,
                    "usage": warmup_usage,
                }),
            ),
            agent_event(
                4,
                AgentEventKind::ModelCallStarted,
                json!({
                    "call_index": 1,
                    "model": "gpt-5.6-sol",
                    "reasoning_mode": "summary",
                    "effort": "high",
                }),
            ),
            agent_event(
                5,
                AgentEventKind::ModelCallCompleted,
                json!({
                    "call_index": 1,
                    "model": "gpt-5.6-sol",
                    "attempt": 2,
                    "connection_generation": 1,
                    "status": "completed",
                    "duration_ns": 22,
                    "time_to_first_event_ns": 2,
                    "time_to_first_output_ns": 3,
                    "tool_calls": 2,
                    "usage": generation_usage,
                }),
            ),
            agent_event(
                6,
                AgentEventKind::ModelCompactionStarted,
                json!({
                    "after_model_call_index": 1,
                    "active_context_tokens": 100,
                    "auto_compact_token_limit": 90,
                }),
            ),
            agent_event(
                7,
                AgentEventKind::ModelCompactionCompleted,
                json!({
                    "after_model_call_index": 1,
                    "attempt": 1,
                    "connection_generation": 1,
                    "status": "completed",
                    "duration_ns": 33,
                    "time_to_first_event_ns": 3,
                    "time_to_first_output_ns": 4,
                    "usage": compaction_usage,
                }),
            ),
            agent_event(
                8,
                AgentEventKind::ModelCallStarted,
                json!({
                    "call_index": 2,
                    "model": "gpt-5.6-sol",
                    "reasoning_mode": "summary",
                    "effort": "high",
                }),
            ),
        ];
        let mut observation = AgentObservation::default();
        for event in &events {
            observation.observe(event).unwrap();
        }

        let selection = observation.select_result(None, BillingCompleteness::Complete);
        assert!(selection.used_lower_bound);
        assert!(selection.terminal_error.is_none());
        let mut outcome = super::AgentTurnOutcome {
            primary: None,
            result: selection.result,
            result_is_lower_bound: selection.used_lower_bound,
        };
        outcome.apply_lower_bound_duration(9_876_543);
        let result = outcome.result.unwrap();

        assert_eq!(result.billing_completeness, BillingCompleteness::Unknown);
        assert_eq!(result.metadata.status, AgentStatus::Cancelled);
        assert_eq!(result.metadata.duration_ns, 9_876_543);
        assert_eq!(result.metadata.duration_ms, 9);
        assert_eq!(result.model_calls, 2);
        assert_eq!(
            result.metadata.runtime_completeness,
            crate::MeasurementCompleteness::ObservedLowerBound
        );
        assert_eq!(result.tool_calls, 2);
        assert_eq!(result.usage.input_tokens, 250);
        assert_eq!(result.usage.cached_input_tokens, 110);
        assert_eq!(result.usage.cache_write_input_tokens, 25);
        assert_eq!(result.usage.output_tokens, 36);
        assert_eq!(result.usage.reasoning_output_tokens, 8);
        assert_eq!(result.usage.total_tokens, 286);
        assert_eq!(result.metadata.warmup_usage.input_tokens, 100);
        assert_eq!(result.metadata.warmup_usage.cached_input_tokens, 40);
        assert_eq!(result.metadata.warmup_usage.output_tokens, 20);
        assert_eq!(result.metadata.compactions, 1);
        assert_eq!(result.metadata.response_attempts, 4);
        assert_eq!(result.metadata.response_retries, 1);
        assert_eq!(result.cost_usd, None);
        assert_eq!(
            result.metadata.cost_status,
            CostStatus::UsageNotReported.as_str()
        );
        assert!(result.metadata.estimated_cost.is_none());
    }

    #[test]
    fn timeout_reconstructs_observed_tool_and_transport_lower_bounds() {
        let events = [
            agent_event(
                1,
                AgentEventKind::RunStarted,
                json!({
                    "mode": "openai_model",
                    "model": "gpt-5.6-sol",
                    "reasoning_mode": "summary",
                    "effort": "high",
                    "transport": "responses_websocket_v2",
                    "orchestration": "agent",
                    "websocket_url": "wss://example.invalid",
                    "workspace": null,
                    "instruction_bytes": 4,
                }),
            ),
            agent_event(2, AgentEventKind::ModelConnectionStarted, json!({})),
            agent_event(
                3,
                AgentEventKind::ModelConnectionCompleted,
                json!({"purpose": "initial", "duration_ns": 10}),
            ),
            agent_event(4, AgentEventKind::ModelAttemptStarted, json!({})),
            agent_event(
                5,
                AgentEventKind::ModelAttemptRetrying,
                json!({"delay_ns": 7}),
            ),
            agent_event(6, AgentEventKind::ModelAttemptStarted, json!({})),
            agent_event(7, AgentEventKind::ModelConnectionStarted, json!({})),
            agent_event(
                8,
                AgentEventKind::ModelConnectionFailed,
                json!({"duration_ns": 11}),
            ),
            agent_event(9, AgentEventKind::ModelConnectionStarted, json!({})),
            agent_event(
                10,
                AgentEventKind::ModelConnectionCompleted,
                json!({"purpose": "reconnect", "duration_ns": 12}),
            ),
            agent_event(
                11,
                AgentEventKind::ModelCallStarted,
                json!({
                    "call_index": 1,
                    "model": "gpt-5.6-sol",
                    "reasoning_mode": "summary",
                    "effort": "high",
                }),
            ),
            agent_event(
                12,
                AgentEventKind::ModelCallCompleted,
                json!({
                    "call_index": 1,
                    "model": "gpt-5.6-sol",
                    "attempt": 2,
                    "connection_generation": 1,
                    "status": "completed",
                    "duration_ns": 13,
                    "time_to_first_event_ns": 2,
                    "time_to_first_output_ns": 3,
                    "tool_calls": 1,
                    "usage": null,
                }),
            ),
            agent_event(13, AgentEventKind::ToolCall, json!({})),
            agent_event(
                14,
                AgentEventKind::ToolResult,
                json!({
                    "call_id": "call-1",
                    "tool": "shell",
                    "status": "completed",
                    "duration_ns": 42,
                    "started_after_ns": null,
                    "result": "done",
                    "metadata": null,
                }),
            ),
            agent_event(
                15,
                AgentEventKind::ModelCallStarted,
                json!({
                    "call_index": 2,
                    "model": "gpt-5.6-sol",
                    "reasoning_mode": "summary",
                    "effort": "high",
                }),
            ),
        ];
        let mut observation = AgentObservation::default();
        for event in &events {
            observation.observe(event).unwrap();
        }

        let result = observation
            .select_result(None, BillingCompleteness::Unknown)
            .result
            .expect("run activity must produce a partial runtime snapshot");

        assert_eq!(
            result.metadata.runtime_completeness,
            crate::MeasurementCompleteness::ObservedLowerBound
        );
        assert_eq!(result.metadata.model_calls, 2);
        assert_eq!(result.metadata.tool_calls, 1);
        assert_eq!(result.metadata.connection_attempts, 3);
        assert_eq!(result.metadata.websocket_reconnects, 1);
        assert_eq!(result.metadata.response_attempts, 2);
        assert_eq!(result.metadata.response_retries, 1);
        assert_eq!(result.metadata.connection_duration_ns, 33);
        assert_eq!(result.metadata.retry_backoff_duration_ns, 7);
        assert_eq!(result.metadata.model_duration_ns, 13);
        assert_eq!(result.metadata.tool_work_duration_ns, 42);
        assert_eq!(result.metadata.tool_wall_duration_ns, 0);
        assert_eq!(
            result.metadata.cost_status,
            CostStatus::UsageNotReported.as_str()
        );
        assert!(result.metadata.estimated_cost.is_none());
    }

    #[test]
    fn missing_usage_marks_terminal_unknown_and_terminal_metrics_take_precedence() {
        let reported_usage = provider_usage(10, 2, 1, 4, 1);
        let mut observation = AgentObservation::default();
        for event in [
            agent_event(
                1,
                AgentEventKind::RunStarted,
                json!({
                    "mode": "openai_model",
                    "model": "gpt-5.6-sol",
                    "reasoning_mode": "summary",
                    "effort": "medium",
                    "transport": "websocket",
                    "orchestration": "agent",
                    "websocket_url": "wss://example.invalid",
                    "workspace": null,
                    "instruction_bytes": 4,
                }),
            ),
            agent_event(
                2,
                AgentEventKind::ModelCallStarted,
                json!({
                    "call_index": 1,
                    "model": "gpt-5.6-sol",
                    "reasoning_mode": "summary",
                    "effort": "medium",
                }),
            ),
            agent_event(
                3,
                AgentEventKind::ModelCallCompleted,
                json!({
                    "call_index": 1,
                    "model": "gpt-5.6-sol",
                    "attempt": 1,
                    "connection_generation": 1,
                    "status": "completed",
                    "duration_ns": 10,
                    "time_to_first_event_ns": 1,
                    "time_to_first_output_ns": 2,
                    "tool_calls": 0,
                    "usage": reported_usage,
                }),
            ),
            agent_event(
                4,
                AgentEventKind::ModelCallStarted,
                json!({
                    "call_index": 2,
                    "model": "gpt-5.6-sol",
                    "reasoning_mode": "summary",
                    "effort": "medium",
                }),
            ),
            agent_event(
                5,
                AgentEventKind::ModelCallCompleted,
                json!({
                    "call_index": 2,
                    "model": "gpt-5.6-sol",
                    "attempt": 1,
                    "connection_generation": 1,
                    "status": "completed",
                    "duration_ns": 10,
                    "time_to_first_event_ns": 1,
                    "time_to_first_output_ns": null,
                    "tool_calls": 0,
                    "usage": null,
                }),
            ),
        ] {
            observation.observe(&event).unwrap();
        }

        assert_eq!(observation.billable_in_flight, 0);
        assert_eq!(
            observation.billing_completeness(),
            BillingCompleteness::Unknown
        );
        let fallback = observation
            .select_result(None, BillingCompleteness::Complete)
            .result
            .unwrap();
        assert_eq!(fallback.billing_completeness, BillingCompleteness::Unknown);
        assert_eq!(fallback.model_calls, 2);
        assert_eq!(fallback.cost_usd, None);
        assert_eq!(
            fallback.metadata.cost_status,
            CostStatus::UsageNotReported.as_str()
        );

        let terminal = agent_event(6, AgentEventKind::RunCompleted, terminal_payload(77, 0.75));
        let terminal_result =
            observation.select_result(Some(&terminal), observation.billing_completeness());
        assert!(!terminal_result.used_lower_bound);
        assert!(terminal_result.terminal_error.is_none());
        let terminal_result = terminal_result.result.unwrap();
        assert_eq!(terminal_result.model_calls, 77);
        assert_eq!(terminal_result.usage.input_tokens, 7);
        assert_eq!(terminal_result.cost_usd, Some(0.75));
        assert_eq!(
            terminal_result.billing_completeness,
            BillingCompleteness::Unknown
        );

        let invalid_terminal =
            agent_event(7, AgentEventKind::RunCompleted, json!({"invalid": true}));
        let invalid =
            observation.select_result(Some(&invalid_terminal), BillingCompleteness::Complete);
        assert!(invalid.used_lower_bound);
        assert!(invalid.terminal_error.is_some());
        let invalid = invalid.result.unwrap();
        assert_eq!(invalid.metadata.status, AgentStatus::Completed);
        assert_eq!(invalid.cost_usd, None);

        let invalid_terminal = agent_event(8, AgentEventKind::RunFailed, json!({"invalid": true}));
        let invalid =
            observation.select_result(Some(&invalid_terminal), BillingCompleteness::Complete);
        assert_eq!(invalid.result.unwrap().metadata.status, AgentStatus::Failed);
    }

    #[test]
    fn failed_model_call_without_a_sent_attempt_keeps_billing_complete() {
        let mut observation = AgentObservation::default();
        observation.observe_lifecycle(AgentEventKind::ModelCallStarted);
        observation.observe_lifecycle(AgentEventKind::ModelCallFailed);

        assert_eq!(observation.billable_in_flight, 0);
        assert_eq!(
            observation.billing_completeness(),
            BillingCompleteness::Complete
        );
    }

    #[test]
    fn failed_sent_attempt_marks_billing_snapshot_unknown() {
        let mut observation = AgentObservation::default();
        observation
            .observe(&agent_event(
                1,
                AgentEventKind::ModelAttemptFailed,
                json!({"billing_uncertain": true}),
            ))
            .unwrap();

        assert_eq!(
            observation.billing_completeness(),
            BillingCompleteness::Unknown
        );
        assert_eq!(observation.billing_uncertain_response_attempts, 1);
    }

    #[test]
    fn retained_terminal_metadata_accepts_legacy_billing_field_and_writes_new_name() {
        let mut payload = terminal_payload(1, 0.25);
        payload
            .as_object_mut()
            .unwrap()
            .insert("accepted_abandoned_response_attempts".to_owned(), json!(2));

        let metadata: crate::AgentMetadata = serde_json::from_value(payload).unwrap();
        assert_eq!(metadata.billing_uncertain_response_attempts, 2);
        let encoded = serde_json::to_value(metadata).unwrap();
        assert_eq!(encoded["billing_uncertain_response_attempts"], 2);
        assert!(
            encoded
                .get("accepted_abandoned_response_attempts")
                .is_none()
        );
    }

    fn provider_usage(
        input_tokens: u64,
        cached_tokens: u64,
        cache_write_tokens: u64,
        output_tokens: u64,
        reasoning_tokens: u64,
    ) -> Usage {
        Usage {
            input_tokens,
            input_tokens_details: Some(InputTokenDetails {
                cached_tokens,
                cache_write_tokens,
            }),
            output_tokens,
            output_tokens_details: Some(OutputTokenDetails { reasoning_tokens }),
            total_tokens: input_tokens.saturating_add(output_tokens),
        }
    }

    fn agent_event(seq: u64, kind: AgentEventKind, payload: Value) -> AgentEvent {
        serde_json::from_value(json!({
            "protocol_version": 1,
            "request_id": "test-request",
            "seq": seq,
            "type": kind,
            "payload": payload,
        }))
        .unwrap()
    }

    fn terminal_payload(model_calls: u32, cost_usd: f64) -> Value {
        json!({
            "status": "completed",
            "model": "terminal-model",
            "effort": "high",
            "transport": "websocket",
            "orchestration": "agent",
            "duration_ms": 5,
            "duration_ns": 5_000_000,
            "model_calls": model_calls,
            "steers": 0,
            "compactions": 0,
            "tool_calls": 0,
            "connection_attempts": 1,
            "websocket_reconnects": 0,
            "response_attempts": 1,
            "response_retries": 0,
            "connection_duration_ns": 1,
            "retry_backoff_duration_ns": 0,
            "model_duration_ns": 4,
            "warmup_duration_ns": 1,
            "tool_work_duration_ns": 0,
            "tool_wall_duration_ns": 0,
            "usage": {
                "input_tokens": 7,
                "cached_input_tokens": 0,
                "cache_write_input_tokens": 0,
                "output_tokens": 3,
                "reasoning_output_tokens": 1,
                "total_tokens": 10,
            },
            "warmup_usage": {
                "input_tokens": 0,
                "cached_input_tokens": 0,
                "cache_write_input_tokens": 0,
                "output_tokens": 0,
                "reasoning_output_tokens": 0,
                "total_tokens": 0,
            },
            "cost_usd": cost_usd,
            "cost_status": "estimated_from_usage",
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expired_terminal_grace_keeps_shutdown_joined() {
        let (release_shutdown, shutdown_released) = tokio::sync::oneshot::channel();
        let shutdown_started = Arc::new(tokio::sync::Notify::new());
        let started = Arc::clone(&shutdown_started);
        let recovery = tokio::spawn(super::recover_timed_out_agent(
            Duration::ZERO,
            async move {
                started.notify_one();
                shutdown_released.await.unwrap();
                "joined"
            },
            std::future::pending::<()>(),
        ));

        shutdown_started.notified().await;
        tokio::task::yield_now().await;
        assert!(
            !recovery.is_finished(),
            "the recovery deadline must not detach resource shutdown"
        );
        release_shutdown.send(()).unwrap();
        let recovered = recovery.await.unwrap();

        assert!(recovered.grace_elapsed);
        assert!(recovered.terminal.is_none());
        assert_eq!(recovered.shutdown, "joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_snapshot_survives_while_shutdown_remains_mandatory() {
        let (release_shutdown, shutdown_released) = tokio::sync::oneshot::channel();
        let recovery = tokio::spawn(super::recover_timed_out_agent(
            Duration::ZERO,
            async move {
                shutdown_released.await.unwrap();
                "joined"
            },
            std::future::ready("terminal"),
        ));

        tokio::task::yield_now().await;
        assert!(
            !recovery.is_finished(),
            "a retained terminal must not let the caller skip shutdown"
        );
        release_shutdown.send(()).unwrap();
        let recovered = recovery.await.unwrap();

        assert_eq!(recovered.terminal, Some("terminal"));
        assert_eq!(recovered.shutdown, "joined");
    }

    async fn run_timed_out_attempt<V, F>(make_verifier: F) -> TimeoutRun
    where
        V: AttemptVerifier + 'static,
        F: Fn() -> V + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let warmup = socket.next().await.unwrap().unwrap();
            assert!(warmup.is_text());
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "response": { "id": "resp-warmup", "usage": null }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let generation = socket.next().await.unwrap().unwrap();
            assert!(generation.is_text());
            while socket.next().await.is_some() {}
        });
        let live_resources = Arc::new(AtomicUsize::new(0));
        let openai = OpenAi::builder("test")
            .websocket_url(endpoint)
            .build()
            .unwrap();
        let tool_resources = Arc::clone(&live_resources);
        let nanocodex = Nanocodex::builder(openai).tools_factory(move |_agent| {
            tool_resources.fetch_add(1, Ordering::AcqRel);
            Tools::builder()
                .without_defaults()
                .provider(AttemptResourceProvider {
                    live_resources: Arc::clone(&tool_resources),
                })
                .build()
        });
        let output = tempdir().unwrap();
        let verifier_resources = Arc::clone(&live_resources);
        let (evaluator, events) = Evaluator::builder(nanocodex)
            .output_directory(output.path())
            .attempt_agent(move |_attempt, builder| {
                Ok::<_, Infallible>(
                    AttemptAgent::new(builder).verifier(ResourceCheckedVerifier {
                        inner: make_verifier(),
                        live_resources: Arc::clone(&verifier_resources),
                    }),
                )
            })
            .build()
            .unwrap();
        let mut event_stream = events.subscribe();
        let recorder = Harbor::new(&evaluator)
            .unwrap()
            .record(events.subscribe())
            .unwrap();
        // Leave enough headroom for a loaded parallel test runner to complete
        // the mock warmup before exercising the intentionally stalled model
        // call.
        let (_task_directory, task) = task_with_agent_timeout(0.5);

        let outcome = evaluator
            .task(task)
            .await
            .expect("an accepted timeout must return a terminal outcome");
        let mut terminal_events = 0;
        while terminal_events == 0 {
            let event = event_stream
                .recv()
                .await
                .unwrap()
                .expect("the evaluator must remain open");
            terminal_events += usize::from(matches!(
                event.kind,
                EvalEventKind::Completed(_) | EvalEventKind::Failed(_)
            ));
        }
        while let Ok(Ok(Some(event))) = timeout(Duration::from_millis(5), event_stream.recv()).await
        {
            terminal_events += usize::from(matches!(
                event.kind,
                EvalEventKind::Completed(_) | EvalEventKind::Failed(_)
            ));
        }
        let job = recorder.finish_all(1).await.unwrap();
        let aggregate = job.aggregate_dataset().unwrap();
        let trial: Value = serde_json::from_slice(
            &fs::read(
                job.directory()
                    .join(outcome.trial_name())
                    .join("result.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let trajectory: Value = serde_json::from_slice(
            &fs::read(
                job.directory()
                    .join(outcome.trial_name())
                    .join("agent/trajectory.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let job_result =
            serde_json::from_slice(&fs::read(job.directory().join("result.json")).unwrap())
                .unwrap();
        server.await.unwrap();

        TimeoutRun {
            outcome,
            trial,
            trajectory,
            job: job_result,
            aggregate,
            terminal_events,
            live_resources: live_resources.load(Ordering::Acquire),
        }
    }

    fn task_with_agent_timeout(timeout_seconds: f64) -> (tempfile::TempDir, Task) {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks/write-greeting");
        let destination = tempdir().unwrap();
        for directory in ["environment", "tests"] {
            fs::create_dir_all(destination.path().join(directory)).unwrap();
        }
        for file in [
            "instruction.md",
            "environment/Dockerfile",
            "environment/README.md",
            "tests/test.sh",
        ] {
            fs::copy(source.join(file), destination.path().join(file)).unwrap();
        }
        let manifest = fs::read_to_string(source.join("task.toml"))
            .unwrap()
            .replace(
                "timeout_sec = 300.0",
                &format!("timeout_sec = {timeout_seconds}"),
            );
        fs::write(destination.path().join("task.toml"), manifest).unwrap();
        let task = Task::load(destination.path()).unwrap();
        (destination, task)
    }
}

#[cfg(test)]
mod tracing_tests {
    use std::{
        collections::HashMap,
        fs,
        sync::{Arc, Mutex, Once, OnceLock},
        time::Duration,
    };

    use nanocodex_agent::{Nanocodex, NanocodexError, OpenAi, transport::ResponsesError};
    use tempfile::tempdir;
    use tracing::{Id, Instrument, Subscriber, field::Visit, span::Attributes};
    use tracing_subscriber::{
        Layer, layer::Context as LayerContext, prelude::*, registry::LookupSpan,
    };
    use uuid::Uuid;

    use super::{
        AdmissionController, EvalError, Evaluator, SweepCoordinate, failure_kind,
        output_aliases_task_package, trial_name, validate_attempt_environment,
    };
    use crate::{EvalExceptionKind, Sweep, Task, native::NativeAttempt, sweep::AgentId};

    #[derive(Clone, Default)]
    struct TraceCapture(Arc<Mutex<HashMap<u64, CapturedSpan>>>);
    static TRACE_CAPTURE: OnceLock<TraceCapture> = OnceLock::new();
    static TRACE_SUBSCRIBER: Once = Once::new();

    struct CapturedSpan {
        name: &'static str,
        parent: Option<u64>,
        fields: HashMap<String, String>,
    }

    struct FieldCapture<'a>(&'a mut HashMap<String, String>);

    fn install_trace_capture() -> TraceCapture {
        let capture = TRACE_CAPTURE.get_or_init(TraceCapture::default).clone();
        TRACE_SUBSCRIBER.call_once(|| {
            tracing::subscriber::set_global_default(
                tracing_subscriber::registry().with(capture.clone()),
            )
            .expect("test process has no pre-existing global tracing subscriber");
            tracing::callsite::rebuild_interest_cache();
        });
        capture
    }

    #[test]
    fn fresh_finite_run_is_bound_before_execution() {
        let output = tempdir().unwrap();
        let task = Task::load(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../tasks/write-greeting"),
        )
        .unwrap();
        let sweep = Sweep::builder()
            .task(task)
            .trials(2)
            .agent(
                "default",
                Nanocodex::builder(OpenAi::new("test-key").unwrap()),
            )
            .unwrap()
            .build()
            .unwrap();
        let (eval, _) = Evaluator::builder(Nanocodex::builder(OpenAi::new("test-key").unwrap()))
            .output_directory(output.path())
            .fresh_run(&sweep)
            .build()
            .unwrap();

        assert!(!eval.resumed());
        assert_eq!(eval.planned_attempts(), Some(2));
        assert_eq!(eval.remaining_attempts(&sweep).unwrap(), 2);
        assert!(eval.directory().join("run.json").is_file());
    }

    #[test]
    fn colliding_task_names_and_immediate_retries_get_distinct_attempt_paths() {
        let tasks = tempdir().unwrap();
        let first = write_named_task(tasks.path(), "first", "one/shared");
        let second = write_named_task(tasks.path(), "second", "two/shared");
        let coordinate = SweepCoordinate {
            agent: AgentId::new("default").unwrap(),
            trial: 1,
        };
        let ids = [
            Uuid::from_u128(0x1234_5678_0000_0000_0000_0000_0000_0001),
            Uuid::from_u128(0x1234_5678_0000_0000_0000_0000_0000_0002),
            Uuid::from_u128(0x1234_5678_0000_0000_0000_0000_0000_0003),
        ];
        let first_name = trial_name(&first, ids[0], Some(&coordinate));
        let second_name = trial_name(&second, ids[1], Some(&coordinate));
        let retry_name = trial_name(&first, ids[2], Some(&coordinate));

        let compact_ids = ids.map(|id| id.simple().to_string());
        assert_eq!(&compact_ids[0][..8], &compact_ids[1][..8]);
        assert_eq!(&compact_ids[0][..8], &compact_ids[2][..8]);
        assert_ne!(first_name, second_name);
        assert_ne!(first_name, retry_name);
        assert_ne!(second_name, retry_name);
        assert!(first_name.ends_with(&compact_ids[0]));
        assert!(second_name.ends_with(&compact_ids[1]));
        assert!(retry_name.ends_with(&compact_ids[2]));

        let output = tempdir().unwrap();
        let first_attempt = NativeAttempt::prepare(output.path(), &first_name, &first).unwrap();
        fs::write(
            first_attempt.paths.workspace.join("abandoned-partial"),
            "partial\n",
        )
        .unwrap();
        let second_attempt = NativeAttempt::prepare(output.path(), &second_name, &second).unwrap();
        let retry_attempt = NativeAttempt::prepare(output.path(), &retry_name, &first).unwrap();

        assert_ne!(first_attempt.paths.root, second_attempt.paths.root);
        assert_ne!(first_attempt.paths.root, retry_attempt.paths.root);
        assert!(
            !retry_attempt
                .paths
                .workspace
                .join("abandoned-partial")
                .exists()
        );
    }

    #[test]
    fn admission_is_work_conserving_within_memory_and_concurrency_limits() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let admission = Arc::new(AdmissionController::new(2, Some(4)));
            let three = admission.acquire(3).await.unwrap();

            assert!(
                tokio::time::timeout(Duration::from_millis(5), admission.acquire(2))
                    .await
                    .is_err()
            );
            let one = tokio::time::timeout(Duration::from_millis(5), admission.acquire(1))
                .await
                .unwrap()
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(5), admission.acquire(1))
                    .await
                    .is_err()
            );

            drop(one);
            drop(three);
            let oversized = admission.acquire(10).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(5), admission.acquire(1))
                    .await
                    .is_err()
            );
            drop(oversized);
        });
    }

    #[test]
    fn draining_closes_admission_without_cancelling_admitted_work() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let admission = Arc::new(AdmissionController::new(1, None));
            let admitted = admission.acquire(1).await.unwrap();
            let waiting = {
                let admission = Arc::clone(&admission);
                tokio::spawn(async move { admission.acquire(1).await })
            };

            assert_eq!(admission.begin_drain(), 1);
            assert_eq!(admission.begin_drain(), 1);
            assert!(waiting.await.unwrap().is_none());

            drop(admitted);
            assert!(admission.acquire(1).await.is_none());
        });
    }

    impl Visit for FieldCapture<'_> {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }
    }

    impl<S> Layer<S> for TraceCapture
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: LayerContext<'_, S>) {
            let parent = attributes
                .parent()
                .map(|parent| parent.clone().into_u64())
                .or_else(|| {
                    attributes
                        .is_contextual()
                        .then(|| context.current_span().id().map(Id::into_u64))
                        .flatten()
                });
            let mut fields = HashMap::new();
            attributes.record(&mut FieldCapture(&mut fields));
            self.0.lock().unwrap().insert(
                id.clone().into_u64(),
                CapturedSpan {
                    name: attributes.metadata().name(),
                    parent,
                    fields,
                },
            );
        }

        fn on_record(
            &self,
            id: &Id,
            values: &tracing::span::Record<'_>,
            _context: LayerContext<'_, S>,
        ) {
            if let Some(span) = self.0.lock().unwrap().get_mut(&id.clone().into_u64()) {
                values.record(&mut FieldCapture(&mut span.fields));
            }
        }
    }

    #[test]
    fn failed_attempt_does_not_cancel_pending_batch_work() {
        let capture = install_trace_capture();
        let task_root = tempdir().unwrap();
        fs::create_dir(task_root.path().join("tests")).unwrap();
        fs::create_dir(task_root.path().join("environment")).unwrap();
        fs::write(
            task_root.path().join("task.toml"),
            r#"
schema_version = "1.1"
[task]
name = "terminal-bench/traced"
description = "Tracing fixture"
[metadata]
custom_docker_compose = true
[agent]
timeout_sec = 1.0
[verifier]
timeout_sec = 1.0
[environment]
docker_image = "example/traced:latest"
cpus = 1
memory_mb = 128
storage_mb = 128
gpus = 0
allow_internet = false
"#,
        )
        .unwrap();
        fs::write(
            task_root.path().join("instruction.md"),
            "do the traced work\n",
        )
        .unwrap();
        fs::write(task_root.path().join("tests/test.sh"), "exit 0\n").unwrap();
        let task = Task::load(task_root.path()).unwrap();
        assert!(matches!(
            validate_attempt_environment(&task, false),
            Err(EvalError::UnsupportedNativeTask { .. })
        ));
        assert!(validate_attempt_environment(&task, true).is_ok());
        let output = tempdir().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let eval_id = runtime.block_on(async {
            let (eval, _events) =
                Evaluator::builder(Nanocodex::builder(OpenAi::new("test").unwrap()))
                    .output_directory(output.path())
                    .build()
                    .unwrap();
            let eval_id = eval.id().to_string();
            let result = eval
                .tasks(vec![task.clone(), task])
                .instrument(tracing::info_span!("test.parent"))
                .await;
            let outcomes = result.expect("accepted failures must remain in the batch result");
            assert_eq!(outcomes.len(), 2);
            assert!(outcomes.iter().all(|outcome| {
                outcome
                    .unscored()
                    .is_some_and(|failure| failure.exception.kind == EvalExceptionKind::Environment)
            }));
            eval_id
        });

        let spans = capture.0.lock().unwrap();
        let attempts = spans
            .iter()
            .filter(|(_, span)| {
                span.name == "eval.attempt"
                    && span.fields.get("eval.id").is_some_and(|id| id == &eval_id)
            })
            .collect::<Vec<_>>();
        assert_eq!(attempts.len(), 2);
        for (_, attempt) in &attempts {
            assert!(attempt.parent.is_none());
            assert_eq!(
                attempt.fields.get("status").map(String::as_str),
                Some("failed")
            );
            assert!(attempt.fields.contains_key("duration_ns"));
        }
        let setups = spans
            .values()
            .filter(|span| {
                span.name == "eval.environment.setup"
                    && attempts
                        .iter()
                        .any(|(attempt_id, _)| span.parent == Some(**attempt_id))
            })
            .collect::<Vec<_>>();
        assert_eq!(setups.len(), 2);
        for setup in setups {
            assert!(
                attempts
                    .iter()
                    .any(|(attempt_id, _)| setup.parent == Some(**attempt_id))
            );
            assert_eq!(
                setup.fields.get("status").map(String::as_str),
                Some("failed")
            );
            assert!(setup.fields.contains_key("duration_ns"));
        }
    }

    #[test]
    fn classifies_cyber_policy_as_an_agent_safety_refusal() {
        let error = EvalError::Nanocodex(NanocodexError::Response(
            ResponsesError::Api {
                event: r#"{"type":"error","error":{"code":"cyber_policy"}}"#.to_owned(),
            }
            .into(),
        ));

        assert_eq!(failure_kind(&error), EvalExceptionKind::AgentSafetyRefusal);
    }

    #[test]
    fn rejects_a_task_package_mutated_after_load_before_attempt_setup() {
        let tasks = tempdir().unwrap();
        let task = write_named_task(tasks.path(), "changed", "terminal-bench/changed");
        fs::write(task.environment_directory().join("late-input"), "changed\n").unwrap();
        let output = tempdir().unwrap();
        let (eval, _) = Evaluator::builder(Nanocodex::builder(OpenAi::new("test").unwrap()))
            .output_directory(output.path())
            .build()
            .unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let outcome = runtime.block_on(eval.task(task)).unwrap();
        let failure = outcome
            .unscored()
            .expect("task package mutation must be an unscored attempt");

        assert!(matches!(
            failure.exception.kind,
            EvalExceptionKind::Environment
        ));
        assert!(
            fs::read_dir(eval.directory())
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry.file_type().is_ok_and(|kind| kind.is_dir()))
        );
    }

    #[test]
    fn rejects_finite_output_nested_in_a_hashed_task_package_before_creation() {
        let tasks = tempdir().unwrap();
        let task = write_named_task(tasks.path(), "overlap", "terminal-bench/overlap");
        let output = task.environment_directory().join("retained/evals");
        let sweep = Sweep::builder()
            .task(task)
            .agent(
                "default",
                Nanocodex::builder(OpenAi::new("test-key").unwrap()),
            )
            .unwrap()
            .build()
            .unwrap();

        let result = Evaluator::builder(Nanocodex::builder(OpenAi::new("test-key").unwrap()))
            .output_directory(&output)
            .fresh_run(&sweep)
            .build();

        assert!(matches!(result, Err(EvalError::OutputOverlapsTask { .. })));
        assert!(!output.exists());
    }

    #[cfg(unix)]
    #[test]
    fn detects_an_output_directory_reached_through_a_filesystem_alias() {
        let tasks = tempdir().unwrap();
        let task = write_named_task(tasks.path(), "alias", "terminal-bench/alias");
        let aliases = tempdir().unwrap();
        let alias = aliases.path().join("environment");
        std::os::unix::fs::symlink(task.environment_directory(), &alias).unwrap();

        assert!(output_aliases_task_package(&alias.join("retained/evals"), task.root()).unwrap());
    }

    fn write_named_task(parent: &std::path::Path, directory: &str, name: &str) -> Task {
        let root = parent.join(directory);
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("environment")).unwrap();
        fs::create_dir(root.join("tests")).unwrap();
        fs::write(root.join("instruction.md"), "Do the work.\n").unwrap();
        fs::write(root.join("tests/test.sh"), "exit 0\n").unwrap();
        fs::write(
            root.join("task.toml"),
            format!(
                r#"
schema_version = "1.1"
[task]
name = "{name}"
description = "attempt identity fixture"
[agent]
timeout_sec = 1.0
[verifier]
timeout_sec = 1.0
[environment]
docker_image = "alpine:3.21"
cpus = 1
memory_mb = 128
storage_mb = 128
gpus = 0
allow_internet = false
"#
            ),
        )
        .unwrap();
        Task::load(root).unwrap()
    }
}
