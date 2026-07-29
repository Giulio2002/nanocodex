//! Typed, durable evaluation for Nanocodex agents.
//!
//! This crate owns task loading, fresh attempt lifecycles, bounded admission,
//! resumable jobs, Harbor projection, typed events and results, and
//! task × agent × trial sweeps. Install an attempt backend with
//! [`EvaluatorBuilder::attempt_agent`] when a task should run somewhere other
//! than a native disposable workspace.
//!
//! # Run a sweep
//!
//! ```no_run
//! use nanocodex_agent::{Nanocodex, OpenAi, Thinking};
//! use nanocodex_eval::{Evaluator, Sweep, Task};
//!
//! # async fn evaluate() -> Result<(), Box<dyn std::error::Error>> {
//! let task = Task::load("tasks/write-greeting")?;
//! let agent = Nanocodex::builder(OpenAi::new(std::env::var("OPENAI_API_KEY")?)?)
//!     .instructions(
//!         "Work directly in the provided workspace. Complete the requested \
//!          task, verify your changes, and keep the final answer concise.",
//!     )
//!     .thinking(Thinking::Medium);
//! let sweep = Sweep::builder()
//!     .task(task)
//!     .agent("gpt-5.6-sol-medium", agent.clone())?
//!     .trials(5)
//!     .build()?;
//!
//! let (evaluator, events) = Evaluator::builder(agent)
//!     .output_directory(".nanocodex/evals")
//!     .max_concurrency(4)
//!     .max_memory_mb(16_384)
//!     .resume_incomplete(&sweep)
//!     .build()?;
//! let mut stream = events.subscribe();
//! let event_task = tokio::spawn(async move {
//!     while let Some(event) = stream.recv().await? {
//!         println!("{} {}", event.sequence, event.task_name);
//!     }
//!     Ok::<_, nanocodex_eval::EvalEventStreamError>(())
//! });
//!
//! let results = evaluator.sweep(sweep).await?;
//! println!("{} attempts, {} skipped", results.attempts().len(), results.skipped());
//! event_task.await??;
//! # Ok(())
//! # }
//! ```
//!
//! Every accepted attempt receives a fresh session and workspace. Results are
//! independently awaitable from the optional event stream.

#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

mod aggregate;
mod atif;
mod capture_proxy;
mod codex;
mod digest;
mod durable;
mod evaluator;
mod event;
pub mod harbor;
mod job;
mod native;
mod result;
mod sweep;
mod task;

pub use aggregate::{
    AggregateDataset, AggregateRunIdentity, AggregateRunTiming, AttemptBuildIdentity,
    AttemptConfigurationIdentity, AttemptFact, AttemptFactArtifacts, AttemptRuntimeMetrics,
    AttemptTaskIdentity, AttemptUsage, AttemptVerifierFact, AttemptVerifierIdentity,
    AttemptVmIdentity, ConfigurationAggregate, CostMetricSummaries, LatencyBreakdown,
    MetricSummary, RateEstimate, TaskAggregate, TokenMetricSummaries,
};
pub use atif::{
    AtifAgent, AtifAgentExtra, AtifBuilder, AtifFinalMetrics, AtifFinalMetricsExtra, AtifMetrics,
    AtifModelCallMetrics, AtifObservation, AtifObservationExtra, AtifObservationResult,
    AtifRuntimeMetrics, AtifSchemaVersion, AtifSource, AtifStep, AtifStepExtra, AtifToolCall,
    AtifToolCallExtra, AtifTrajectory,
};
#[doc(hidden)]
pub use capture_proxy::{
    ResponsesCaptureProxy, ResponsesCaptureProxyConfig, ResponsesCaptureProxyError,
    ResponsesModelCatalogOverride,
};
pub use codex::{
    CodexCommandOutput, CodexCommandRunner, CodexCommandRunnerError, CodexCommandStatus, CodexExec,
    CodexExecError, project_codex_atif,
};
pub use evaluator::{
    AttemptAgent, AttemptVerification, AttemptVerificationFailure, AttemptVerifier, EvalAttempt,
    EvalError, Evaluator, EvaluatorBuilder,
};
pub use event::{EvalEvent, EvalEventKind, EvalEventStream, EvalEventStreamError, EvalEvents};
#[allow(deprecated)]
pub use result::{
    AgentMetadata, AgentResult, AgentStatus, BillingCompleteness, CleanupDiagnostic, CleanupPhase,
    CleanupStatus, EvalArtifacts, EvalAttemptOutcome, EvalCleanup, EvalEnvironment, EvalException,
    EvalExceptionKind, EvalFailure, EvalFailureKind, EvalFailureTiming, EvalOutcome, EvalResult,
    EvalStatus, EvalTiming, MeasurementCompleteness, PhaseTiming, SweepAttemptResult, SweepResults,
    UsageTotals, VerifierResult, infer_retained_scored,
};
pub use sweep::{AgentId, AgentIdError, Sweep, SweepBuilder, SweepError};
pub use task::{
    NetworkPolicy, OciImage, Resources, Task, TaskLoadError, Verifier, VerifierCollect,
    VerifierEnvironmentMode,
};
