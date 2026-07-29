//! Stable plot-ready facts derived from retained evaluation attempts.

use std::{cmp::Ordering, collections::BTreeMap, path::PathBuf};

use chrono::{DateTime, Utc};
use nanocodex_oai_api::pricing::EstimatedUsdCost;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    BillingCompleteness, EvalAttemptOutcome, EvalEnvironment, EvalExceptionKind, EvalFailure,
    EvalOutcome, EvalResult, MeasurementCompleteness, SweepAttemptResult, Task, UsageTotals,
    VerifierResult, digest::PACKAGE_DIGEST_SCHEMA,
};

/// One self-contained attempt row used by aggregate and plotting consumers.
#[derive(Clone, Debug, Serialize)]
pub struct AttemptFact {
    /// Stable attempt identity used to navigate to retained evidence.
    pub attempt_id: Uuid,
    /// Dataset, task package, image, verifier, and scoring-policy identity.
    pub task: AttemptTaskIdentity,
    /// Structured model, harness, environment, and sweep configuration.
    pub configuration: AttemptConfigurationIdentity,
    /// Exact executable build identity when the application attached it.
    pub build: Option<AttemptBuildIdentity>,
    /// One-based repetition number.
    pub repetition: u16,
    /// Semantic attempt outcome.
    pub outcome: EvalOutcome,
    /// Whether this row contributes to score denominators.
    pub scored: bool,
    /// Verifier-derived success state.
    pub passed: bool,
    /// Whether the attempt retained a primary lifecycle exception.
    pub errored: bool,
    /// Whether the lifecycle exception was a safety refusal.
    ///
    /// Refusals also set [`Self::errored`], matching Harbor's overlapping
    /// lifecycle axes.
    pub refused: bool,
    /// Exact lifecycle exception class, independent from the verifier score.
    pub exception_kind: Option<EvalExceptionKind>,
    /// Whether score retention was accompanied by a cleanup failure.
    pub cleanup_failed: bool,
    /// Raw verifier exit status and reward dimensions.
    pub verifier: AttemptVerifierFact,
    /// Task, warmup, and combined token composition when the agent retained a
    /// provider billing snapshot. `None` is distinct from reported zero usage.
    pub usage: Option<AttemptUsage>,
    /// Runtime counters and durations when agent execution retained a snapshot.
    pub runtime: Option<AttemptRuntimeMetrics>,
    /// Observed USD cost, including warmup when reported. This is a lower
    /// bound when [`Self::billing_completeness`] is unknown.
    pub cost_usd: Option<f64>,
    /// Exact input/cache/output estimate composition, when usage was priced.
    pub estimated_cost: Option<EstimatedUsdCost>,
    /// Pricing catalog revision used for [`Self::estimated_cost`].
    pub pricing_revision: Option<String>,
    /// Whether provider billing is known to be terminal.
    pub billing_completeness: Option<BillingCompleteness>,
    /// Whether agent execution began but retained no billing snapshot.
    pub billing_snapshot_missing: bool,
    /// Exact phase measurements available for this attempt.
    pub latency: LatencyBreakdown,
    /// Retained attempt and trajectory locations.
    pub artifacts: AttemptFactArtifacts,
}

/// Dataset and immutable task package identity for one attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AttemptTaskIdentity {
    /// Dataset or suite name derived from the qualified task name, when present.
    pub dataset: Option<String>,
    /// Dataset release or source revision, when configured.
    pub dataset_revision: Option<String>,
    /// Stable qualified task name.
    pub name: String,
    /// Canonical source task root.
    pub root: PathBuf,
    /// Nanocodex package-digest schema.
    pub package_digest_schema: String,
    /// Immutable task package digest.
    pub package_digest: String,
    /// Harbor-compatible task checksum, when retained.
    pub harbor_checksum: Option<String>,
    /// Declared OCI image reference, when retained.
    pub image_reference: Option<String>,
    /// Verifier recipe and scoring-policy identity.
    pub verifier: AttemptVerifierIdentity,
}

/// Stable verifier recipe needed to reproduce score classification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AttemptVerifierIdentity {
    /// Task-relative verifier script path, when retained.
    pub script: Option<PathBuf>,
    /// Whether verification shares or separates the agent environment.
    pub environment_mode: Option<String>,
    /// Verifier timeout in nanoseconds, when retained.
    pub timeout_ns: Option<u64>,
    /// Stable reward-to-pass classification policy.
    pub scoring_policy: String,
}

/// Structured identity for one swept agent configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AttemptConfigurationIdentity {
    /// Caller-defined sweep configuration identity.
    pub id: String,
    /// Exact model slug.
    pub model: String,
    /// Published model tier such as standard, pro, or ultra, when configured.
    pub model_tier: Option<String>,
    /// Reasoning effort.
    pub reasoning_effort: String,
    /// Responses reasoning mode, when retained.
    pub reasoning_mode: Option<String>,
    /// Provider service tier used for pricing and execution, when known.
    pub service_tier: Option<String>,
    /// Responses transport spelling, when agent execution began.
    pub transport: Option<String>,
    /// Agent orchestration spelling, when agent execution began.
    pub orchestration: Option<String>,
    /// Application-owned tool configuration identity.
    pub tool_profile: Option<String>,
    /// Reproducibility seed, when configured.
    pub seed: Option<u64>,
    /// Agent-count and topology identity.
    pub agent_topology: String,
    /// Host or microVM attempt environment.
    pub environment: EvalEnvironment,
    /// Exact VM and guest-runtime identity, when attached by the application.
    pub vm: Option<AttemptVmIdentity>,
}

/// Executable build identity attached by an evaluation application.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AttemptBuildIdentity {
    /// Nanocodex package version.
    pub version: String,
    /// Source revision, when retained.
    pub git_sha: Option<String>,
    /// Reproducible build timestamp, when retained.
    pub built_at: Option<String>,
    /// Executable content digest, when retained.
    pub executable_sha256: Option<String>,
}

/// VM image and guest-runtime identity for one configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AttemptVmIdentity {
    /// Requested or prepared root filesystem path, when retained.
    pub rootfs: Option<PathBuf>,
    /// Guest runtime target triple, when retained.
    pub guest_runtime_target: Option<String>,
    /// Guest runtime executable digest, when retained.
    pub guest_runtime_sha256: Option<String>,
    /// Prepared guest runtime disk digest, when retained.
    pub runtime_disk_digest: Option<String>,
}

/// Run identity supplied by an embedding application after durable execution.
///
/// Applying this identity copies the compact typed provenance into every
/// attempt row, so a row remains sufficient for plot regeneration when
/// exported independently.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AggregateRunIdentity {
    /// Exact executable build.
    pub build: AttemptBuildIdentity,
    /// Dataset release or source revision shared by the run, when configured.
    pub dataset_revision: Option<String>,
    /// Model selected by the application.
    pub model: String,
    /// Published model tier, when applicable.
    pub model_tier: Option<String>,
    /// Reasoning effort selected by the application.
    pub reasoning_effort: String,
    /// Provider service tier.
    pub service_tier: Option<String>,
    /// Pricing catalog revision.
    pub pricing_revision: String,
    /// Application-owned tool configuration identity.
    pub tool_profile: String,
    /// Reproducibility seed, when configured.
    pub seed: Option<u64>,
    /// Agent-count and topology identity.
    pub agent_topology: String,
    /// Exact VM identity, when the run used a microVM.
    pub vm: Option<AttemptVmIdentity>,
}

/// Raw verifier evidence retained on one attempt row.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AttemptVerifierFact {
    /// Verifier process exit code, when retained.
    pub exit_code: Option<i32>,
    /// Named raw reward dimensions.
    pub rewards: BTreeMap<String, f64>,
}

/// Full provider token composition for one attempt.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AttemptUsage {
    /// Whether these token counts are terminal or observed lower bounds.
    pub completeness: MeasurementCompleteness,
    /// Task-execution usage excluding connection warmup.
    pub task_execution: UsageTotals,
    /// Prompt-cache warmup usage.
    pub warmup: UsageTotals,
    /// Task execution plus warmup.
    pub combined: UsageTotals,
}

/// Runtime counters and durations retained for one attempt.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct AttemptRuntimeMetrics {
    /// Whether these values are terminal or observed lower bounds.
    pub completeness: MeasurementCompleteness,
    /// Logical model calls observed.
    pub model_calls: u32,
    /// Steering messages observed.
    pub steers: u32,
    /// Context compactions observed.
    pub compactions: u32,
    /// Tool calls observed.
    pub tool_calls: u32,
    /// Responses connection attempts observed.
    pub connection_attempts: u32,
    /// Successful WebSocket replacements observed.
    pub websocket_reconnects: u32,
    /// Physical Responses attempts observed.
    pub response_attempts: u32,
    /// Responses retries observed.
    pub response_retries: u32,
    /// Potentially billable attempts without provider usage.
    pub billing_uncertain_response_attempts: u32,
    /// Observed time spent connecting.
    pub connection_duration_ns: u64,
    /// Observed completed retry-backoff time.
    pub retry_backoff_duration_ns: u64,
    /// Observed model-wait time.
    pub model_duration_ns: u64,
    /// Observed warmup time.
    pub warmup_duration_ns: u64,
    /// Observed tool execution time.
    pub tool_work_duration_ns: u64,
    /// Observed tool wall time.
    pub tool_wall_duration_ns: u64,
}

/// Plot-relevant latency phases in nanoseconds.
#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencyBreakdown {
    /// Time waiting for scheduler admission.
    pub queue_wait_ns: u64,
    /// Cold image resolution and construction attributed to this attempt.
    pub cold_image_ns: u64,
    /// Disposable environment materialization.
    pub environment_setup_ns: u64,
    /// Attempt backend construction, boot, and readiness.
    pub environment_readiness_ns: u64,
    /// VM materialization, boot, and readiness.
    pub vm_bootstrap_ns: u64,
    /// Agent setup.
    pub agent_setup_ns: u64,
    /// Complete warm agent execution.
    pub agent_execution_ns: u64,
    /// Sum of model waits, when agent runtime metrics were retained.
    pub model_ns: Option<u64>,
    /// Sum of actual tool work, when agent runtime metrics were retained.
    pub tool_work_ns: Option<u64>,
    /// Tool wall time including overlap, when agent runtime metrics were
    /// retained.
    pub tool_wall_ns: Option<u64>,
    /// Verifier execution.
    pub verifier_ns: u64,
    /// Explicit agent and verifier cleanup.
    pub cleanup_ns: u64,
    /// Sum of disjoint measured phases.
    pub total_ns: u64,
}

/// Paths that connect one plot point back to exact evidence.
#[derive(Clone, Debug, Serialize)]
pub struct AttemptFactArtifacts {
    /// Retained attempt directory.
    pub directory: PathBuf,
    /// Canonical terminal result.
    pub result: PathBuf,
    /// Exact agent input JSONL.
    pub input: PathBuf,
    /// Exact typed agent event JSONL.
    pub events: PathBuf,
    /// Canonical ATIF trajectory.
    pub trajectory: PathBuf,
    /// Verifier output.
    pub verifier_output: PathBuf,
    /// Isolated task workspace.
    pub workspace: PathBuf,
    /// Immutable trial lock and task identity.
    pub lock: PathBuf,
}

/// Stable aggregate export consumed by plot renderers and notebooks.
#[derive(Clone, Debug, Serialize)]
pub struct AggregateDataset {
    /// Schema version for the stable JSON export.
    pub schema_version: u32,
    /// Complete source rows; aggregates never replace drilldown evidence.
    pub attempts: Vec<AttemptFact>,
    /// Run-level cold preparation that is shared rather than attributed to an
    /// arbitrary attempt.
    pub run_timing: Option<AggregateRunTiming>,
    /// One point per configuration.
    pub configurations: Vec<ConfigurationAggregate>,
}

/// Run-level latency that cannot be assigned honestly to one attempt.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct AggregateRunTiming {
    /// Image resolution, rootfs preparation, and shared verifier cache setup.
    pub cold_image_and_cache_ns: u64,
}

/// Plot-ready summary for one configuration.
#[derive(Clone, Debug, Serialize)]
pub struct ConfigurationAggregate {
    /// Structured configuration identity.
    pub configuration: AttemptConfigurationIdentity,
    /// Attempt IDs contributing to this point.
    pub attempt_ids: Vec<Uuid>,
    /// Pass-rate estimate and Wilson interval.
    ///
    /// Every terminal attempt contributes; missing verifier evidence is a
    /// failure. This is the canonical benchmark pass@1 axis.
    pub success: RateEstimate,
    /// Verifier-conditioned pass rate over scored attempts only.
    pub verifier_conditioned_success: RateEstimate,
    /// Terminal-complete estimated cost distribution. Use this field for
    /// comparisons that require exact per-attempt billing snapshots.
    pub cost_usd: MetricSummary,
    /// Every observed cost, including lower bounds from attempts whose billing
    /// completeness is unknown.
    pub observed_cost_lower_bound_usd: MetricSummary,
    /// Terminal-complete cost composition by token class.
    pub cost_components_usd: CostMetricSummaries,
    /// Observed cost composition, including incomplete lower bounds.
    pub observed_cost_components_lower_bound_usd: CostMetricSummaries,
    /// Terminal-complete combined token composition.
    pub tokens: TokenMetricSummaries,
    /// Every observed token composition, including incomplete lower bounds.
    pub observed_tokens_lower_bound: TokenMetricSummaries,
    /// Raw verifier reward distributions by named dimension.
    pub rewards: BTreeMap<String, MetricSummary>,
    /// Lifecycle exception counts by exact class.
    pub exceptions: BTreeMap<EvalExceptionKind, usize>,
    /// Total latency distribution in seconds.
    pub latency_seconds: MetricSummary,
    /// Unbiased pass-at-k estimates supported by every task.
    pub pass_at_k: BTreeMap<u16, f64>,
    /// Attempts excluded from score denominators.
    pub unscored_attempts: usize,
    /// Attempts with a primary lifecycle exception, including scored attempts.
    pub errored_attempts: usize,
    /// Attempts refused by the provider's safety policy. These are also errors.
    pub refused_attempts: usize,
    /// Attempts with an explicit cleanup failure.
    pub cleanup_failures: usize,
    /// Attempts whose potentially billable provider usage is not terminal.
    pub billing_unknown_attempts: usize,
    /// Attempts without any terminal or partial agent billing snapshot.
    pub billing_missing_attempts: usize,
    /// Per-task distributions for deeper drilldown.
    pub tasks: Vec<TaskAggregate>,
}

/// Per-task contribution to a configuration point.
#[derive(Clone, Debug, Serialize)]
pub struct TaskAggregate {
    /// Dataset and immutable task package identity.
    pub task: AttemptTaskIdentity,
    /// Attempt IDs contributing to this task distribution.
    pub attempt_ids: Vec<Uuid>,
    /// Pass-rate estimate and Wilson interval.
    ///
    /// Every terminal attempt contributes; missing verifier evidence is a
    /// failure.
    pub success: RateEstimate,
    /// Verifier-conditioned pass rate over scored attempts only.
    pub verifier_conditioned_success: RateEstimate,
    /// Attempts excluded from score denominators.
    pub unscored_attempts: usize,
    /// Attempts with a primary lifecycle exception, including scored attempts.
    pub errored_attempts: usize,
    /// Attempts refused by the provider's safety policy. These are also errors.
    pub refused_attempts: usize,
    /// Attempts with an explicit cleanup failure.
    pub cleanup_failures: usize,
    /// Attempts whose potentially billable provider usage is not terminal.
    pub billing_unknown_attempts: usize,
    /// Attempts without any terminal or partial agent billing snapshot.
    pub billing_missing_attempts: usize,
    /// Terminal-complete estimated cost distribution. Use this field for
    /// comparisons that require exact per-attempt billing snapshots.
    pub cost_usd: MetricSummary,
    /// Every observed cost, including lower bounds from attempts whose billing
    /// completeness is unknown.
    pub observed_cost_lower_bound_usd: MetricSummary,
    /// Terminal-complete cost composition by token class.
    pub cost_components_usd: CostMetricSummaries,
    /// Observed cost composition, including incomplete lower bounds.
    pub observed_cost_components_lower_bound_usd: CostMetricSummaries,
    /// Terminal-complete combined token composition.
    pub tokens: TokenMetricSummaries,
    /// Every observed token composition, including incomplete lower bounds.
    pub observed_tokens_lower_bound: TokenMetricSummaries,
    /// Raw verifier reward distributions by named dimension.
    pub rewards: BTreeMap<String, MetricSummary>,
    /// Lifecycle exception counts by exact class.
    pub exceptions: BTreeMap<EvalExceptionKind, usize>,
    /// Total latency distribution in seconds.
    pub latency_seconds: MetricSummary,
}

/// Binomial success estimate with an approximate 95% Wilson interval.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct RateEstimate {
    /// Successful attempts.
    pub successes: usize,
    /// Total attempts.
    pub samples: usize,
    /// Observed success fraction.
    pub rate: Option<f64>,
    /// Lower Wilson bound.
    pub confidence_low: Option<f64>,
    /// Upper Wilson bound.
    pub confidence_high: Option<f64>,
}

/// Distribution summaries for provider token composition.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct TokenMetricSummaries {
    /// All input tokens.
    pub input_tokens: MetricSummary,
    /// Input tokens served from cache.
    pub cached_input_tokens: MetricSummary,
    /// Input tokens written to cache.
    pub cache_write_input_tokens: MetricSummary,
    /// All output tokens.
    pub output_tokens: MetricSummary,
    /// Reasoning subset of output tokens.
    pub reasoning_output_tokens: MetricSummary,
    /// Provider-reported total tokens.
    pub total_tokens: MetricSummary,
}

/// Distribution summaries for estimated cost composition.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct CostMetricSummaries {
    /// Ordinary-input cost.
    pub input_usd: MetricSummary,
    /// Cache-read input cost.
    pub cached_input_usd: MetricSummary,
    /// Cache-write input cost.
    pub cache_write_input_usd: MetricSummary,
    /// Output cost, including reasoning output.
    pub output_usd: MetricSummary,
    /// Aggregate cost.
    pub total_usd: MetricSummary,
}

/// Exact sample count plus common distribution summaries.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct MetricSummary {
    /// Number of known values.
    pub samples: usize,
    /// Minimum value.
    pub minimum: Option<f64>,
    /// Median value.
    pub median: Option<f64>,
    /// Arithmetic mean.
    pub mean: Option<f64>,
    /// Maximum value.
    pub maximum: Option<f64>,
}

impl AttemptFact {
    /// Builds a plot fact from one successful or verifier-failed sweep result.
    #[must_use]
    pub fn from_sweep_attempt(attempt: &SweepAttemptResult) -> Self {
        Self::from_outcome(attempt.agent().as_str(), attempt.trial(), attempt.outcome())
    }

    /// Builds a plot fact from a complete typed terminal attempt output.
    #[must_use]
    pub fn from_outcome(
        configuration: &str,
        repetition: u16,
        outcome: &EvalAttemptOutcome,
    ) -> Self {
        match outcome {
            EvalAttemptOutcome::Scored(result) => {
                Self::from_result(configuration, repetition, result)
            }
            EvalAttemptOutcome::Unscored(failure) => {
                Self::from_failure(configuration, repetition, failure)
            }
        }
    }

    /// Builds a plot fact from one typed result and explicit coordinates.
    #[must_use]
    pub fn from_result(configuration: &str, repetition: u16, result: &EvalResult) -> Self {
        let duration = |started: DateTime<Utc>, finished: DateTime<Utc>| {
            u64::try_from(
                finished
                    .signed_duration_since(started)
                    .num_nanoseconds()
                    .unwrap_or_default()
                    .max(0),
            )
            .unwrap_or(u64::MAX)
        };
        let agent = result.agent.as_ref();
        let task = AttemptTaskIdentity::from_task(result.task());
        let configuration = AttemptConfigurationIdentity::from_agent(
            configuration,
            result.environment,
            nanocodex_oai_api::MODEL,
            "",
            agent,
        );
        let exception_kind = result.exception.as_ref().map(|exception| exception.kind);
        let usage = AttemptUsage::from_agent(agent);
        Self {
            attempt_id: result.attempt_id,
            task,
            configuration,
            build: None,
            repetition,
            outcome: result.outcome,
            scored: true,
            passed: result.status == crate::EvalStatus::Passed,
            errored: result.exception.is_some(),
            refused: exception_kind == Some(EvalExceptionKind::AgentSafetyRefusal),
            exception_kind,
            cleanup_failed: result.cleanup.is_failed(),
            verifier: AttemptVerifierFact::from_result(&result.verifier),
            billing_snapshot_missing: usage.is_none(),
            usage,
            runtime: AttemptRuntimeMetrics::from_agent(agent),
            cost_usd: agent.and_then(|agent| agent.cost_usd),
            estimated_cost: agent.and_then(|agent| agent.metadata.estimated_cost.clone()),
            pricing_revision: agent.and_then(|agent| agent.metadata.pricing_revision.clone()),
            billing_completeness: agent.map(|agent| agent.billing_completeness),
            latency: LatencyBreakdown {
                queue_wait_ns: duration(
                    result.timing.queue_wait.started_at,
                    result.timing.queue_wait.finished_at,
                ),
                environment_setup_ns: duration(
                    result.timing.environment_setup.started_at,
                    result.timing.environment_setup.finished_at,
                ),
                environment_readiness_ns: duration(
                    result.timing.environment_readiness.started_at,
                    result.timing.environment_readiness.finished_at,
                ),
                vm_bootstrap_ns: if result.environment == crate::EvalEnvironment::MicroVm {
                    duration(
                        result.timing.environment_readiness.started_at,
                        result.timing.environment_readiness.finished_at,
                    )
                } else {
                    0
                },
                agent_setup_ns: duration(
                    result.timing.agent_setup.started_at,
                    result.timing.agent_setup.finished_at,
                ),
                agent_execution_ns: duration(
                    result.timing.agent_execution.started_at,
                    result.timing.agent_execution.finished_at,
                ),
                model_ns: agent.map(|agent| agent.metadata.model_duration_ns),
                tool_work_ns: agent.map(|agent| agent.metadata.tool_work_duration_ns),
                tool_wall_ns: agent.map(|agent| agent.metadata.tool_wall_duration_ns),
                verifier_ns: duration(
                    result.timing.verifier.started_at,
                    result.timing.verifier.finished_at,
                ),
                cleanup_ns: [&result.cleanup.agent, &result.cleanup.verifier]
                    .into_iter()
                    .filter_map(|cleanup| cleanup.timing.as_ref())
                    .map(|timing| duration(timing.started_at, timing.finished_at))
                    .sum(),
                ..LatencyBreakdown::default()
            },
            artifacts: AttemptFactArtifacts::new(
                &result.artifacts.directory,
                &result.artifacts.workspace,
                &result.artifacts.verifier_output,
            ),
        }
        .with_total()
    }

    /// Builds a plot fact from one typed unscored terminal failure.
    #[must_use]
    pub fn from_failure(configuration: &str, repetition: u16, failure: &EvalFailure) -> Self {
        let duration = |timing: Option<&crate::PhaseTiming>| {
            timing.map_or(0, |timing| {
                u64::try_from(
                    timing
                        .finished_at
                        .signed_duration_since(timing.started_at)
                        .num_nanoseconds()
                        .unwrap_or_default()
                        .max(0),
                )
                .unwrap_or(u64::MAX)
            })
        };
        let agent = failure.agent.as_ref();
        let task = AttemptTaskIdentity::from_task(failure.task());
        let configuration = AttemptConfigurationIdentity::from_agent(
            configuration,
            failure.environment,
            &failure.model,
            &failure.effort,
            agent,
        );
        let usage = AttemptUsage::from_agent(agent);
        Self {
            attempt_id: failure.attempt_id,
            task,
            configuration,
            build: None,
            repetition,
            outcome: failure.exception.outcome,
            scored: false,
            passed: false,
            errored: true,
            refused: failure.exception.kind == EvalExceptionKind::AgentSafetyRefusal,
            exception_kind: Some(failure.exception.kind),
            cleanup_failed: failure.cleanup.is_failed(),
            verifier: failure.verifier.as_ref().map_or_else(
                AttemptVerifierFact::default,
                AttemptVerifierFact::from_result,
            ),
            billing_snapshot_missing: usage.is_none() && failure.timing.agent_execution.is_some(),
            usage,
            runtime: AttemptRuntimeMetrics::from_agent(agent),
            cost_usd: agent.and_then(|agent| agent.cost_usd),
            estimated_cost: agent.and_then(|agent| agent.metadata.estimated_cost.clone()),
            pricing_revision: agent.and_then(|agent| agent.metadata.pricing_revision.clone()),
            billing_completeness: agent.map(|agent| agent.billing_completeness),
            latency: LatencyBreakdown {
                queue_wait_ns: duration(Some(&failure.timing.queue_wait)),
                environment_setup_ns: duration(failure.timing.environment_setup.as_ref()),
                environment_readiness_ns: duration(failure.timing.environment_readiness.as_ref()),
                vm_bootstrap_ns: if failure.environment == crate::EvalEnvironment::MicroVm {
                    duration(failure.timing.environment_readiness.as_ref())
                } else {
                    0
                },
                agent_setup_ns: duration(failure.timing.agent_setup.as_ref()),
                agent_execution_ns: duration(failure.timing.agent_execution.as_ref()),
                model_ns: agent.map(|agent| agent.metadata.model_duration_ns),
                tool_work_ns: agent.map(|agent| agent.metadata.tool_work_duration_ns),
                tool_wall_ns: agent.map(|agent| agent.metadata.tool_wall_duration_ns),
                verifier_ns: duration(failure.timing.verifier.as_ref()),
                cleanup_ns: [&failure.cleanup.agent, &failure.cleanup.verifier]
                    .into_iter()
                    .filter_map(|cleanup| cleanup.timing.as_ref())
                    .map(|timing| duration(Some(timing)))
                    .sum(),
                ..LatencyBreakdown::default()
            },
            artifacts: AttemptFactArtifacts::new(
                &failure.artifacts.directory,
                &failure.artifacts.workspace,
                &failure.artifacts.verifier_output,
            ),
        }
        .with_total()
    }

    const fn with_total(mut self) -> Self {
        let latency = &self.latency;
        self.latency.total_ns = latency
            .queue_wait_ns
            .saturating_add(latency.cold_image_ns)
            .saturating_add(latency.environment_setup_ns)
            .saturating_add(latency.environment_readiness_ns)
            .saturating_add(latency.agent_setup_ns)
            .saturating_add(latency.agent_execution_ns)
            .saturating_add(latency.verifier_ns)
            .saturating_add(latency.cleanup_ns);
        self
    }
}

impl AttemptTaskIdentity {
    fn from_task(task: &Task) -> Self {
        Self {
            dataset: dataset_name(task.name()),
            dataset_revision: None,
            name: task.name().to_owned(),
            root: task.root().to_path_buf(),
            package_digest_schema: PACKAGE_DIGEST_SCHEMA.to_owned(),
            package_digest: format!("sha256:{}", task.content_digest()),
            harbor_checksum: None,
            image_reference: Some(task.image().reference().to_owned()),
            verifier: AttemptVerifierIdentity {
                script: Some(
                    task.verifier()
                        .script()
                        .strip_prefix(task.root())
                        .unwrap_or_else(|_| task.verifier().script())
                        .to_path_buf(),
                ),
                environment_mode: Some(task.verifier().environment_mode().as_str().to_owned()),
                timeout_ns: Some(duration_ns_saturating(task.verifier().timeout())),
                scoring_policy: "all_rewards_positive-v1".to_owned(),
            },
        }
    }
}

impl AttemptConfigurationIdentity {
    fn from_agent(
        id: &str,
        environment: EvalEnvironment,
        fallback_model: &str,
        fallback_effort: &str,
        agent: Option<&crate::AgentResult>,
    ) -> Self {
        let metadata = agent.map(|agent| &agent.metadata);
        Self {
            id: id.to_owned(),
            model: agent.map_or_else(|| fallback_model.to_owned(), |agent| agent.model.clone()),
            model_tier: None,
            reasoning_effort: agent
                .map_or_else(|| fallback_effort.to_owned(), |agent| agent.effort.clone()),
            reasoning_mode: metadata.and_then(|metadata| metadata.reasoning_mode.clone()),
            service_tier: metadata
                .and_then(|metadata| metadata.estimated_cost.as_ref())
                .map(|cost| cost.service_tier().as_str().to_owned()),
            transport: metadata.and_then(|metadata| nonempty(&metadata.transport)),
            orchestration: metadata.and_then(|metadata| nonempty(&metadata.orchestration)),
            tool_profile: None,
            seed: None,
            agent_topology: "single_agent".to_owned(),
            environment,
            vm: None,
        }
    }
}

impl AttemptVerifierFact {
    fn from_result(result: &VerifierResult) -> Self {
        Self {
            exit_code: Some(result.exit_code),
            rewards: result.rewards.clone(),
        }
    }
}

impl AttemptUsage {
    fn from_agent(agent: Option<&crate::AgentResult>) -> Option<Self> {
        let agent = agent?;
        if !agent.has_observed_usage() {
            return None;
        }
        let task_execution = agent.usage.clone();
        let warmup = agent.metadata.warmup_usage.clone();
        let combined = combine_usage(&task_execution, &warmup);
        Some(Self {
            completeness: if agent.billing_completeness == BillingCompleteness::Complete {
                MeasurementCompleteness::Complete
            } else {
                MeasurementCompleteness::ObservedLowerBound
            },
            task_execution,
            warmup,
            combined,
        })
    }
}

impl AttemptRuntimeMetrics {
    fn from_agent(agent: Option<&crate::AgentResult>) -> Option<Self> {
        let metadata = &agent?.metadata;
        Some(Self {
            completeness: metadata.runtime_completeness,
            model_calls: metadata.model_calls,
            steers: metadata.steers,
            compactions: metadata.compactions,
            tool_calls: metadata.tool_calls,
            connection_attempts: metadata.connection_attempts,
            websocket_reconnects: metadata.websocket_reconnects,
            response_attempts: metadata.response_attempts,
            response_retries: metadata.response_retries,
            billing_uncertain_response_attempts: metadata.billing_uncertain_response_attempts,
            connection_duration_ns: metadata.connection_duration_ns,
            retry_backoff_duration_ns: metadata.retry_backoff_duration_ns,
            model_duration_ns: metadata.model_duration_ns,
            warmup_duration_ns: metadata.warmup_duration_ns,
            tool_work_duration_ns: metadata.tool_work_duration_ns,
            tool_wall_duration_ns: metadata.tool_wall_duration_ns,
        })
    }
}

impl AttemptFactArtifacts {
    fn new(
        directory: &std::path::Path,
        workspace: &std::path::Path,
        verifier: &std::path::Path,
    ) -> Self {
        Self {
            directory: directory.to_path_buf(),
            result: directory.join("result.json"),
            input: directory.join("agent/input.jsonl"),
            events: directory.join("agent/events.jsonl"),
            trajectory: directory.join("agent/trajectory.json"),
            verifier_output: verifier.to_path_buf(),
            workspace: workspace.to_path_buf(),
            lock: directory.join("lock.json"),
        }
    }
}

fn dataset_name(task_name: &str) -> Option<String> {
    task_name
        .split_once('/')
        .map(|(dataset, _)| dataset.to_owned())
}

fn nonempty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

const fn combine_usage(left: &UsageTotals, right: &UsageTotals) -> UsageTotals {
    UsageTotals {
        input_tokens: left.input_tokens.saturating_add(right.input_tokens),
        cached_input_tokens: left
            .cached_input_tokens
            .saturating_add(right.cached_input_tokens),
        cache_write_input_tokens: left
            .cache_write_input_tokens
            .saturating_add(right.cache_write_input_tokens),
        output_tokens: left.output_tokens.saturating_add(right.output_tokens),
        reasoning_output_tokens: left
            .reasoning_output_tokens
            .saturating_add(right.reasoning_output_tokens),
        total_tokens: left.total_tokens.saturating_add(right.total_tokens),
    }
}

fn duration_ns_saturating(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl AggregateDataset {
    /// Builds deterministic plot points while retaining every source row.
    #[must_use]
    pub fn new(attempts: Vec<AttemptFact>) -> Self {
        let mut groups = BTreeMap::<String, Vec<&AttemptFact>>::new();
        for attempt in &attempts {
            groups
                .entry(attempt.configuration.id.clone())
                .or_default()
                .push(attempt);
        }
        let configurations = groups
            .into_values()
            .map(|attempts| ConfigurationAggregate::new(&attempts))
            .collect();
        Self {
            schema_version: 4,
            attempts,
            run_timing: None,
            configurations,
        }
    }

    /// Attaches application-owned build and run provenance to every attempt.
    ///
    /// This is intentionally a row-level projection rather than a loose
    /// run-sidecar reference: exported or filtered rows remain self-contained.
    #[must_use]
    pub fn with_run_identity(mut self, identity: AggregateRunIdentity) -> Self {
        for attempt in &mut self.attempts {
            attempt.build = Some(identity.build.clone());
            attempt
                .task
                .dataset_revision
                .clone_from(&identity.dataset_revision);
            attempt.configuration.model.clone_from(&identity.model);
            attempt
                .configuration
                .model_tier
                .clone_from(&identity.model_tier);
            attempt
                .configuration
                .reasoning_effort
                .clone_from(&identity.reasoning_effort);
            attempt
                .configuration
                .service_tier
                .clone_from(&identity.service_tier);
            attempt.configuration.tool_profile = Some(identity.tool_profile.clone());
            attempt.configuration.seed = identity.seed;
            attempt
                .configuration
                .agent_topology
                .clone_from(&identity.agent_topology);
            attempt.configuration.vm.clone_from(&identity.vm);
            attempt.pricing_revision = Some(identity.pricing_revision.clone());
        }
        let run_timing = self.run_timing;
        let mut rebuilt = Self::new(self.attempts);
        rebuilt.run_timing = run_timing;
        rebuilt
    }

    /// Attaches run-level cold image and shared cache preparation latency.
    #[must_use]
    pub const fn with_run_timing(mut self, run_timing: AggregateRunTiming) -> Self {
        self.run_timing = Some(run_timing);
        self
    }

    /// Builds an aggregate directly from typed sweep results.
    #[must_use]
    pub fn from_sweep(attempts: &[SweepAttemptResult]) -> Self {
        Self::new(
            attempts
                .iter()
                .map(AttemptFact::from_sweep_attempt)
                .collect(),
        )
    }
}

impl ConfigurationAggregate {
    fn new(attempts: &[&AttemptFact]) -> Self {
        let mut tasks = BTreeMap::<String, Vec<&AttemptFact>>::new();
        for attempt in attempts {
            tasks
                .entry(attempt.task.name.clone())
                .or_default()
                .push(*attempt);
        }
        let pass_at_k = pass_at_k(&tasks);
        let configuration = attempts
            .first()
            .map_or_else(default_configuration, |attempt| {
                attempt.configuration.clone()
            });
        Self {
            configuration,
            attempt_ids: attempts.iter().map(|attempt| attempt.attempt_id).collect(),
            success: RateEstimate::all_terminal(attempts),
            verifier_conditioned_success: RateEstimate::scored_only(attempts),
            cost_usd: MetricSummary::new(attempts.iter().filter_map(|attempt| {
                (attempt.billing_completeness == Some(BillingCompleteness::Complete))
                    .then_some(attempt.cost_usd)
                    .flatten()
            })),
            observed_cost_lower_bound_usd: MetricSummary::new(
                attempts.iter().filter_map(|attempt| attempt.cost_usd),
            ),
            cost_components_usd: CostMetricSummaries::new(attempts, true),
            observed_cost_components_lower_bound_usd: CostMetricSummaries::new(attempts, false),
            tokens: TokenMetricSummaries::new(attempts, true),
            observed_tokens_lower_bound: TokenMetricSummaries::new(attempts, false),
            rewards: reward_summaries(attempts),
            exceptions: exception_counts(attempts),
            latency_seconds: MetricSummary::new(
                attempts
                    .iter()
                    .map(|attempt| attempt.latency.total_ns as f64 / 1_000_000_000.0),
            ),
            pass_at_k,
            unscored_attempts: attempts.iter().filter(|attempt| !attempt.scored).count(),
            errored_attempts: attempts.iter().filter(|attempt| attempt.errored).count(),
            refused_attempts: attempts.iter().filter(|attempt| attempt.refused).count(),
            cleanup_failures: attempts
                .iter()
                .filter(|attempt| attempt.cleanup_failed)
                .count(),
            billing_unknown_attempts: attempts
                .iter()
                .filter(|attempt| {
                    attempt.billing_completeness == Some(BillingCompleteness::Unknown)
                })
                .count(),
            billing_missing_attempts: attempts
                .iter()
                .filter(|attempt| attempt.billing_snapshot_missing)
                .count(),
            tasks: tasks
                .into_values()
                .map(|attempts| TaskAggregate {
                    task: attempts
                        .first()
                        .map_or_else(default_task, |attempt| attempt.task.clone()),
                    attempt_ids: attempts.iter().map(|attempt| attempt.attempt_id).collect(),
                    success: RateEstimate::all_terminal(&attempts),
                    verifier_conditioned_success: RateEstimate::scored_only(&attempts),
                    unscored_attempts: attempts.iter().filter(|attempt| !attempt.scored).count(),
                    errored_attempts: attempts.iter().filter(|attempt| attempt.errored).count(),
                    refused_attempts: attempts.iter().filter(|attempt| attempt.refused).count(),
                    cleanup_failures: attempts
                        .iter()
                        .filter(|attempt| attempt.cleanup_failed)
                        .count(),
                    billing_unknown_attempts: attempts
                        .iter()
                        .filter(|attempt| {
                            attempt.billing_completeness == Some(BillingCompleteness::Unknown)
                        })
                        .count(),
                    billing_missing_attempts: attempts
                        .iter()
                        .filter(|attempt| attempt.billing_snapshot_missing)
                        .count(),
                    cost_usd: MetricSummary::new(attempts.iter().filter_map(|attempt| {
                        (attempt.billing_completeness == Some(BillingCompleteness::Complete))
                            .then_some(attempt.cost_usd)
                            .flatten()
                    })),
                    observed_cost_lower_bound_usd: MetricSummary::new(
                        attempts.iter().filter_map(|attempt| attempt.cost_usd),
                    ),
                    cost_components_usd: CostMetricSummaries::new(&attempts, true),
                    observed_cost_components_lower_bound_usd: CostMetricSummaries::new(
                        &attempts, false,
                    ),
                    tokens: TokenMetricSummaries::new(&attempts, true),
                    observed_tokens_lower_bound: TokenMetricSummaries::new(&attempts, false),
                    rewards: reward_summaries(&attempts),
                    exceptions: exception_counts(&attempts),
                    latency_seconds: MetricSummary::new(
                        attempts
                            .iter()
                            .map(|attempt| attempt.latency.total_ns as f64 / 1_000_000_000.0),
                    ),
                })
                .collect(),
        }
    }
}

impl RateEstimate {
    fn all_terminal(attempts: &[&AttemptFact]) -> Self {
        Self::from_counts(
            attempts.iter().filter(|attempt| attempt.passed).count(),
            attempts.len(),
        )
    }

    fn scored_only(attempts: &[&AttemptFact]) -> Self {
        let samples = attempts.iter().filter(|attempt| attempt.scored).count();
        let successes = attempts
            .iter()
            .filter(|attempt| attempt.scored && attempt.passed)
            .count();
        Self::from_counts(successes, samples)
    }

    fn from_counts(successes: usize, samples: usize) -> Self {
        if samples == 0 {
            return Self {
                successes,
                samples,
                rate: None,
                confidence_low: None,
                confidence_high: None,
            };
        }
        let n = samples as f64;
        let rate = successes as f64 / n;
        let z = 1.959_963_984_540_054;
        let denominator = 1.0 + z * z / n;
        let center = (rate + z * z / (2.0 * n)) / denominator;
        let margin = z * ((rate * (1.0 - rate) + z * z / (4.0 * n)) / n).sqrt() / denominator;
        Self {
            successes,
            samples,
            rate: Some(rate),
            confidence_low: Some((center - margin).max(0.0)),
            confidence_high: Some((center + margin).min(1.0)),
        }
    }
}

impl TokenMetricSummaries {
    fn new(attempts: &[&AttemptFact], complete_only: bool) -> Self {
        let usage = || {
            attempts.iter().filter_map(|attempt| {
                let usage = attempt.usage.as_ref()?;
                (!complete_only || usage.completeness == MeasurementCompleteness::Complete)
                    .then_some(usage)
            })
        };
        Self {
            input_tokens: MetricSummary::new(
                usage().map(|usage| usage.combined.input_tokens as f64),
            ),
            cached_input_tokens: MetricSummary::new(
                usage().map(|usage| usage.combined.cached_input_tokens as f64),
            ),
            cache_write_input_tokens: MetricSummary::new(
                usage().map(|usage| usage.combined.cache_write_input_tokens as f64),
            ),
            output_tokens: MetricSummary::new(
                usage().map(|usage| usage.combined.output_tokens as f64),
            ),
            reasoning_output_tokens: MetricSummary::new(
                usage().map(|usage| usage.combined.reasoning_output_tokens as f64),
            ),
            total_tokens: MetricSummary::new(
                usage().map(|usage| usage.combined.total_tokens as f64),
            ),
        }
    }
}

impl CostMetricSummaries {
    fn new(attempts: &[&AttemptFact], complete_only: bool) -> Self {
        let costs = attempts
            .iter()
            .filter(|attempt| {
                !complete_only
                    || attempt.billing_completeness == Some(BillingCompleteness::Complete)
            })
            .filter_map(|attempt| attempt.estimated_cost.as_ref())
            .collect::<Vec<_>>();
        Self {
            input_usd: MetricSummary::new(costs.iter().map(|cost| cost.input().as_f64())),
            cached_input_usd: MetricSummary::new(
                costs.iter().map(|cost| cost.cached_input().as_f64()),
            ),
            cache_write_input_usd: MetricSummary::new(
                costs.iter().map(|cost| cost.cache_write_input().as_f64()),
            ),
            output_usd: MetricSummary::new(costs.iter().map(|cost| cost.output().as_f64())),
            total_usd: MetricSummary::new(costs.iter().map(|cost| cost.amount().as_f64())),
        }
    }
}

impl MetricSummary {
    fn new(values: impl IntoIterator<Item = f64>) -> Self {
        let mut values = values
            .into_iter()
            .filter(|value| value.is_finite())
            .collect::<Vec<_>>();
        values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
        let samples = values.len();
        if samples == 0 {
            return Self::default();
        }
        let median = if samples % 2 == 0 {
            (values[samples / 2 - 1] + values[samples / 2]) / 2.0
        } else {
            values[samples / 2]
        };
        Self {
            samples,
            minimum: values.first().copied(),
            median: Some(median),
            mean: Some(values.iter().sum::<f64>() / samples as f64),
            maximum: values.last().copied(),
        }
    }
}

fn reward_summaries(attempts: &[&AttemptFact]) -> BTreeMap<String, MetricSummary> {
    let mut rewards = BTreeMap::<String, Vec<f64>>::new();
    for attempt in attempts {
        for (name, reward) in &attempt.verifier.rewards {
            rewards.entry(name.clone()).or_default().push(*reward);
        }
    }
    rewards
        .into_iter()
        .map(|(name, values)| (name, MetricSummary::new(values)))
        .collect()
}

fn exception_counts(attempts: &[&AttemptFact]) -> BTreeMap<EvalExceptionKind, usize> {
    let mut exceptions = BTreeMap::new();
    for kind in attempts.iter().filter_map(|attempt| attempt.exception_kind) {
        *exceptions.entry(kind).or_default() += 1;
    }
    exceptions
}

fn default_configuration() -> AttemptConfigurationIdentity {
    AttemptConfigurationIdentity {
        id: String::new(),
        model: String::new(),
        model_tier: None,
        reasoning_effort: String::new(),
        reasoning_mode: None,
        service_tier: None,
        transport: None,
        orchestration: None,
        tool_profile: None,
        seed: None,
        agent_topology: "single_agent".to_owned(),
        environment: EvalEnvironment::Native,
        vm: None,
    }
}

fn default_task() -> AttemptTaskIdentity {
    AttemptTaskIdentity {
        dataset: None,
        dataset_revision: None,
        name: String::new(),
        root: PathBuf::new(),
        package_digest_schema: String::new(),
        package_digest: String::new(),
        harbor_checksum: None,
        image_reference: None,
        verifier: AttemptVerifierIdentity {
            script: None,
            environment_mode: None,
            timeout_ns: None,
            scoring_policy: "all_rewards_positive-v1".to_owned(),
        },
    }
}

fn pass_at_k(tasks: &BTreeMap<String, Vec<&AttemptFact>>) -> BTreeMap<u16, f64> {
    let Some(minimum) = tasks.values().map(Vec::len).min() else {
        return BTreeMap::new();
    };
    eligible_k_values(minimum)
        .into_iter()
        .map(|k| {
            let estimate = tasks
                .values()
                .map(|attempts| {
                    let n = attempts.len() as u64;
                    let correct = attempts.iter().filter(|attempt| attempt.passed).count() as u64;
                    if n - correct < u64::from(k) {
                        return 1.0;
                    }
                    1.0 - (0..u64::from(k)).fold(1.0, |product, index| {
                        product * (n - correct - index) as f64 / (n - index) as f64
                    })
                })
                .sum::<f64>()
                / tasks.len() as f64;
            (k, estimate)
        })
        .collect()
}

fn eligible_k_values(max_k: usize) -> Vec<u16> {
    let mut values = std::collections::BTreeSet::new();
    let mut power = 2_usize;
    while power <= max_k {
        if let Ok(k) = u16::try_from(power) {
            values.insert(k);
        }
        let Some(next) = power.checked_mul(2) else {
            break;
        };
        power = next;
    }
    let mut multiple = 5_usize;
    while multiple <= max_k {
        if let Ok(k) = u16::try_from(multiple) {
            values.insert(k);
        }
        let Some(next) = multiple.checked_add(5) else {
            break;
        };
        multiple = next;
    }
    values.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fact(configuration: &str, task: &str, repetition: u16, passed: bool) -> AttemptFact {
        AttemptFact {
            attempt_id: Uuid::now_v7(),
            task: AttemptTaskIdentity {
                dataset: Some("fixture".to_owned()),
                dataset_revision: Some("fixture-2026-07-28".to_owned()),
                name: task.to_owned(),
                root: PathBuf::from(task),
                package_digest_schema: "fixture-v1".to_owned(),
                package_digest: format!("sha256:{task}"),
                harbor_checksum: None,
                image_reference: Some("fixture:latest".to_owned()),
                verifier: AttemptVerifierIdentity {
                    script: Some(PathBuf::from("tests/test.sh")),
                    environment_mode: Some("same".to_owned()),
                    timeout_ns: Some(1_000_000_000),
                    scoring_policy: "all_rewards_positive-v1".to_owned(),
                },
            },
            configuration: AttemptConfigurationIdentity {
                id: configuration.to_owned(),
                model: "gpt-test".to_owned(),
                model_tier: None,
                reasoning_effort: "medium".to_owned(),
                reasoning_mode: Some("adaptive".to_owned()),
                service_tier: Some("standard".to_owned()),
                transport: Some("responses_websocket_v2".to_owned()),
                orchestration: Some("local_code_mode".to_owned()),
                tool_profile: Some("fixture-tools".to_owned()),
                seed: Some(7),
                agent_topology: "single_agent".to_owned(),
                environment: EvalEnvironment::Native,
                vm: None,
            },
            build: Some(AttemptBuildIdentity {
                version: "test".to_owned(),
                git_sha: Some("deadbeef".to_owned()),
                built_at: Some("2026-07-28T00:00:00Z".to_owned()),
                executable_sha256: Some("abc123".to_owned()),
            }),
            repetition,
            outcome: if passed {
                EvalOutcome::Passed
            } else {
                EvalOutcome::VerifierFailed
            },
            scored: true,
            passed,
            errored: false,
            refused: false,
            exception_kind: None,
            cleanup_failed: false,
            verifier: AttemptVerifierFact {
                exit_code: Some(i32::from(!passed)),
                rewards: BTreeMap::from([
                    ("reward".to_owned(), if passed { 1.0 } else { 0.0 }),
                    ("safety".to_owned(), 1.0),
                ]),
            },
            usage: Some(AttemptUsage {
                completeness: MeasurementCompleteness::Complete,
                task_execution: UsageTotals {
                    input_tokens: 10,
                    cached_input_tokens: 4,
                    cache_write_input_tokens: 2,
                    output_tokens: u64::from(repetition),
                    reasoning_output_tokens: 1,
                    total_tokens: 10 + u64::from(repetition),
                },
                warmup: UsageTotals::default(),
                combined: UsageTotals {
                    input_tokens: 10,
                    cached_input_tokens: 4,
                    cache_write_input_tokens: 2,
                    output_tokens: u64::from(repetition),
                    reasoning_output_tokens: 1,
                    total_tokens: 10 + u64::from(repetition),
                },
            }),
            runtime: None,
            cost_usd: Some(f64::from(repetition)),
            estimated_cost: None,
            pricing_revision: Some("fixture-pricing-v1".to_owned()),
            billing_completeness: Some(BillingCompleteness::Complete),
            billing_snapshot_missing: false,
            latency: LatencyBreakdown {
                total_ns: u64::from(repetition) * 1_000_000_000,
                ..LatencyBreakdown::default()
            },
            artifacts: AttemptFactArtifacts {
                directory: PathBuf::from("attempt"),
                result: PathBuf::from("attempt/result.json"),
                input: PathBuf::from("attempt/agent/input.jsonl"),
                events: PathBuf::from("attempt/agent/events.jsonl"),
                trajectory: PathBuf::from("attempt/agent/trajectory.json"),
                verifier_output: PathBuf::from("attempt/verifier/test-stdout.txt"),
                workspace: PathBuf::from("attempt/workspace"),
                lock: PathBuf::from("attempt/lock.json"),
            },
        }
    }

    #[test]
    fn builds_plot_points_with_exact_drilldown_and_pass_at_k() {
        let dataset = AggregateDataset::new(vec![
            fact("medium", "a", 1, true),
            fact("medium", "a", 2, false),
            fact("medium", "b", 1, false),
            fact("medium", "b", 2, false),
        ]);
        let point = &dataset.configurations[0];
        assert_eq!(point.attempt_ids.len(), 4);
        assert_eq!(point.success.successes, 1);
        assert_eq!(point.success.samples, 4);
        assert_eq!(point.cost_usd.median, Some(1.5));
        assert_eq!(point.latency_seconds.mean, Some(1.5));
        assert_eq!(point.tasks.len(), 2);
        assert_eq!(point.tokens.output_tokens.mean, Some(1.5));
        assert_eq!(point.rewards["reward"].mean, Some(0.25));
        assert_eq!(dataset.attempts[0].task.package_digest_schema, "fixture-v1");
        assert_eq!(
            dataset.attempts[0].configuration.tool_profile.as_deref(),
            Some("fixture-tools")
        );
        assert_eq!(
            dataset.attempts[0].pricing_revision.as_deref(),
            Some("fixture-pricing-v1")
        );
        let encoded = serde_json::to_value(&dataset).unwrap();
        assert_eq!(encoded["schema_version"], 4);
        assert_eq!(
            encoded["attempts"][0]["task"]["package_digest_schema"],
            "fixture-v1"
        );
        assert_eq!(encoded["attempts"][0]["configuration"]["model"], "gpt-test");
        assert_eq!(encoded["attempts"][0]["build"]["git_sha"], "deadbeef");
        assert_eq!(encoded["attempts"][0]["verifier"]["rewards"]["safety"], 1.0);
        assert_eq!(
            encoded["attempts"][0]["usage"]["combined"]["cache_write_input_tokens"],
            2
        );
        assert_eq!(
            encoded["attempts"][0]["artifacts"]["result"],
            "attempt/result.json"
        );
        assert!((point.pass_at_k[&2] - 0.5).abs() < f64::EPSILON);
        assert_eq!(point.pass_at_k.keys().copied().collect::<Vec<_>>(), [2]);
    }

    #[test]
    fn token_summaries_distinguish_missing_zero_and_partial_usage() {
        let known = fact("medium", "a", 1, true);
        let mut missing = fact("medium", "b", 1, false);
        missing.usage = None;
        missing.billing_completeness = None;
        missing.billing_snapshot_missing = true;
        let mut true_zero = fact("medium", "c", 1, true);
        true_zero.usage = Some(AttemptUsage {
            completeness: MeasurementCompleteness::Complete,
            ..AttemptUsage::default()
        });
        let mut partial = fact("medium", "d", 4, false);
        partial.usage.as_mut().unwrap().completeness = MeasurementCompleteness::ObservedLowerBound;
        partial.billing_completeness = Some(BillingCompleteness::Unknown);

        let dataset = AggregateDataset::new(vec![known, missing, true_zero, partial]);
        let point = &dataset.configurations[0];

        assert_eq!(point.tokens.output_tokens.samples, 2);
        assert_eq!(point.tokens.output_tokens.mean, Some(0.5));
        assert_eq!(point.observed_tokens_lower_bound.output_tokens.samples, 3);
        assert_eq!(
            point.observed_tokens_lower_bound.output_tokens.mean,
            Some(5.0 / 3.0)
        );
        assert!(dataset.attempts[1].usage.is_none());
        assert_eq!(
            dataset.attempts[2]
                .usage
                .as_ref()
                .map(|usage| usage.combined.output_tokens),
            Some(0)
        );
    }

    #[test]
    fn excludes_infrastructure_errors_without_hiding_cleanup_failures() {
        let passed = fact("medium", "a", 1, true);
        let mut infrastructure = fact("medium", "a", 2, false);
        infrastructure.outcome = EvalOutcome::InfrastructureError;
        infrastructure.scored = false;
        infrastructure.errored = true;
        infrastructure.exception_kind = Some(EvalExceptionKind::Environment);
        infrastructure.cleanup_failed = true;

        let dataset = AggregateDataset::new(vec![passed, infrastructure]);
        let point = &dataset.configurations[0];

        assert_eq!(point.success.samples, 2);
        assert_eq!(point.success.successes, 1);
        assert_eq!(point.verifier_conditioned_success.samples, 1);
        assert_eq!(point.verifier_conditioned_success.successes, 1);
        assert_eq!(point.unscored_attempts, 1);
        assert_eq!(point.errored_attempts, 1);
        assert_eq!(point.cleanup_failures, 1);
        assert_eq!(point.tasks[0].success.samples, 2);
        assert_eq!(point.tasks[0].verifier_conditioned_success.samples, 1);
        assert_eq!(point.tasks[0].errored_attempts, 1);
        assert_eq!(point.tasks[0].cleanup_failures, 1);
        assert_eq!(point.exceptions[&EvalExceptionKind::Environment], 1);
    }

    #[test]
    fn pass_at_k_counts_every_terminal_attempt_and_uses_harbor_k_values() {
        let mut attempts = Vec::new();
        for repetition in 1..=20 {
            let mut attempt = fact("medium", "a", repetition, repetition == 1);
            if repetition == 2 {
                attempt.scored = false;
                attempt.outcome = EvalOutcome::InfrastructureError;
                attempt.errored = true;
            }
            attempts.push(attempt);
        }
        let dataset = AggregateDataset::new(attempts);
        let point = &dataset.configurations[0];

        assert_eq!(point.success.samples, 20);
        assert_eq!(point.success.successes, 1);
        assert_eq!(point.verifier_conditioned_success.samples, 19);
        assert_eq!(point.verifier_conditioned_success.successes, 1);
        assert_eq!(
            point.pass_at_k.keys().copied().collect::<Vec<_>>(),
            [2, 4, 5, 8, 10, 15, 16, 20]
        );
        assert!((point.pass_at_k[&20] - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn safety_refusals_overlap_refusal_and_error_axes() {
        let mut refusal = fact("medium", "a", 1, false);
        refusal.outcome = EvalOutcome::SafetyRefusal;
        refusal.scored = false;
        refusal.errored = true;
        refusal.refused = true;

        let dataset = AggregateDataset::new(vec![refusal]);
        let point = &dataset.configurations[0];

        assert_eq!(point.success.samples, 1);
        assert_eq!(point.success.successes, 0);
        assert_eq!(point.verifier_conditioned_success.samples, 0);
        assert_eq!(point.verifier_conditioned_success.rate, None);
        assert_eq!(point.verifier_conditioned_success.confidence_low, None);
        assert_eq!(point.verifier_conditioned_success.confidence_high, None);
        let encoded = serde_json::to_value(point.verifier_conditioned_success).unwrap();
        assert!(encoded["rate"].is_null());
        assert!(encoded["confidence_low"].is_null());
        assert!(encoded["confidence_high"].is_null());
        assert_eq!(point.errored_attempts, 1);
        assert_eq!(point.refused_attempts, 1);
        assert_eq!(point.tasks[0].errored_attempts, 1);
        assert_eq!(point.tasks[0].refused_attempts, 1);
    }

    #[test]
    fn billing_uncertain_attempt_excludes_exact_cost_but_retains_lower_bound() {
        let known = fact("medium", "a", 1, true);
        let mut billing_uncertain = fact("medium", "a", 2, false);
        billing_uncertain.billing_completeness = Some(BillingCompleteness::Unknown);

        let dataset = AggregateDataset::new(vec![known, billing_uncertain]);
        let point = &dataset.configurations[0];

        assert_eq!(point.cost_usd.samples, 1);
        assert_eq!(point.cost_usd.mean, Some(1.0));
        assert_eq!(point.observed_cost_lower_bound_usd.samples, 2);
        assert_eq!(point.observed_cost_lower_bound_usd.mean, Some(1.5));
        assert_eq!(point.billing_unknown_attempts, 1);
        assert_eq!(point.tasks[0].cost_usd.samples, 1);
        assert_eq!(point.tasks[0].observed_cost_lower_bound_usd.samples, 2);
        assert_eq!(point.tasks[0].billing_unknown_attempts, 1);
        assert_eq!(dataset.attempts[1].cost_usd, Some(2.0));
    }
}
