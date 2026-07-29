use std::{
    collections::BTreeMap,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use clap::Args;
use eyre::{Result, eyre};
use nanocodex_eval::{
    AtifSource, AtifTrajectory, BillingCompleteness, EvalCleanup, EvalOutcome,
    MeasurementCompleteness, PhaseTiming, UsageTotals, infer_retained_scored,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;
use yansi::Painted;

#[derive(Args)]
pub(crate) struct Inspect {
    /// Harbor job or trial directory to inspect.
    #[arg(value_name = "DIRECTORY")]
    directory: PathBuf,

    /// Select one trial by its exact name or unique prefix.
    #[arg(long, value_name = "NAME")]
    trial: Option<String>,

    /// Emit the typed inspection report as JSON.
    #[arg(long)]
    json: bool,

    /// Include complete verifier, agent stderr, and VM network logs.
    #[arg(long)]
    full: bool,
}

impl Inspect {
    pub(crate) fn run(self) -> Result<()> {
        let directory = self.directory.canonicalize()?;
        let report = if directory.join("agent/trajectory.json").is_file() {
            if self.trial.is_some() {
                return Err(eyre!("--trial is only valid when inspecting a job"));
            }
            Inspection::Trial(Box::new(TrialInspection::load(&directory, self.full)?))
        } else {
            let job = JobInspection::load(&directory, self.full)?;
            match self.trial {
                Some(selector) => {
                    Inspection::Trial(Box::new(job.select_trial(&selector)?.to_owned()))
                }
                None => Inspection::Job(job),
            }
        };
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        if self.json {
            serde_json::to_writer_pretty(&mut stdout, &report)?;
            writeln!(stdout)?;
        } else {
            report.write_human(&mut stdout, self.full)?;
        }
        Ok(())
    }
}

#[derive(Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Inspection {
    Job(JobInspection),
    Trial(Box<TrialInspection>),
}

impl Inspection {
    fn write_human(&self, output: &mut impl Write, full: bool) -> io::Result<()> {
        match self {
            Self::Job(job) => job.write_human(output),
            Self::Trial(trial) => trial.write_human(output, full),
        }
    }
}

#[derive(Clone, Serialize)]
struct JobInspection {
    id: Uuid,
    directory: PathBuf,
    total: usize,
    passed: usize,
    failed: usize,
    unscored: usize,
    refused: usize,
    errored: usize,
    cleanup_failed: usize,
    trials: Vec<TrialInspection>,
}

impl JobInspection {
    fn load(directory: &Path, full: bool) -> Result<Self> {
        let result = read_json::<HarborJobResult>(&directory.join("result.json"))?;
        let mut trial_directories = fs::read_dir(directory)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir() && path.join("agent/trajectory.json").is_file())
            .collect::<Vec<_>>();
        trial_directories.sort();
        let trials = trial_directories
            .iter()
            .map(|path| TrialInspection::load(path, full))
            .collect::<Result<Vec<_>>>()?;
        let passed = trials
            .iter()
            .filter(|trial| trial.score_status == TrialScoreStatus::Passed)
            .count();
        let failed = trials
            .iter()
            .filter(|trial| trial.score_status == TrialScoreStatus::Failed)
            .count();
        let unscored = trials
            .iter()
            .filter(|trial| trial.score_status == TrialScoreStatus::Unscored)
            .count();
        let refused = trials.iter().filter(|trial| trial.refused).count();
        let errored = trials.iter().filter(|trial| trial.errored).count();
        let cleanup_failed = trials.iter().filter(|trial| trial.cleanup_failed).count();
        Ok(Self {
            id: result.id,
            directory: directory.to_path_buf(),
            total: result.n_total_trials,
            passed,
            failed,
            unscored,
            refused,
            errored,
            cleanup_failed,
            trials,
        })
    }

    fn select_trial(&self, selector: &str) -> Result<&TrialInspection> {
        if let Some(trial) = self
            .trials
            .iter()
            .find(|trial| trial.trial_name == selector)
        {
            return Ok(trial);
        }
        let matches = self
            .trials
            .iter()
            .filter(|trial| trial.trial_name.starts_with(selector))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [trial] => Ok(*trial),
            [] => Err(eyre!("job contains no trial matching {selector:?}")),
            _ => Err(eyre!(
                "trial selector {selector:?} is ambiguous; use the exact trial name"
            )),
        }
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(
            output,
            "Job {} scores: {} passed, {} failed, {} unscored ({} retained / {} expected)",
            self.id,
            self.passed,
            self.failed,
            self.unscored,
            self.trials.len(),
            self.total
        )?;
        writeln!(
            output,
            "Lifecycle: {} errored attempts (including {} safety refusals), {} cleanup failures",
            self.errored, self.refused, self.cleanup_failed
        )?;
        writeln!(output, "{}", self.directory.display())?;
        for trial in &self.trials {
            trial.write_summary(output)?;
            if trial.score_status != TrialScoreStatus::Passed
                || trial.errored
                || trial.cleanup_failed
            {
                trial.write_failure_summary(output)?;
            }
        }
        if self.failed + self.unscored + self.errored > 0 {
            writeln!(
                output,
                "\nUse `nanocodex eval inspect {} --trial <name> --full` for complete evidence.",
                self.directory.display()
            )?;
        }
        Ok(())
    }
}

#[derive(Clone, Serialize)]
struct TrialInspection {
    id: Uuid,
    task_name: String,
    trial_name: String,
    score_status: TrialScoreStatus,
    refused: bool,
    errored: bool,
    cleanup_failed: bool,
    reward: Option<f64>,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    duration: DurationMillis,
    phases: PhaseInspection,
    agent: Option<AgentInspection>,
    exception: Option<ExceptionInspection>,
    tests: Option<TestInspection>,
    final_response: Option<String>,
    artifacts: ArtifactPaths,
    full_output: Option<FullOutput>,
}

impl TrialInspection {
    fn load(directory: &Path, full: bool) -> Result<Self> {
        let result = read_json::<HarborTrialResult>(&directory.join("result.json"))?;
        let trajectory = read_json::<AtifTrajectory>(&directory.join("agent/trajectory.json"))?;
        let ctrf = read_optional_json::<CtrfReport>(&directory.join("verifier/ctrf.json"))?;
        let reward = result
            .verifier_result
            .as_ref()
            .and_then(|verifier| verifier.rewards.get("reward"))
            .copied();
        let classification = trial_classification(
            result.outcome,
            result.scored,
            result
                .exception_info
                .as_ref()
                .map(|exception| exception.exception_type.as_str()),
            result.verifier_result.as_ref(),
        );
        let cleanup_failed = result.cleanup.is_failed()
            || result
                .exception_info
                .as_ref()
                .is_some_and(|exception| exception.exception_type == "CleanupError");
        let artifacts = ArtifactPaths::new(directory);
        let full_output = full.then(|| FullOutput::load(&artifacts)).transpose()?;
        let phases = PhaseInspection::from_result(&result);
        let duration = disjoint_duration(&result);
        Ok(Self {
            id: result.id,
            task_name: result.task_name,
            trial_name: result.trial_name,
            score_status: classification.score,
            refused: classification.refused,
            errored: classification.errored,
            cleanup_failed,
            reward,
            started_at: result.started_at,
            finished_at: result.finished_at,
            duration,
            phases,
            agent: result.agent_result.map(Into::into),
            exception: result.exception_info.map(Into::into),
            tests: ctrf.map(Into::into),
            final_response: trajectory
                .steps
                .iter()
                .rev()
                .find(|step| matches!(step.source, AtifSource::Agent) && !step.message.is_empty())
                .map(|step| step.message.clone()),
            artifacts,
            full_output,
        })
    }

    fn write_human(&self, output: &mut impl Write, full: bool) -> io::Result<()> {
        self.write_summary(output)?;
        writeln!(output, "task: {}", self.task_name)?;
        writeln!(
            output,
            "duration: {} (agent {}, verifier {})",
            self.duration,
            format_duration(self.phases.agent_execution),
            format_duration(self.phases.verifier)
        )?;
        if let Some(agent) = &self.agent {
            let runtime_prefix =
                if agent.runtime_completeness == MeasurementCompleteness::ObservedLowerBound {
                    "at least "
                } else {
                    ""
                };
            writeln!(
                output,
                "agent: {runtime_prefix}{} model calls, {runtime_prefix}{} tool calls",
                agent.model_calls, agent.tool_calls,
            )?;
            match agent.usage_completeness {
                Some(completeness) => {
                    let usage_prefix =
                        if completeness == MeasurementCompleteness::ObservedLowerBound {
                            "at least "
                        } else {
                            ""
                        };
                    writeln!(
                        output,
                        "tokens: {usage_prefix}{} input / {usage_prefix}{} cached / \
                         {usage_prefix}{} output ({}.{:01}% cache)",
                        agent.input_tokens,
                        agent.cached_tokens,
                        agent.output_tokens,
                        agent.cache_percent_tenths / 10,
                        agent.cache_percent_tenths % 10
                    )?;
                }
                None => writeln!(output, "tokens: unreported")?,
            }
            let billing = match agent.billing_completeness {
                Some(BillingCompleteness::Complete) => "complete",
                Some(BillingCompleteness::Unknown) => "unknown",
                None => "unreported",
            };
            let cost = agent
                .cost_usd
                .map_or_else(|| "unavailable".to_owned(), |cost| format!("${cost:.6}"));
            writeln!(output, "billing: {billing}; estimated cost: {cost}")?;
        }
        self.write_failure_reason(output)?;
        if let Some(response) = &self.final_response {
            writeln!(output, "\nFinal agent response:\n{response}")?;
        }
        writeln!(output, "\nArtifacts:")?;
        self.artifacts.write_human(output)?;
        if full && let Some(full_output) = &self.full_output {
            full_output.write_human(output)?;
        }
        Ok(())
    }

    fn write_summary(&self, output: &mut impl Write) -> io::Result<()> {
        let reward = self
            .reward
            .map_or_else(|| "-".to_owned(), |reward| format!("{reward:.3}"));
        writeln!(
            output,
            "{} {} reward={reward}{}{}{}",
            self.score_status.label(),
            self.trial_name,
            if self.refused { " refusal=true" } else { "" },
            if self.errored { " error=true" } else { "" },
            if self.cleanup_failed {
                " cleanup=failed"
            } else {
                ""
            }
        )
    }

    fn write_failure_reason(&self, output: &mut impl Write) -> io::Result<()> {
        if let Some(exception) = &self.exception {
            writeln!(
                output,
                "  exception: {}: {}",
                exception.exception_type, exception.message
            )?;
        }
        if let Some(tests) = &self.tests {
            writeln!(
                output,
                "  tests: {} passed, {} failed, {} skipped",
                tests.passed, tests.failed, tests.skipped
            )?;
            for test in &tests.failures {
                writeln!(output, "  - {} [{}]", test.name, test.status.as_str())?;
                if let Some(message) = &test.message {
                    writeln!(output, "    {message}")?;
                }
                if let Some(trace) = &test.trace {
                    for line in trace.lines() {
                        writeln!(output, "    {line}")?;
                    }
                }
            }
        }
        Ok(())
    }

    fn write_failure_summary(&self, output: &mut impl Write) -> io::Result<()> {
        if let Some(exception) = &self.exception {
            writeln!(
                output,
                "  exception: {}: {}",
                exception.exception_type, exception.message
            )?;
        }
        if let Some(tests) = &self.tests {
            writeln!(
                output,
                "  tests: {} passed, {} failed, {} skipped",
                tests.passed, tests.failed, tests.skipped
            )?;
            for test in &tests.failures {
                let message = test
                    .message
                    .as_deref()
                    .and_then(|message| message.lines().next())
                    .unwrap_or("no failure message");
                writeln!(
                    output,
                    "  - {} [{}]: {message}",
                    test.name,
                    test.status.as_str()
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TrialScoreStatus {
    Passed,
    Failed,
    Unscored,
}

impl TrialScoreStatus {
    const fn label(self) -> Painted<&'static str> {
        match self {
            Self::Passed => Painted::new("PASS   ").green(),
            Self::Failed => Painted::new("FAIL   ").red(),
            Self::Unscored => Painted::new("UNSCORED").yellow(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TrialClassification {
    score: TrialScoreStatus,
    refused: bool,
    errored: bool,
}

fn trial_classification(
    outcome: Option<EvalOutcome>,
    scored: Option<bool>,
    exception_type: Option<&str>,
    verifier: Option<&HarborVerifierResult>,
) -> TrialClassification {
    let scored = infer_retained_scored(
        scored,
        outcome,
        verifier.is_some(),
        exception_type.is_some(),
    );
    let passed = scored
        && verifier.is_some_and(|verifier| verifier.rewards.values().all(|reward| *reward > 0.0));
    let (refused, errored) = match exception_type {
        Some(exception) => (
            exception == "AgentSafetyRefusalError",
            exception != "CleanupError",
        ),
        None => (
            outcome == Some(EvalOutcome::SafetyRefusal),
            outcome.is_some_and(|outcome| {
                matches!(
                    outcome,
                    EvalOutcome::SafetyRefusal
                        | EvalOutcome::AgentTimeout
                        | EvalOutcome::InfrastructureError
                )
            }),
        ),
    };
    TrialClassification {
        score: if !scored {
            TrialScoreStatus::Unscored
        } else if passed {
            TrialScoreStatus::Passed
        } else {
            TrialScoreStatus::Failed
        },
        refused,
        errored,
    }
}

#[cfg(test)]
fn trial_classification_for_reward(
    outcome: Option<EvalOutcome>,
    scored: Option<bool>,
    exception_type: Option<&str>,
    reward: Option<f64>,
) -> TrialClassification {
    let verifier = reward.map(|reward| HarborVerifierResult {
        rewards: BTreeMap::from([("reward".to_owned(), reward)]),
    });
    trial_classification(outcome, scored, exception_type, verifier.as_ref())
}

#[derive(Clone, Serialize)]
struct PhaseInspection {
    queue_wait: Option<DurationMillis>,
    environment_setup: Option<DurationMillis>,
    environment_readiness: Option<DurationMillis>,
    agent_setup: Option<DurationMillis>,
    agent_execution: Option<DurationMillis>,
    verifier: Option<DurationMillis>,
    agent_cleanup: Option<DurationMillis>,
    verifier_cleanup: Option<DurationMillis>,
}

#[derive(Clone, Copy, Serialize)]
#[serde(transparent)]
struct DurationMillis(i64);

impl std::fmt::Display for DurationMillis {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let seconds = self.0 / 1_000;
        let millis = self.0.unsigned_abs() % 1_000;
        write!(formatter, "{seconds}.{millis:03}s")
    }
}

impl PhaseInspection {
    fn from_result(result: &HarborTrialResult) -> Self {
        Self {
            queue_wait: phase_duration(result.queue_wait.as_ref()),
            environment_setup: phase_duration(result.environment_setup.as_ref()),
            environment_readiness: phase_duration(result.environment_readiness.as_ref()),
            agent_setup: phase_duration(result.agent_setup.as_ref()),
            agent_execution: phase_duration(result.agent_execution.as_ref()),
            verifier: phase_duration(result.verifier.as_ref()),
            agent_cleanup: result
                .cleanup
                .agent
                .timing
                .as_ref()
                .map(phase_duration_exact),
            verifier_cleanup: result
                .cleanup
                .verifier
                .timing
                .as_ref()
                .map(phase_duration_exact),
        }
    }
}

#[derive(Clone, Serialize)]
struct AgentInspection {
    input_tokens: u64,
    cached_tokens: u64,
    output_tokens: u64,
    cache_percent_tenths: u16,
    model_calls: u32,
    tool_calls: u32,
    runtime_completeness: MeasurementCompleteness,
    usage_completeness: Option<MeasurementCompleteness>,
    cost_usd: Option<f64>,
    billing_completeness: Option<BillingCompleteness>,
}

impl From<HarborAgentResult> for AgentInspection {
    fn from(result: HarborAgentResult) -> Self {
        let usage_completeness = retained_usage_observed(&result).then_some(
            if result.billing_completeness == Some(BillingCompleteness::Complete) {
                MeasurementCompleteness::Complete
            } else {
                MeasurementCompleteness::ObservedLowerBound
            },
        );
        let cache_percent_tenths = if result.n_input_tokens == 0 {
            0
        } else {
            u16::try_from(
                u128::from(result.n_cache_tokens).saturating_mul(1_000)
                    / u128::from(result.n_input_tokens),
            )
            .unwrap_or(u16::MAX)
        };
        Self {
            input_tokens: result.n_input_tokens,
            cached_tokens: result.n_cache_tokens,
            output_tokens: result.n_output_tokens,
            cache_percent_tenths,
            model_calls: result.metadata.model_calls,
            tool_calls: result.metadata.tool_calls,
            runtime_completeness: result.metadata.runtime_completeness,
            usage_completeness,
            cost_usd: result.cost_usd,
            billing_completeness: result.billing_completeness,
        }
    }
}

fn retained_usage_observed(result: &HarborAgentResult) -> bool {
    result.cost_usd.is_some()
        || result.metadata.estimated_cost.is_some()
        || matches!(
            result.metadata.cost_status.as_str(),
            "estimated_from_usage" | "estimated_lower_bound"
        )
        || result.n_input_tokens != 0
        || result.n_cache_tokens != 0
        || result.n_output_tokens != 0
        || usage_nonzero(&result.metadata.usage)
        || usage_nonzero(&result.metadata.warmup_usage)
}

const fn usage_nonzero(usage: &UsageTotals) -> bool {
    usage.input_tokens != 0
        || usage.cached_input_tokens != 0
        || usage.cache_write_input_tokens != 0
        || usage.output_tokens != 0
        || usage.reasoning_output_tokens != 0
        || usage.total_tokens != 0
}

#[derive(Clone, Serialize)]
struct ExceptionInspection {
    exception_type: String,
    message: String,
    traceback: String,
    occurred_at: DateTime<Utc>,
}

impl From<HarborExceptionInfo> for ExceptionInspection {
    fn from(exception: HarborExceptionInfo) -> Self {
        Self {
            exception_type: exception.exception_type,
            message: exception.exception_message,
            traceback: exception.exception_traceback,
            occurred_at: exception.occurred_at,
        }
    }
}

#[derive(Clone, Serialize)]
struct TestInspection {
    total: u32,
    passed: u32,
    failed: u32,
    skipped: u32,
    failures: Vec<TestFailure>,
}

impl From<CtrfReport> for TestInspection {
    fn from(report: CtrfReport) -> Self {
        let failures = report
            .results
            .tests
            .into_iter()
            .filter(|test| test.status != CtrfStatus::Passed)
            .map(Into::into)
            .collect();
        Self {
            total: report.results.summary.tests,
            passed: report.results.summary.passed,
            failed: report.results.summary.failed,
            skipped: report.results.summary.skipped,
            failures,
        }
    }
}

#[derive(Clone, Serialize)]
struct TestFailure {
    name: String,
    status: CtrfStatus,
    duration_seconds: Option<f64>,
    message: Option<String>,
    trace: Option<String>,
}

impl From<CtrfTest> for TestFailure {
    fn from(test: CtrfTest) -> Self {
        Self {
            name: test.name,
            status: test.status,
            duration_seconds: test.duration,
            message: test.message,
            trace: test.trace,
        }
    }
}

#[derive(Clone, Serialize)]
struct ArtifactPaths {
    result: PathBuf,
    trajectory: PathBuf,
    events: PathBuf,
    verifier_output: PathBuf,
    ctrf: PathBuf,
    agent_stderr: PathBuf,
    network_log: PathBuf,
    rootfs: PathBuf,
}

impl ArtifactPaths {
    fn new(directory: &Path) -> Self {
        Self {
            result: directory.join("result.json"),
            trajectory: directory.join("agent/trajectory.json"),
            events: directory.join("agent/events.jsonl"),
            verifier_output: directory.join("verifier/test-stdout.txt"),
            ctrf: directory.join("verifier/ctrf.json"),
            agent_stderr: directory.join("agent/stderr.log"),
            network_log: directory.join("vm/gvproxy.log"),
            rootfs: directory.join("rootfs.ext4"),
        }
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        for (name, path) in [
            ("result", &self.result),
            ("trajectory", &self.trajectory),
            ("events", &self.events),
            ("verifier", &self.verifier_output),
            ("ctrf", &self.ctrf),
            ("agent stderr", &self.agent_stderr),
            ("VM network", &self.network_log),
            ("retained rootfs", &self.rootfs),
        ] {
            if path.exists() {
                writeln!(output, "  {name}: {}", path.display())?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Serialize)]
struct FullOutput {
    verifier: Option<String>,
    agent_stderr: Option<String>,
    network: Option<String>,
}

impl FullOutput {
    fn load(paths: &ArtifactPaths) -> io::Result<Self> {
        Ok(Self {
            verifier: read_optional_text(&paths.verifier_output)?,
            agent_stderr: read_optional_text(&paths.agent_stderr)?,
            network: read_optional_text(&paths.network_log)?,
        })
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        for (name, contents) in [
            ("Verifier output", self.verifier.as_deref()),
            ("Agent stderr", self.agent_stderr.as_deref()),
            ("VM network log", self.network.as_deref()),
        ] {
            if let Some(contents) = contents.filter(|contents| !contents.is_empty()) {
                writeln!(output, "\n{name}:\n{contents}")?;
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct HarborJobResult {
    id: Uuid,
    n_total_trials: usize,
}

#[derive(Deserialize)]
struct HarborTrialResult {
    id: Uuid,
    task_name: String,
    trial_name: String,
    agent_result: Option<HarborAgentResult>,
    verifier_result: Option<HarborVerifierResult>,
    outcome: Option<EvalOutcome>,
    scored: Option<bool>,
    #[serde(default)]
    cleanup: EvalCleanup,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    queue_wait: Option<HarborPhaseTiming>,
    environment_setup: Option<HarborPhaseTiming>,
    environment_readiness: Option<HarborPhaseTiming>,
    agent_setup: Option<HarborPhaseTiming>,
    agent_execution: Option<HarborPhaseTiming>,
    verifier: Option<HarborPhaseTiming>,
    exception_info: Option<HarborExceptionInfo>,
}

#[derive(Deserialize)]
struct HarborAgentResult {
    n_input_tokens: u64,
    n_cache_tokens: u64,
    n_output_tokens: u64,
    cost_usd: Option<f64>,
    billing_completeness: Option<BillingCompleteness>,
    metadata: nanocodex_eval::AgentMetadata,
}

#[derive(Deserialize)]
struct HarborVerifierResult {
    rewards: BTreeMap<String, f64>,
}

#[derive(Deserialize)]
struct HarborExceptionInfo {
    exception_type: String,
    exception_message: String,
    exception_traceback: String,
    occurred_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct CtrfReport {
    results: CtrfResults,
}

#[derive(Deserialize)]
struct CtrfResults {
    summary: CtrfSummary,
    tests: Vec<CtrfTest>,
}

#[derive(Deserialize)]
struct CtrfSummary {
    tests: u32,
    passed: u32,
    failed: u32,
    skipped: u32,
}

#[derive(Deserialize)]
struct CtrfTest {
    name: String,
    status: CtrfStatus,
    duration: Option<f64>,
    message: Option<String>,
    trace: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum CtrfStatus {
    Passed,
    Failed,
    Skipped,
    Pending,
    Other,
}

impl CtrfStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Pending => "pending",
            Self::Other => "other",
        }
    }
}

#[derive(Deserialize)]
struct HarborPhaseTiming {
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
}

fn phase_duration(timing: Option<&HarborPhaseTiming>) -> Option<DurationMillis> {
    timing.map(|timing| {
        DurationMillis(
            timing
                .finished_at
                .signed_duration_since(timing.started_at)
                .num_milliseconds(),
        )
    })
}

fn phase_duration_exact(timing: &PhaseTiming) -> DurationMillis {
    DurationMillis(
        timing
            .finished_at
            .signed_duration_since(timing.started_at)
            .num_milliseconds()
            .max(0),
    )
}

fn disjoint_duration(result: &HarborTrialResult) -> DurationMillis {
    let retained = [
        result.queue_wait.as_ref(),
        result.environment_setup.as_ref(),
        result.environment_readiness.as_ref(),
        result.agent_setup.as_ref(),
        result.agent_execution.as_ref(),
        result.verifier.as_ref(),
    ]
    .into_iter()
    .flatten()
    .map(|timing| phase_duration(Some(timing)).map_or(0, |duration| duration.0));
    let cleanup = [
        result.cleanup.agent.timing.as_ref(),
        result.cleanup.verifier.timing.as_ref(),
    ]
    .into_iter()
    .flatten()
    .map(|timing| phase_duration_exact(timing).0);
    DurationMillis(retained.chain(cleanup).fold(0_i64, i64::saturating_add))
}

fn format_duration(duration: Option<DurationMillis>) -> String {
    duration.map_or_else(|| "-".to_owned(), |duration| duration.to_string())
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let contents = fs::read(path)?;
    serde_json::from_slice(&contents).map_err(Into::into)
}

fn read_optional_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::read(path) {
        Ok(contents) => serde_json::from_slice(&contents)
            .map(Some)
            .map_err(Into::into),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_optional_text(path: &Path) -> io::Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        AgentInspection, EvalOutcome, HarborAgentResult, MeasurementCompleteness,
        TrialClassification, TrialScoreStatus, trial_classification_for_reward,
    };

    #[test]
    fn refusals_overlap_the_error_axis() {
        assert_eq!(
            trial_classification_for_reward(None, None, Some("AgentSafetyRefusalError"), None),
            TrialClassification {
                score: TrialScoreStatus::Unscored,
                refused: true,
                errored: true,
            }
        );
        assert_eq!(
            trial_classification_for_reward(None, None, Some("AgentAuthenticationError"), None),
            TrialClassification {
                score: TrialScoreStatus::Unscored,
                refused: false,
                errored: true,
            }
        );
    }

    #[test]
    fn explicit_exception_precedes_legacy_outcome_lifecycle_axes() {
        let cleanup = trial_classification_for_reward(
            Some(EvalOutcome::InfrastructureError),
            Some(false),
            Some("CleanupError"),
            None,
        );
        assert!(!cleanup.refused);
        assert!(!cleanup.errored);

        let explicit_non_refusal = trial_classification_for_reward(
            Some(EvalOutcome::SafetyRefusal),
            Some(false),
            Some("VerifierError"),
            None,
        );
        assert!(!explicit_non_refusal.refused);
        assert!(explicit_non_refusal.errored);

        let legacy_refusal = trial_classification_for_reward(
            Some(EvalOutcome::SafetyRefusal),
            Some(false),
            None,
            None,
        );
        assert!(legacy_refusal.refused);
        assert!(legacy_refusal.errored);
    }

    #[test]
    fn classifies_scored_trials_from_reward() {
        assert_eq!(
            trial_classification_for_reward(None, None, None, Some(1.0)).score,
            TrialScoreStatus::Passed
        );
        assert_eq!(
            trial_classification_for_reward(None, None, None, Some(0.0)).score,
            TrialScoreStatus::Failed
        );
        assert_eq!(
            trial_classification_for_reward(None, None, None, None).score,
            TrialScoreStatus::Unscored
        );
        assert_eq!(
            trial_classification_for_reward(None, Some(false), None, Some(1.0)).score,
            TrialScoreStatus::Unscored
        );
        assert_eq!(
            trial_classification_for_reward(
                Some(EvalOutcome::Passed),
                Some(true),
                Some("CleanupError"),
                Some(1.0),
            )
            .score,
            TrialScoreStatus::Passed
        );
        let scored_timeout = trial_classification_for_reward(
            Some(EvalOutcome::AgentTimeout),
            Some(true),
            Some("AgentTimeoutError"),
            Some(1.0),
        );
        assert_eq!(scored_timeout.score, TrialScoreStatus::Passed);
        assert!(scored_timeout.errored);

        assert_eq!(
            trial_classification_for_reward(
                Some(EvalOutcome::AgentTimeout),
                None,
                None,
                Some(1.0),
            )
            .score,
            TrialScoreStatus::Unscored
        );
        assert_eq!(
            trial_classification_for_reward(None, None, Some("AgentTimeoutError"), Some(1.0),)
                .score,
            TrialScoreStatus::Unscored
        );
    }

    #[test]
    fn inspection_preserves_runtime_lower_bounds_and_missing_usage() {
        let retained: HarborAgentResult = serde_json::from_value(json!({
            "n_input_tokens": 0,
            "n_cache_tokens": 0,
            "n_output_tokens": 0,
            "cost_usd": null,
            "billing_completeness": "unknown",
            "metadata": {
                "status": "cancelled",
                "model": "gpt-5.6-sol",
                "effort": "medium",
                "transport": "responses_websocket_v2",
                "orchestration": "agent",
                "duration_ms": 1,
                "duration_ns": 1_000_000,
                "model_calls": 1,
                "steers": 0,
                "compactions": 0,
                "tool_calls": 0,
                "connection_attempts": 1,
                "websocket_reconnects": 0,
                "response_attempts": 1,
                "response_retries": 0,
                "connection_duration_ns": 1,
                "retry_backoff_duration_ns": 0,
                "model_duration_ns": 0,
                "warmup_duration_ns": 0,
                "tool_work_duration_ns": 0,
                "tool_wall_duration_ns": 0,
                "usage": {
                    "input_tokens": 0,
                    "cached_input_tokens": 0,
                    "cache_write_input_tokens": 0,
                    "output_tokens": 0,
                    "reasoning_output_tokens": 0,
                    "total_tokens": 0,
                },
                "warmup_usage": {
                    "input_tokens": 0,
                    "cached_input_tokens": 0,
                    "cache_write_input_tokens": 0,
                    "output_tokens": 0,
                    "reasoning_output_tokens": 0,
                    "total_tokens": 0,
                },
                "cost_usd": null,
                "cost_status": "usage_not_reported",
            },
        }))
        .unwrap();

        let inspection = AgentInspection::from(retained);

        assert_eq!(
            inspection.runtime_completeness,
            MeasurementCompleteness::ObservedLowerBound
        );
        assert_eq!(inspection.usage_completeness, None);
        let encoded = serde_json::to_value(inspection).unwrap();
        assert_eq!(encoded["runtime_completeness"], "observed_lower_bound");
        assert!(encoded["usage_completeness"].is_null());
    }
}
