use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display, Formatter, Write as _},
    fs::{self, File},
    future::Future,
    io::{self, BufRead, BufReader, Read, Write},
    net::{Ipv4Addr, TcpListener},
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use chrono::{DateTime, Utc};
use nanocodex_agent::{NanocodexBuilder, Thinking, events::AgentEventKind};
use nanocodex_oai_api::MODEL;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use tokio::{
    io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _},
    sync::mpsc,
    task::JoinHandle,
    time::Instant,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    AgentResult, AtifBuilder, AtifSource, AtifStep, AtifToolCall, AtifTrajectory, AttemptAgent,
    CodexCommandOutput, CodexCommandRunner, CodexCommandRunnerError, CodexCommandStatus, CodexExec,
    EvalAttempt, EvalAttemptOutcome, EvalEventKind, EvalEventStream, EvalExceptionKind,
    EvalOutcome, EvalStatus, Evaluator, EvaluatorBuilder, MeasurementCompleteness,
    ResponsesCaptureProxy, ResponsesCaptureProxyConfig, ResponsesModelCatalogOverride, Task,
    UsageTotals, project_codex_atif,
    vm::{
        SharedDirectory, VmAttempt, VmAttemptError, VmBackend, VmCommand, VmEnvironment,
        VmResources, VmToolSessionError, VmToolSessionHandle, reflink_or_sparse_copy,
    },
};

type BoxError = Box<dyn Error + Send + Sync + 'static>;
type InternalResult<T, E = BoxError> = std::result::Result<T, E>;

macro_rules! diff_error {
    ($message:literal $(, $argument:expr)* $(,)?) => {
        boxed_message(format!($message $(, $argument)*))
    };
    ($error:expr $(,)?) => {
        boxed_message($error.to_string())
    };
}

fn boxed_message(message: impl Into<String>) -> BoxError {
    Box::new(io::Error::other(message.into()))
}

#[derive(Debug)]
struct ContextError {
    context: String,
    source: BoxError,
}

impl Display for ContextError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.context)
    }
}

impl Error for ContextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

trait WrapErr<T> {
    fn wrap_err(self, context: impl Into<String>) -> InternalResult<T>;

    fn wrap_err_with(self, context: impl FnOnce() -> String) -> InternalResult<T>;
}

impl<T, E> WrapErr<T> for std::result::Result<T, E>
where
    E: Error + Send + Sync + 'static,
{
    fn wrap_err(self, context: impl Into<String>) -> InternalResult<T> {
        self.map_err(|source| {
            Box::new(ContextError {
                context: context.into(),
                source: Box::new(source),
            }) as BoxError
        })
    }

    fn wrap_err_with(self, context: impl FnOnce() -> String) -> InternalResult<T> {
        self.map_err(|source| {
            Box::new(ContextError {
                context: context(),
                source: Box::new(source),
            }) as BoxError
        })
    }
}

const DEFAULT_OUTPUT_DIRECTORY: &str = ".nanocodex/eval-diff";
const COMPARISON_FILE: &str = "comparison.json";
const COMPARISON_SCHEMA_VERSION: u32 = 5;
const PROGRESS_FILE: &str = "progress.jsonl";
const PROGRESS_SCHEMA_VERSION: u32 = 1;
const PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const PROGRESS_HEARTBEAT_SUMMARY_CHARS: usize = 64;
const PROGRESS_SUMMARY_CHARS: usize = 180;
const TRAJECTORY_FILE: &str = "agent/trajectory.json";
const API_EXCHANGES_FILE: &str = "agent/api-exchanges.jsonl";
const API_COMPARISON_FILE: &str = "api-comparison.json";
const API_CAPTURE_SCHEMA_VERSION: u32 = 1;
const API_COMPARISON_SCHEMA_VERSION: u32 = 11;
const DIFF_CODEX_SHARE_TAG: &str = "nanoeval-codex";
const DIFF_CODEX_SHARE_MOUNT: &str = "/run/nanoeval-codex";
const DIFF_CODEX_GUEST_BINARY: &str = "/run/nanoeval-codex/codex";
const DIFF_CAPTURE_PROXY_VM_HOST: &str = "host.containers.internal";
const DIFF_CAPTURE_PROXY_API_UPSTREAM: &str = "https://api.openai.com/v1";
const DIFF_CAPTURE_PROXY_CHATGPT_UPSTREAM: &str = "https://chatgpt.com/backend-api/codex";
const DIFF_CAPTURE_PROXY_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const DIFF_API_EXCHANGES_FILENAME: &str = "api-exchanges.jsonl";
const DIFF_CODEX_HOME: &str = "/run/nanoeval-codex-home";
const DIFF_CODEX_AUTH_FILE: &str = "/run/nanoeval-codex-home/auth.json";
const DIFF_CODEX_CLOUD_CONFIG_CACHE_FILENAME: &str = "cloud-config-bundle-cache.json";
const DIFF_CODEX_CLOUD_CONFIG_CACHE_FILE: &str =
    "/run/nanoeval-codex-home/cloud-config-bundle-cache.json";
const DIFF_CODEX_CA_BUNDLE_FILENAME: &str = "ca-certificates.pem";
const DIFF_CODEX_CA_BUNDLE_FILE: &str = "/run/nanoeval-codex/ca-certificates.pem";
const DIFF_CODEX_CA_CERTIFICATE_ENVIRONMENT: &str = "CODEX_CA_CERTIFICATE";
const DIFF_CODEX_SSL_CERT_FILE_ENVIRONMENT: &str = "SSL_CERT_FILE";
const DIFF_CODEX_NIX_SSL_CERT_FILE_ENVIRONMENT: &str = "NIX_SSL_CERT_FILE";
const DIFF_CODEX_LIVE_STDOUT_FILE: &str = "/run/nanoeval-codex-home/codex-live-events.jsonl";
const DIFF_CODEX_LIVE_STDERR_FILE: &str = "/run/nanoeval-codex-home/codex-live-stderr.log";
const DIFF_CODEX_PROGRESS_POLL: Duration = Duration::from_millis(500);
const DIFF_CODEX_OUTPUT_BYTES: usize = 8 * 1024 * 1024;
const DIFF_CODEX_VERSION_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(target_arch = "aarch64")]
const VM_GUEST_TARGET: &str = "aarch64-unknown-linux-musl";
#[cfg(target_arch = "x86_64")]
const VM_GUEST_TARGET: &str = "x86_64-unknown-linux-musl";
#[cfg(target_arch = "aarch64")]
const VM_GUEST_ELF_MACHINE: u16 = 183;
#[cfg(target_arch = "x86_64")]
const VM_GUEST_ELF_MACHINE: u16 = 62;

/// One owned, matched Nanocodex-versus-Codex evaluation.
pub struct DifferentialEval {
    task: Task,
    nanocodex: NanocodexBuilder,
    codex_binary: PathBuf,
    codex_auth: CodexAuth,
    vm: VmResources,
    output: PathBuf,
    thinking: Thinking,
    web_search: bool,
    nanocodex_build: ExecutableIdentity,
}

/// Deliberate policy and required components for [`DifferentialEval`].
pub struct DifferentialEvalBuilder {
    task: Task,
    nanocodex: NanocodexBuilder,
    codex: Option<(PathBuf, CodexAuth)>,
    vm: Option<VmResources>,
    output: PathBuf,
    thinking: Thinking,
    web_search: bool,
    nanocodex_build: Option<ExecutableIdentity>,
}

/// Authentication material forwarded to a pinned stock-Codex guest.
#[derive(Clone)]
pub struct CodexAuth {
    kind: CodexAuthKind,
}

#[derive(Clone)]
enum CodexAuthKind {
    ApiKey(Arc<str>),
    AuthFile(PathBuf),
}

impl CodexAuth {
    /// Uses an OpenAI API key in the stock-Codex guest.
    #[must_use]
    pub fn api_key(api_key: impl Into<Arc<str>>) -> Self {
        Self {
            kind: CodexAuthKind::ApiKey(api_key.into()),
        }
    }

    /// Uses one Codex-compatible ChatGPT credential file in the guest.
    #[must_use]
    pub fn auth_file(path: impl Into<PathBuf>) -> Self {
        Self {
            kind: CodexAuthKind::AuthFile(path.into()),
        }
    }
}

/// A pinned executable recorded in a differential report.
#[derive(Clone, Debug, Serialize)]
pub struct ExecutableIdentity {
    path: PathBuf,
    version: String,
    git_sha: Option<String>,
    built_at: Option<String>,
    sha256: String,
}

impl ExecutableIdentity {
    /// Creates identity metadata for an executable.
    ///
    /// The file digest is computed only when the differential run begins.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>, version: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            version: version.into(),
            git_sha: None,
            built_at: None,
            sha256: String::new(),
        }
    }

    /// Records the source revision used to build the executable.
    #[must_use]
    pub fn git_sha(mut self, git_sha: impl Into<String>) -> Self {
        self.git_sha = Some(git_sha.into());
        self
    }

    /// Records the build timestamp supplied by the embedding application.
    #[must_use]
    pub fn built_at(mut self, built_at: impl Into<String>) -> Self {
        self.built_at = Some(built_at.into());
        self
    }
}

/// Missing required component while building a differential evaluation.
#[derive(Debug, thiserror::Error)]
pub enum DifferentialBuildError {
    /// No pinned stock-Codex executable and auth were supplied.
    #[error("a differential evaluation requires a stock-Codex executable and auth")]
    MissingCodex,

    /// No prepared VM resource set was supplied.
    #[error("a differential evaluation requires prepared VM resources")]
    MissingVm,

    /// No Nanocodex executable identity was supplied.
    #[error("a differential evaluation requires Nanocodex executable identity")]
    MissingNanocodexIdentity,
}

/// Runtime or retained-evidence failure in a differential evaluation.
#[derive(Debug)]
pub struct DifferentialError {
    source: BoxError,
}

impl DifferentialError {
    fn new(source: BoxError) -> Self {
        Self { source }
    }
}

impl Display for DifferentialError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.source, formatter)
    }
}

impl Error for DifferentialError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Result returned by differential execution and retained-evidence analysis.
pub type DifferentialResult<T> = std::result::Result<T, DifferentialError>;

#[derive(Serialize)]
/// Complete retained outcome and evidence index for one paired run.
pub struct DifferentialReport {
    schema_version: u32,
    id: Uuid,
    task: TaskIdentity,
    model: String,
    thinking: String,
    policy: ComparisonPolicy,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    duration_ms: u64,
    classification: DifferentialClassification,
    trajectory_comparison: TrajectoryComparison,
    api_comparison: ApiComparisonSummary,
    nanocodex_build: ExecutableIdentity,
    codex_build: ExecutableIdentity,
    nanocodex: ArmReport,
    codex: ArmReport,
    artifacts: ComparisonArtifacts,
}

/// Result of rebuilding derived trajectory and API comparisons from retained evidence.
#[derive(Serialize)]
pub struct DifferentialReanalysis {
    comparison: serde_json::Value,
    comparison_path: PathBuf,
    api_comparison_path: Option<PathBuf>,
    #[serde(skip)]
    human_summary: String,
}

#[derive(Serialize)]
struct TaskIdentity {
    name: String,
    root: PathBuf,
}

#[derive(Serialize)]
struct ComparisonPolicy {
    runner: &'static str,
    environment: &'static str,
    attempts_per_agent: u8,
    execution_mode: &'static str,
    web_search: bool,
    codex_ephemeral: bool,
    codex_approval_policy: &'static str,
    codex_sandbox: &'static str,
    tool_mode: &'static str,
    multi_agent: &'static str,
    reasoning_summary: &'static str,
    expected_visible_tools: [&'static str; 2],
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
/// Outcome relationship between the two matched verifier results.
pub enum DifferentialClassification {
    /// Both agents passed the verifier.
    BothPassed,
    /// Only stock Codex passed the verifier.
    CodexOnlyPassed,
    /// Only Nanocodex passed the verifier.
    NanocodexOnlyPassed,
    /// Both agents completed without passing the verifier.
    NeitherPassed,
    /// At least one runner or derived evidence path failed operationally.
    Incomplete,
}

#[derive(Serialize)]
struct ArmReport {
    summary: ArmSummary,
    evaluator_directory: Option<PathBuf>,
    event_log: Option<PathBuf>,
    trajectory: Option<PathBuf>,
    trajectory_summary: Option<TrajectorySummary>,
    trajectory_error: Option<String>,
    api_exchanges: Option<PathBuf>,
    api_capture: Option<ApiCaptureSummary>,
    api_capture_error: Option<String>,
    codex_events: Option<PathBuf>,
    codex_stderr: Option<PathBuf>,
    codex_summary: Option<PathBuf>,
    operational_error: Option<String>,
    event_error: Option<String>,
    outcome: Option<EvalAttemptOutcome>,
}

#[derive(Serialize)]
struct TrajectorySummary {
    total_steps: u32,
    agent_steps: u32,
    message_steps: u32,
    reasoning_steps: u32,
    tool_calls: u32,
    observations: u32,
    model_calls: Option<u32>,
    tool_projection: &'static str,
    tool_sequence: Vec<String>,
    shell_polling: ShellPollingSummary,
    usage_completeness: Option<MeasurementCompleteness>,
    runtime_completeness: MeasurementCompleteness,
}

#[derive(Serialize)]
struct ShellPollingSummary {
    poll_only_steps: u32,
    model_call_attribution_complete: bool,
    confirmed_model_calls: Option<u32>,
    empty_stdin_tool_calls: u32,
    sessions: u32,
    explicit_requested_yield_ms: u64,
    tool_wait_duration_ns: u64,
    model_duration_ns: u64,
    prompt_tokens: u64,
    cached_tokens: u64,
    completion_tokens: u64,
}

#[derive(Serialize)]
struct TrajectoryComparison {
    comparable: bool,
    tool_sequence_comparable: bool,
    tool_sequence_equal: Option<bool>,
    codex_minus_nanocodex: Option<TrajectoryDelta>,
}

#[derive(Serialize)]
struct TrajectoryDelta {
    total_steps: i64,
    agent_steps: i64,
    message_steps: i64,
    reasoning_steps: i64,
    tool_calls: Option<i64>,
    observations: Option<i64>,
    model_calls: Option<i64>,
    shell_polling: ShellPollingDelta,
}

#[derive(Serialize)]
struct ShellPollingDelta {
    poll_only_steps: i64,
    confirmed_model_calls: Option<i64>,
    empty_stdin_tool_calls: i64,
    sessions: i64,
    explicit_requested_yield_ms: i64,
    tool_wait_duration_ns: i64,
    model_duration_ns: i64,
    prompt_tokens: i64,
    cached_tokens: i64,
    completion_tokens: i64,
}

enum TrajectoryProjection {
    Nanocodex,
    Codex { version: CodexVersion },
}

enum CodexVersion {
    #[cfg(test)]
    Fixed(String),
    Guest(Arc<OnceLock<String>>),
}

impl CodexVersion {
    fn resolve(&self) -> InternalResult<String> {
        match self {
            #[cfg(test)]
            Self::Fixed(version) => Ok(version.clone()),
            Self::Guest(version) => version.get().cloned().ok_or_else(|| {
                diff_error!("stock Codex did not report its version inside the guest")
            }),
        }
    }
}

struct EventRecording {
    atif: AtifBuilder,
    atif_error: Option<String>,
}

struct TrajectoryArtifact {
    path: PathBuf,
    summary: TrajectorySummary,
}

#[derive(Serialize)]
struct ArmSummary {
    status: ArmStatus,
    outcome: Option<EvalOutcome>,
    exception: Option<EvalExceptionKind>,
    verifier_exit_code: Option<i32>,
    rewards: BTreeMap<String, f64>,
    model: Option<String>,
    tool_calls: Option<u32>,
    usage: Option<UsageTotals>,
    duration_ms: Option<u64>,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ArmStatus {
    Passed,
    VerifierFailed,
    Unscored,
    RunnerError,
}

#[derive(Serialize)]
struct ComparisonArtifacts {
    directory: PathBuf,
    comparison: PathBuf,
    progress: PathBuf,
    progress_error: Option<String>,
    api_comparison: Option<PathBuf>,
    api_comparison_error: Option<String>,
    profile_validation_error: Option<String>,
}

#[derive(Clone, Serialize)]
struct ApiCaptureSummary {
    schema_version: u32,
    payload_scope: &'static str,
    header_scope: &'static str,
    payload_fidelity: &'static str,
    records: u64,
    requests: u64,
    response_requests: u64,
    auxiliary_requests: u64,
    inbound_events: u64,
    terminal_events: u64,
    http_responses_completed: u64,
    payload_bytes: u64,
    exchange_complete: bool,
    transports: BTreeMap<String, u64>,
    phases: BTreeMap<String, u64>,
}

#[derive(Serialize)]
struct ApiComparisonReport {
    schema_version: u32,
    comparable: bool,
    request_count_equal: Option<bool>,
    aligned_requests: u64,
    nanocodex_unpaired_requests: u64,
    codex_unpaired_requests: u64,
    equal_requests: u64,
    differing_requests: u64,
    nanocodex: Option<ApiCaptureSummary>,
    codex: Option<ApiCaptureSummary>,
    first_divergence: Option<ApiFirstDivergence>,
    event_loop: ApiEventLoopComparison,
    requests: Vec<ApiRequestComparison>,
}

#[derive(Clone, Serialize)]
struct ApiComparisonSummary {
    comparable: bool,
    request_count_equal: Option<bool>,
    aligned_requests: u64,
    nanocodex_unpaired_requests: u64,
    codex_unpaired_requests: u64,
    equal_requests: u64,
    differing_requests: u64,
    first_divergence: Option<ApiFirstDivergence>,
    event_loop: ApiEventLoopComparison,
}

#[derive(Clone, Serialize)]
struct ApiFirstDivergence {
    request_index: u64,
    pointer: String,
}

#[derive(Serialize)]
struct ApiRequestComparison {
    request_index: u64,
    nanocodex_request_index: Option<u64>,
    codex_request_index: Option<u64>,
    nanocodex_phase: Option<String>,
    codex_phase: Option<String>,
    equal: bool,
    nanocodex_sha256: Option<String>,
    codex_sha256: Option<String>,
    differences: Vec<ApiJsonDifference>,
    event_loop: ApiEventLoopTurnComparison,
}

#[derive(Clone, Serialize)]
struct ApiEventLoopComparison {
    comparable: bool,
    request_count_equal: Option<bool>,
    chain_invariants_equal: Option<bool>,
    model_visible_tool_sequence_equal: Option<bool>,
    initial_input_text_sections_equal: Option<bool>,
    initial_generation_input_text_sections_equal: Option<bool>,
    initial_code_mode_tool_names_equal: Option<bool>,
    initial_code_mode_tool_definitions_equal: Option<bool>,
    aligned_turns: u64,
    nanocodex_unpaired_turns: u64,
    codex_unpaired_turns: u64,
    equal_turns: u64,
    differing_turns: u64,
    first_divergence: Option<ApiEventLoopFirstDivergence>,
    first_generation_divergence: Option<ApiEventLoopFirstDivergence>,
    nanocodex_unpaired_tail: Option<ApiEventLoopTailSummary>,
    codex_unpaired_tail: Option<ApiEventLoopTailSummary>,
    nanocodex: Option<ApiEventLoopArmSummary>,
    codex: Option<ApiEventLoopArmSummary>,
}

#[derive(Clone, Serialize)]
struct ApiEventLoopFirstDivergence {
    request_index: u64,
    pointer: String,
    categories: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct ApiEventLoopTailSummary {
    turns: u64,
    generation_turns: u64,
    tool_call_turns: u64,
    detected_poll_only_turns: u64,
    turns_with_usage: u64,
    turns_without_usage: u64,
    usage: ApiTokenUsageSummary,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct ApiTokenUsageSummary {
    input_tokens: u64,
    cached_input_tokens: u64,
    uncached_input_tokens: u64,
    output_tokens: u64,
    reasoning_output_tokens: u64,
    total_tokens: u64,
}

#[derive(Clone, Eq, PartialEq, Serialize)]
struct ApiEventLoopArmSummary {
    turns: u64,
    generation_turns: u64,
    terminal_turns: u64,
    tool_call_turns: u64,
    model_visible_tool_calls: u64,
    model_visible_tool_sequence: Vec<String>,
    initial_model: Option<String>,
    initial_reasoning_effort: Option<String>,
    initial_reasoning_summary: Option<String>,
    initial_visible_tools: Vec<String>,
    initial_input_text_sections: Vec<ApiInputTextSectionSummary>,
    initial_generation_input_text_sections: Vec<ApiInputTextSectionSummary>,
    initial_code_mode_tools: Option<Vec<String>>,
    initial_code_mode_tool_definitions: Option<Vec<ApiCodeModeToolDefinitionSummary>>,
    detected_poll_only_turns: u64,
    max_consecutive_detected_poll_only_turns: u64,
    detected_empty_stdin_calls: u64,
    detected_poll_only_input_tokens: u64,
    detected_poll_only_cached_tokens: u64,
    detected_poll_only_output_tokens: u64,
    prompt_cache_key_stable: Option<bool>,
    previous_response_links: u64,
    full_history_replays: u64,
    full_history_replays_after_nonterminal_turn: u64,
    broken_previous_response_links: u64,
    tool_result_links: u64,
    replayed_tool_result_links: u64,
    broken_tool_result_links: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ApiCodeModeToolDefinitionSummary {
    name: String,
    ordinal: u64,
    section_bytes: u64,
    section_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ApiInputTextSectionSummary {
    item_ordinal: u64,
    content_ordinal: u64,
    role: String,
    label: String,
    text_bytes: u64,
    text_sha256: String,
}

#[derive(Default)]
struct DetectedPollingTurn {
    empty_stdin_calls: u64,
    input_tokens: u64,
    cached_tokens: u64,
    output_tokens: u64,
}

impl ApiEventLoopArmSummary {
    fn chain_invariants_equal(&self, other: &Self) -> bool {
        self.turns == other.turns
            && self.generation_turns == other.generation_turns
            && self.terminal_turns == other.terminal_turns
            && self.prompt_cache_key_stable == other.prompt_cache_key_stable
            && self.previous_response_links == other.previous_response_links
            && self.full_history_replays == other.full_history_replays
            && self.full_history_replays_after_nonterminal_turn
                == other.full_history_replays_after_nonterminal_turn
            && self.broken_previous_response_links == other.broken_previous_response_links
            && self.tool_result_links == other.tool_result_links
            && self.replayed_tool_result_links == other.replayed_tool_result_links
            && self.broken_tool_result_links == other.broken_tool_result_links
    }
}

impl ApiEventLoopTrace {
    fn unpaired_tail(&self, aligned_turns: usize) -> ApiEventLoopTailSummary {
        ApiEventLoopTailSummary::from_turns(
            self.turn_metrics.get(aligned_turns..).unwrap_or_default(),
        )
    }
}

impl ApiEventLoopTailSummary {
    fn from_turns(turns: &[ApiEventLoopTurnMetrics]) -> Self {
        let mut summary = Self {
            turns: u64::try_from(turns.len()).unwrap_or(u64::MAX),
            ..Self::default()
        };
        for turn in turns {
            if turn.generation {
                summary.generation_turns = summary.generation_turns.saturating_add(1);
            }
            if turn.tool_calls > 0 {
                summary.tool_call_turns = summary.tool_call_turns.saturating_add(1);
            }
            if turn.detected_poll_only {
                summary.detected_poll_only_turns =
                    summary.detected_poll_only_turns.saturating_add(1);
            }
            if let Some(usage) = &turn.usage {
                summary.turns_with_usage = summary.turns_with_usage.saturating_add(1);
                summary.usage.add(usage);
            } else {
                summary.turns_without_usage = summary.turns_without_usage.saturating_add(1);
            }
        }
        summary
    }
}

impl ApiTokenUsageSummary {
    const fn add(&mut self, usage: &Self) {
        self.input_tokens = self.input_tokens.saturating_add(usage.input_tokens);
        self.cached_input_tokens = self
            .cached_input_tokens
            .saturating_add(usage.cached_input_tokens);
        self.uncached_input_tokens = self
            .uncached_input_tokens
            .saturating_add(usage.uncached_input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(usage.output_tokens);
        self.reasoning_output_tokens = self
            .reasoning_output_tokens
            .saturating_add(usage.reasoning_output_tokens);
        self.total_tokens = self.total_tokens.saturating_add(usage.total_tokens);
    }
}

#[derive(Serialize)]
struct ApiEventLoopTurnComparison {
    equal: bool,
    categories: Vec<String>,
    nanocodex: Option<serde_json::Value>,
    codex: Option<serde_json::Value>,
    differences: Vec<ApiJsonDifference>,
}

#[derive(Serialize)]
struct ApiJsonDifference {
    pointer: String,
    nanocodex: ApiJsonSide,
    codex: ApiJsonSide,
}

#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum ApiJsonSide {
    Missing,
    Value { value: serde_json::Value },
}

struct ApiCaptureArtifact {
    path: PathBuf,
    summary: ApiCaptureSummary,
}

struct ApiRequestPayload {
    request_index: u64,
    phase: Option<String>,
    payload: serde_json::Value,
    sha256: String,
    response_events: Vec<serde_json::Value>,
}

struct ApiEventLoopTrace {
    turns: Vec<serde_json::Value>,
    turn_metrics: Vec<ApiEventLoopTurnMetrics>,
    summary: ApiEventLoopArmSummary,
}

struct ApiEventLoopTurnMetrics {
    generation: bool,
    tool_calls: u64,
    detected_poll_only: bool,
    usage: Option<ApiTokenUsageSummary>,
}

#[derive(Clone, Default)]
pub(super) struct DiffProgress {
    active: Option<ActiveDiffProgress>,
}

#[derive(Clone)]
struct ActiveDiffProgress {
    sender: mpsc::UnboundedSender<PendingProgressRecord>,
    started: Instant,
    api_diff: Arc<Mutex<LiveApiDiff>>,
}

struct DiffProgressRecorder {
    path: PathBuf,
    task: JoinHandle<std::io::Result<()>>,
}

struct PendingProgressRecord {
    observed_at: DateTime<Utc>,
    elapsed_ms: u64,
    arm: &'static str,
    kind: String,
    summary: Option<String>,
}

struct LaneProgressState {
    elapsed_ms: u64,
    kind: String,
    summary: Option<String>,
}

#[derive(Default)]
struct LiveApiDiff {
    arms: BTreeMap<&'static str, LiveApiArm>,
    compared_requests: usize,
    compared_responses: usize,
}

#[derive(Default)]
struct LiveApiArm {
    requests: Vec<ApiRequestPayload>,
    source_offsets: BTreeMap<u64, usize>,
    active_offset: Option<usize>,
}

struct LiveApiNotice {
    kind: &'static str,
    summary: String,
}

#[derive(Serialize)]
struct ProgressRecord {
    schema_version: u32,
    sequence: u64,
    observed_at: DateTime<Utc>,
    elapsed_ms: u64,
    arm: &'static str,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
}

impl DiffProgress {
    async fn start(
        path: PathBuf,
        started: Instant,
    ) -> InternalResult<(Self, DiffProgressRecorder)> {
        Self::start_with_heartbeat(path, started, PROGRESS_HEARTBEAT_INTERVAL).await
    }

    async fn start_with_heartbeat(
        path: PathBuf,
        started: Instant,
        heartbeat_interval: Duration,
    ) -> InternalResult<(Self, DiffProgressRecorder)> {
        let mut output = tokio::fs::File::create(&path)
            .await
            .wrap_err_with(|| format!("failed to create live progress log {}", path.display()))?;
        let (sender, mut receiver) = mpsc::unbounded_channel::<PendingProgressRecord>();
        let task = tokio::spawn(async move {
            let mut sequence = 0_u64;
            let mut lanes = BTreeMap::new();
            let mut heartbeat = tokio::time::interval(heartbeat_interval);
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            heartbeat.tick().await;
            loop {
                tokio::select! {
                    biased;
                    pending = receiver.recv() => {
                        let Some(pending) = pending else {
                            break;
                        };
                        if matches!(pending.arm, "nanocodex" | "codex") {
                            lanes.insert(
                                pending.arm,
                                LaneProgressState {
                                    elapsed_ms: pending.elapsed_ms,
                                    kind: pending.kind.clone(),
                                    summary: pending.summary.clone(),
                                },
                            );
                        }
                        sequence = sequence.saturating_add(1);
                        write_progress_record(&mut output, sequence, pending).await?;
                    }
                    _ = heartbeat.tick(), if heartbeat_needed(&lanes) => {
                        let elapsed_ms = elapsed_ms(started);
                        sequence = sequence.saturating_add(1);
                        write_progress_record(
                            &mut output,
                            sequence,
                            PendingProgressRecord {
                                observed_at: Utc::now(),
                                elapsed_ms,
                                arm: "runner",
                                kind: "heartbeat".to_owned(),
                                summary: Some(heartbeat_summary(&lanes, elapsed_ms)),
                            },
                        )
                        .await?;
                    }
                }
            }
            output.sync_all().await
        });
        Ok((
            Self {
                active: Some(ActiveDiffProgress {
                    sender,
                    started,
                    api_diff: Arc::new(Mutex::new(LiveApiDiff::default())),
                }),
            },
            DiffProgressRecorder { path, task },
        ))
    }

    pub(super) fn emit(
        &self,
        arm: &'static str,
        kind: impl Into<String>,
        summary: impl Into<String>,
    ) {
        let Some(active) = &self.active else {
            return;
        };
        let summary = summary.into();
        let _ = active.sender.send(PendingProgressRecord {
            observed_at: Utc::now(),
            elapsed_ms: elapsed_ms(active.started),
            arm,
            kind: kind.into(),
            summary: (!summary.is_empty()).then_some(summary),
        });
    }

    fn observe_nanocodex(&self, event: &nanocodex_agent::events::AgentEvent) {
        let kind = serde_json::to_value(event.kind)
            .ok()
            .and_then(|kind| kind.as_str().map(str::to_owned))
            .unwrap_or_else(|| format!("{:?}", event.kind));
        let payload = serde_json::from_str(event.payload.get()).unwrap_or_default();
        self.emit(
            "nanocodex",
            kind,
            summarize_nanocodex(&event.kind, &payload),
        );
    }

    fn observe_evaluator(&self, arm: &'static str, event: &EvalEventKind) {
        match event {
            EvalEventKind::VerifierStarted => {
                self.emit(arm, "verifier.started", "canonical verifier");
            }
            EvalEventKind::VerifierOutput { stdout, stderr } => {
                self.emit(
                    arm,
                    "verifier.output",
                    format!(
                        "{} stdout bytes · {} stderr bytes",
                        stdout.len(),
                        stderr.len()
                    ),
                );
            }
            EvalEventKind::VerifierCompleted(result) => {
                let rewards = result
                    .rewards
                    .iter()
                    .map(|(name, reward)| format!("{name}={reward}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                self.emit(
                    arm,
                    "verifier.completed",
                    format!("exit {} · {rewards}", result.exit_code),
                );
            }
            EvalEventKind::AttemptStarted { .. }
            | EvalEventKind::Agent(_)
            | EvalEventKind::Completed(_)
            | EvalEventKind::Failed(_) => {}
        }
    }

    fn observe_nanocodex_api(&self, payload: &serde_json::Value) {
        let direction = payload
            .get("direction")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let phase = payload
            .get("phase")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let request_index = payload
            .get("model_call_index")
            .and_then(serde_json::Value::as_u64);
        let event = payload.get("event").unwrap_or(&serde_json::Value::Null);
        self.observe_live_api("nanocodex", direction, phase, request_index, event);
        self.emit_api_boundary("nanocodex", direction, phase, request_index, event);
    }

    pub(super) fn observe_api_exchange(&self, arm: &'static str, exchange: &serde_json::Value) {
        let direction = exchange
            .get("direction")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let phase = exchange
            .get("phase")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let request_index = exchange
            .get("request_index")
            .and_then(serde_json::Value::as_u64);
        let payload = exchange.get("payload").unwrap_or(&serde_json::Value::Null);
        let event = payload
            .get("event")
            .or_else(|| payload.get("text"))
            .unwrap_or(payload);
        for event in record_api_events(exchange) {
            self.observe_live_api(arm, direction, phase, request_index, &event);
        }
        self.emit_api_boundary(arm, direction, phase, request_index, event);
    }

    fn observe_live_api(
        &self,
        arm: &'static str,
        direction: &str,
        phase: &str,
        request_index: Option<u64>,
        event: &serde_json::Value,
    ) {
        let Some(active) = &self.active else {
            return;
        };
        let notices = {
            let Ok(mut diff) = active.api_diff.lock() else {
                return;
            };
            diff.observe(arm, direction, phase, request_index, event)
        };
        for notice in notices {
            self.emit("runner", notice.kind, notice.summary);
        }
    }

    fn emit_api_boundary(
        &self,
        arm: &'static str,
        direction: &str,
        phase: &str,
        request_index: Option<u64>,
        event: &serde_json::Value,
    ) {
        let event_type = api_event_type(event);
        let terminal = matches!(
            event_type.as_deref(),
            Some("response.completed" | "response.failed" | "error")
        );
        if direction != "outbound" && !terminal {
            return;
        }
        let kind = if direction == "outbound" {
            "api.request".to_owned()
        } else {
            format!("api.{}", event_type.as_deref().unwrap_or("response"))
        };
        self.emit(
            arm,
            kind,
            summarize_api_boundary(phase, request_index, event_type.as_deref(), event),
        );
    }

    pub(super) fn observe_codex(&self, event: &serde_json::Value) {
        let kind = event
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        self.emit("codex", kind, summarize_codex(event));
    }

    pub(super) fn observe_codex_diagnostic(&self, diagnostic: &[u8]) {
        self.emit(
            "codex",
            "diagnostic",
            preview(&String::from_utf8_lossy(diagnostic)),
        );
    }
}

impl LiveApiDiff {
    fn observe(
        &mut self,
        arm: &'static str,
        direction: &str,
        phase: &str,
        request_index: Option<u64>,
        event: &serde_json::Value,
    ) -> Vec<LiveApiNotice> {
        if !matches!(arm, "nanocodex" | "codex") {
            return Vec::new();
        }
        self.arms
            .entry(arm)
            .or_default()
            .observe(direction, phase, request_index, event);
        let mut notices = Vec::new();
        let (Some(nanocodex), Some(codex)) = (self.arms.get("nanocodex"), self.arms.get("codex"))
        else {
            return notices;
        };
        let nanocodex_trace = build_event_loop_trace(&nanocodex.requests);
        let codex_trace = build_event_loop_trace(&codex.requests);
        let aligned = nanocodex_trace.turns.len().min(codex_trace.turns.len());
        while self.compared_requests < aligned {
            let offset = self.compared_requests;
            let request_number = offset.saturating_add(1);
            let nanocodex_request = serde_json::json!({
                "phase": nanocodex_trace.turns[offset].get("phase"),
                "request": nanocodex_trace.turns[offset].get("request"),
            });
            let codex_request = serde_json::json!({
                "phase": codex_trace.turns[offset].get("phase"),
                "request": codex_trace.turns[offset].get("request"),
            });
            let mut differences = Vec::new();
            diff_json(
                "",
                Some(&nanocodex_request),
                Some(&codex_request),
                &mut differences,
            );
            notices.push(live_api_notice(request_number, "request", &differences));
            self.compared_requests = self.compared_requests.saturating_add(1);
        }
        while self.compared_responses < aligned {
            let offset = self.compared_responses;
            if !live_api_turn_terminal(&nanocodex.requests[offset])
                || !live_api_turn_terminal(&codex.requests[offset])
            {
                break;
            }
            let request_number = offset.saturating_add(1);
            let nanocodex_response = serde_json::json!({
                "response": nanocodex_trace.turns[offset].get("response"),
            });
            let codex_response = serde_json::json!({
                "response": codex_trace.turns[offset].get("response"),
            });
            let mut differences = Vec::new();
            diff_json(
                "",
                Some(&nanocodex_response),
                Some(&codex_response),
                &mut differences,
            );
            notices.push(live_api_notice(request_number, "response", &differences));
            let nanocodex_polling =
                detected_polling_turn(&nanocodex.requests[offset].response_events);
            let codex_polling = detected_polling_turn(&codex.requests[offset].response_events);
            if nanocodex_polling.is_some() || codex_polling.is_some() {
                let matches = nanocodex_polling.is_some() == codex_polling.is_some();
                notices.push(LiveApiNotice {
                    kind: if matches {
                        "api.polling.match"
                    } else {
                        "api.polling.diff"
                    },
                    summary: format!(
                        "turn {request_number} poll-only response · nanocodex={} · codex={}",
                        nanocodex_polling
                            .as_ref()
                            .map_or(0, |polling| { polling.empty_stdin_calls }),
                        codex_polling
                            .as_ref()
                            .map_or(0, |polling| polling.empty_stdin_calls),
                    ),
                });
            }
            self.compared_responses = self.compared_responses.saturating_add(1);
        }
        notices
    }
}

impl LiveApiArm {
    fn observe(
        &mut self,
        direction: &str,
        phase: &str,
        request_index: Option<u64>,
        event: &serde_json::Value,
    ) {
        if direction == "outbound" && api_event_type(event).as_deref() == Some("response.create") {
            let offset = self.requests.len();
            let encoded = serde_json::to_vec(event).unwrap_or_default();
            self.requests.push(ApiRequestPayload {
                request_index: request_index
                    .unwrap_or_else(|| u64::try_from(offset).unwrap_or(u64::MAX).saturating_add(1)),
                phase: Some(phase.to_owned()),
                payload: event.clone(),
                sha256: hex::encode(Sha256::digest(encoded)),
                response_events: Vec::new(),
            });
            if let Some(request_index) = request_index {
                self.source_offsets.insert(request_index, offset);
            }
            self.active_offset = Some(offset);
        } else if direction == "inbound"
            && let Some(offset) = request_index
                .and_then(|request_index| self.source_offsets.get(&request_index).copied())
                .or(self.active_offset)
            && let Some(request) = self.requests.get_mut(offset)
        {
            request.response_events.push(event.clone());
        }
    }
}

fn live_api_turn_terminal(request: &ApiRequestPayload) -> bool {
    request
        .response_events
        .iter()
        .filter_map(api_event_type)
        .any(|kind| is_terminal_api_event(&kind))
}

fn live_api_notice(
    request_number: usize,
    stage: &'static str,
    differences: &[ApiJsonDifference],
) -> LiveApiNotice {
    if differences.is_empty() {
        return LiveApiNotice {
            kind: if stage == "request" {
                "api.request.match"
            } else {
                "api.response.match"
            },
            summary: format!("turn {request_number} {stage} invariants match"),
        };
    }
    let categories = event_loop_difference_categories(differences);
    let pointer = differences
        .first()
        .map_or("", |difference| difference.pointer.as_str());
    LiveApiNotice {
        kind: if stage == "request" {
            "api.request.diff"
        } else {
            "api.response.diff"
        },
        summary: format!(
            "turn {request_number} {stage} drift · {} · {pointer}",
            categories.join(",")
        ),
    }
}

impl DiffProgressRecorder {
    async fn finish(self, progress: DiffProgress) -> InternalResult<()> {
        drop(progress);
        self.task
            .await
            .wrap_err("live progress recorder task failed")?
            .wrap_err_with(|| format!("failed to write live progress log {}", self.path.display()))
    }
}

async fn write_progress_record(
    output: &mut tokio::fs::File,
    sequence: u64,
    pending: PendingProgressRecord,
) -> std::io::Result<()> {
    let record = ProgressRecord {
        schema_version: PROGRESS_SCHEMA_VERSION,
        sequence,
        observed_at: pending.observed_at,
        elapsed_ms: pending.elapsed_ms,
        arm: pending.arm,
        kind: pending.kind,
        summary: pending.summary,
    };
    let mut encoded = serde_json::to_vec(&record).map_err(std::io::Error::other)?;
    encoded.push(b'\n');
    output.write_all(&encoded).await?;
    output.flush().await?;
    tracing::info!(
        target: "nanocodex_eval::diff_progress",
        elapsed_ms = record.elapsed_ms,
        comparison_arm = record.arm,
        event_kind = %record.kind,
        summary = record.summary.as_deref().unwrap_or(""),
        "differential progress"
    );
    Ok(())
}

fn heartbeat_needed(lanes: &BTreeMap<&'static str, LaneProgressState>) -> bool {
    !lanes.is_empty()
        && ["nanocodex", "codex"].into_iter().any(|arm| {
            lanes.get(arm).is_none_or(|lane| {
                !matches!(lane.kind.as_str(), "attempt.completed" | "attempt.failed")
            })
        })
}

fn heartbeat_summary(lanes: &BTreeMap<&'static str, LaneProgressState>, elapsed_ms: u64) -> String {
    ["nanocodex", "codex"]
        .into_iter()
        .map(|arm| {
            lanes.get(arm).map_or_else(
                || format!("{arm}: not started"),
                |lane| {
                    let state = lane.summary.as_deref().map_or_else(
                        || lane.kind.clone(),
                        |summary| {
                            format!(
                                "{} ({})",
                                lane.kind,
                                preview_chars(summary, PROGRESS_HEARTBEAT_SUMMARY_CHARS)
                            )
                        },
                    );
                    format!(
                        "{arm}: {state} for {}",
                        format_duration(elapsed_ms.saturating_sub(lane.elapsed_ms))
                    )
                },
            )
        })
        .collect::<Vec<_>>()
        .join(" · ")
}

fn format_duration(duration_ms: u64) -> String {
    if duration_ms < 1_000 {
        format!("{duration_ms}ms")
    } else {
        format!("{:.1}s", duration_ms as f64 / 1_000.0)
    }
}

fn summarize_nanocodex(kind: &AgentEventKind, payload: &serde_json::Value) -> String {
    match kind {
        AgentEventKind::AssistantMessage => value_preview(payload, "text"),
        AgentEventKind::ToolCall => join_summary([
            value_string(payload, "tool"),
            value_preview_option(payload, "arguments"),
        ]),
        AgentEventKind::ToolResult => join_summary([
            value_string(payload, "tool"),
            value_string(payload, "status"),
            value_preview_option(payload, "result"),
        ]),
        AgentEventKind::ModelCallStarted
        | AgentEventKind::ModelCallCompleted
        | AgentEventKind::ModelCallFailed => join_summary([
            labeled_value(payload, "call", "call_index"),
            value_string(payload, "status"),
            labeled_value(payload, "tools", "tool_calls"),
            value_preview_option(payload, "error"),
        ]),
        AgentEventKind::RunError | AgentEventKind::RunFailed => value_preview(payload, "message"),
        AgentEventKind::RunCompleted => join_summary([
            labeled_value(payload, "model calls", "model_calls"),
            labeled_value(payload, "tools", "tool_calls"),
        ]),
        _ => String::new(),
    }
}

fn summarize_codex(event: &serde_json::Value) -> String {
    let kind = event
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    match kind {
        "thread.started" => labeled_value(event, "thread", "thread_id").unwrap_or_default(),
        "turn.completed" => event
            .get("usage")
            .map(|usage| format!("usage {}", preview_json(usage)))
            .unwrap_or_default(),
        "turn.failed" => event.get("error").map(preview_json).unwrap_or_default(),
        "item.started" | "item.updated" | "item.completed" => {
            summarize_codex_item(event.get("item"))
        }
        _ => String::new(),
    }
}

fn summarize_codex_item(item: Option<&serde_json::Value>) -> String {
    let Some(item) = item else {
        return String::new();
    };
    let item_kind = item
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown item");
    let detail = match item_kind {
        "agent_message" | "reasoning" => value_preview(item, "text"),
        "command_execution" => join_summary([
            value_preview_option(item, "command"),
            labeled_value(item, "exit", "exit_code"),
            value_string(item, "status"),
        ]),
        "file_change" => join_summary([
            item.get("changes").map(preview_json),
            value_string(item, "status"),
        ]),
        "mcp_tool_call" => join_summary([
            value_string(item, "server"),
            value_string(item, "tool"),
            value_preview_option(item, "arguments"),
            value_string(item, "status"),
        ]),
        "web_search" => join_summary([
            value_preview_option(item, "query"),
            value_string(item, "status"),
        ]),
        _ => value_string(item, "status").unwrap_or_default(),
    };
    join_summary([
        Some(item_kind.to_owned()),
        (!detail.is_empty()).then_some(detail),
    ])
}

fn api_event_type(event: &serde_json::Value) -> Option<String> {
    if let Some(event_type) = event.get("type").and_then(serde_json::Value::as_str) {
        return Some(event_type.to_owned());
    }
    let text = event.as_str()?;
    for line in text.lines() {
        let data = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
        if let Ok(event) = serde_json::from_str::<serde_json::Value>(data)
            && let Some(event_type) = event.get("type").and_then(serde_json::Value::as_str)
        {
            return Some(event_type.to_owned());
        }
    }
    [
        "response.completed",
        "response.failed",
        "response.created",
        "error",
    ]
    .into_iter()
    .find(|event_type| text.contains(event_type))
    .map(str::to_owned)
}

fn summarize_api_boundary(
    phase: &str,
    request_index: Option<u64>,
    event_type: Option<&str>,
    event: &serde_json::Value,
) -> String {
    let request = request_index.map(|index| format!("request {index}"));
    let event_type = event_type.map(str::to_owned);
    let model = value_string(event, "model").map(|model| format!("model {model}"));
    let input = event
        .get("input")
        .and_then(serde_json::Value::as_array)
        .map(|input| format!("input {}", input.len()));
    let tools = event
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .map(|tools| format!("tools {}", tools.len()));
    let previous = event.get("previous_response_id").map(|previous| {
        if previous.is_null() {
            "previous none".to_owned()
        } else {
            "previous set".to_owned()
        }
    });
    let usage = event
        .pointer("/response/usage")
        .or_else(|| event.get("usage"))
        .map(|usage| format!("usage {}", preview_json(usage)));
    let raw = event.as_str().map(preview).filter(|text| !text.is_empty());
    join_summary([
        request,
        Some(phase.to_owned()),
        event_type,
        model,
        input,
        tools,
        previous,
        usage,
        raw,
    ])
}

fn labeled_value(value: &serde_json::Value, label: &str, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(|value| (!value.is_null()).then(|| format!("{label} {}", preview_json(value))))
}

fn value_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(|value| {
        value
            .as_str()
            .map(str::to_owned)
            .or_else(|| (!value.is_null()).then(|| preview_json(value)))
    })
}

fn value_preview(value: &serde_json::Value, key: &str) -> String {
    value_preview_option(value, key).unwrap_or_default()
}

fn value_preview_option(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(|value| {
        if value.is_null() {
            None
        } else if let Some(text) = value.as_str() {
            Some(preview(text))
        } else {
            Some(preview_json(value))
        }
    })
}

fn preview_json(value: &serde_json::Value) -> String {
    preview(&value.to_string())
}

fn preview(text: &str) -> String {
    preview_chars(text, PROGRESS_SUMMARY_CHARS)
}

fn preview_chars(text: &str, limit: usize) -> String {
    let mut normalized = String::with_capacity(text.len().min(limit));
    let mut whitespace = false;
    let mut truncated = false;
    let mut characters = 0_usize;
    for character in text.chars() {
        if character.is_whitespace() {
            whitespace = true;
            continue;
        }
        if whitespace && !normalized.is_empty() {
            if characters >= limit {
                truncated = true;
                break;
            }
            normalized.push(' ');
            characters = characters.saturating_add(1);
        }
        whitespace = false;
        if characters >= limit {
            truncated = true;
            break;
        }
        normalized.push(character);
        characters = characters.saturating_add(1);
    }
    if truncated {
        normalized.push('…');
    }
    normalized
}

fn join_summary<const N: usize>(parts: [Option<String>; N]) -> String {
    parts
        .into_iter()
        .flatten()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" · ")
}

struct DiffVmResources {
    environment: VmEnvironment,
    nanocodex: VmBackend,
    codex: VmBackend,
    codex_ca_bundle: Option<DiffCodexCaBundle>,
}

async fn prepare_diff_vm_resources(
    task: &Task,
    comparison_directory: &Path,
    vm: &VmResources,
    web_search: bool,
    codex_binary: &Path,
) -> InternalResult<DiffVmResources> {
    let codex_share_root = comparison_directory.join("codex-release");
    fs::create_dir(&codex_share_root)?;
    let staged_codex = codex_share_root.join("codex");
    reflink_or_sparse_copy(codex_binary, &staged_codex)?;
    fs::set_permissions(&staged_codex, fs::Permissions::from_mode(0o755))?;
    let mut header = [0_u8; 20];
    fs::File::open(&staged_codex)?.read_exact(&mut header)?;
    validate_vm_guest_elf(&header, &staged_codex)?;
    let codex_ca_bundle = resolve_diff_codex_ca_source()?
        .as_ref()
        .map(|source| stage_diff_codex_ca_bundle(source, &codex_share_root))
        .transpose()?;
    let environment = vm.environment(task).cloned().ok_or_else(|| {
        diff_error!(
            "VM diff did not prepare the requested task root {}",
            task.root().display()
        )
    })?;
    let nanocodex = vm
        .backend(
            VmBackend::builder()
                .retain_passed_rootfs(true)
                .web_search(web_search),
        )
        .await?;
    let codex = vm
        .backend(
            VmBackend::builder()
                .retain_passed_rootfs(true)
                .web_search(web_search)
                .shared_directory(SharedDirectory::read_only(
                    DIFF_CODEX_SHARE_TAG,
                    codex_share_root,
                )),
        )
        .await?;
    Ok(DiffVmResources {
        environment,
        nanocodex,
        codex,
        codex_ca_bundle,
    })
}

impl DiffVmResources {
    fn nanocodex_backend(&self) -> VmBackend {
        self.nanocodex.clone()
    }

    fn codex_backend(&self) -> VmBackend {
        self.codex.clone()
    }

    fn codex_attempt(
        &self,
        runtime: VmAttempt,
        attempt: EvalAttempt<'_>,
        codex: CodexExec,
        auth: CodexAuth,
        version: Arc<OnceLock<String>>,
        progress: DiffProgress,
    ) -> InternalResult<AttemptAgent, VmAttemptError> {
        let model_catalog_override = codex
            .code_mode_only_model()
            .map(ResponsesModelCatalogOverride::code_mode_only);
        let session = runtime.session_handle()?;
        let runner = DiffVmCodexRunner::new(
            session,
            attempt,
            &self.environment,
            auth,
            self.codex_ca_bundle,
            version,
            progress,
        )?
        .model_catalog_override(model_catalog_override);
        let api_base_url = runner.api_base_url().to_owned();
        let runner = Arc::new(runner);
        let readiness = Arc::clone(&runner);
        Ok(runtime
            .codex(codex.api_base_url(api_base_url).command_runner(runner))
            .ready(async move { readiness.prepare().await }))
    }
}

#[derive(Clone, Copy)]
struct DiffCodexCaBundle {
    guest_environment: &'static str,
}

struct DiffCodexCaSource {
    path: PathBuf,
    source_environment: &'static str,
    guest_environment: &'static str,
}

fn resolve_diff_codex_ca_source() -> InternalResult<Option<DiffCodexCaSource>, io::Error> {
    for (source_environment, guest_environment) in [
        (
            DIFF_CODEX_CA_CERTIFICATE_ENVIRONMENT,
            DIFF_CODEX_CA_CERTIFICATE_ENVIRONMENT,
        ),
        (
            DIFF_CODEX_SSL_CERT_FILE_ENVIRONMENT,
            DIFF_CODEX_SSL_CERT_FILE_ENVIRONMENT,
        ),
        (
            DIFF_CODEX_NIX_SSL_CERT_FILE_ENVIRONMENT,
            DIFF_CODEX_SSL_CERT_FILE_ENVIRONMENT,
        ),
    ] {
        let Some(path) = std::env::var_os(source_environment).filter(|value| !value.is_empty())
        else {
            continue;
        };
        return Ok(Some(DiffCodexCaSource {
            path: fs::canonicalize(PathBuf::from(path))?,
            source_environment,
            guest_environment,
        }));
    }
    for path in [
        Path::new("/etc/ssl/certs/ca-certificates.crt"),
        Path::new("/etc/ssl/cert.pem"),
    ] {
        if path.is_file() {
            return Ok(Some(DiffCodexCaSource {
                path: fs::canonicalize(path)?,
                source_environment: "host_system",
                guest_environment: DIFF_CODEX_SSL_CERT_FILE_ENVIRONMENT,
            }));
        }
    }
    Ok(None)
}

fn stage_diff_codex_ca_bundle(
    source: &DiffCodexCaSource,
    codex_share_root: &Path,
) -> InternalResult<DiffCodexCaBundle, io::Error> {
    let staged = codex_share_root.join(DIFF_CODEX_CA_BUNDLE_FILENAME);
    reflink_or_sparse_copy(&source.path, &staged)?;
    if staged.metadata()?.len() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Codex CA bundle selected by {} is empty: {}",
                source.source_environment,
                source.path.display()
            ),
        ));
    }
    fs::set_permissions(&staged, fs::Permissions::from_mode(0o444))?;
    info!(
        target: "nanocodex_eval",
        source_environment = source.source_environment,
        source_path = %source.path.display(),
        staged_path = %staged.display(),
        "staged the host CA bundle for the pinned guest Codex release"
    );
    Ok(DiffCodexCaBundle {
        guest_environment: source.guest_environment,
    })
}

enum DiffVmCodexAuth {
    ApiKey(Arc<str>),
    AuthFile {
        contents: Vec<u8>,
        cloud_config_cache: Option<Vec<u8>>,
    },
}

struct DiffVmCodexRunner {
    session: VmToolSessionHandle,
    workspace: String,
    environment: Vec<(String, String)>,
    auth_file: Option<Vec<u8>>,
    cloud_config_cache: Option<Vec<u8>>,
    capture_upstream: &'static str,
    model_catalog_override: Option<ResponsesModelCatalogOverride>,
    capture_listener: Mutex<Option<TcpListener>>,
    capture_base_url: String,
    api_exchanges: PathBuf,
    version: Arc<OnceLock<String>>,
    progress: DiffProgress,
}

impl DiffVmCodexRunner {
    fn new(
        session: VmToolSessionHandle,
        attempt: EvalAttempt<'_>,
        environment: &VmEnvironment,
        auth: CodexAuth,
        ca_bundle: Option<DiffCodexCaBundle>,
        version: Arc<OnceLock<String>>,
        progress: DiffProgress,
    ) -> InternalResult<Self, VmAttemptError> {
        let artifact_directory = attempt.directory().join("agent");
        fs::create_dir_all(&artifact_directory)?;
        let auth = match auth.kind {
            CodexAuthKind::ApiKey(api_key) => DiffVmCodexAuth::ApiKey(api_key),
            CodexAuthKind::AuthFile(path) => {
                let contents = fs::read(&path)?;
                let cloud_config_cache = read_optional_codex_cloud_config_cache(&path)?;
                DiffVmCodexAuth::AuthFile {
                    contents,
                    cloud_config_cache,
                }
            }
        };
        let mut command_environment = environment.guest_environment(attempt.task());
        command_environment.insert("CODEX_HOME".to_owned(), DIFF_CODEX_HOME.to_owned());
        if let Some(ca_bundle) = ca_bundle {
            command_environment.insert(
                ca_bundle.guest_environment.to_owned(),
                DIFF_CODEX_CA_BUNDLE_FILE.to_owned(),
            );
        }
        let (auth_file, cloud_config_cache, capture_upstream) = match auth {
            DiffVmCodexAuth::ApiKey(api_key) => {
                command_environment.insert("OPENAI_API_KEY".to_owned(), api_key.to_string());
                (None, None, DIFF_CAPTURE_PROXY_API_UPSTREAM)
            }
            DiffVmCodexAuth::AuthFile {
                contents,
                cloud_config_cache,
            } => {
                command_environment.remove("OPENAI_API_KEY");
                (
                    Some(contents),
                    cloud_config_cache,
                    DIFF_CAPTURE_PROXY_CHATGPT_UPSTREAM,
                )
            }
        };
        let capture_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let capture_port = capture_listener.local_addr()?.port();
        let capture_base_url = format!("http://{DIFF_CAPTURE_PROXY_VM_HOST}:{capture_port}");
        Ok(Self {
            session,
            workspace: environment.workspace().to_owned(),
            environment: command_environment.into_iter().collect(),
            auth_file,
            cloud_config_cache,
            capture_upstream,
            model_catalog_override: None,
            capture_listener: Mutex::new(Some(capture_listener)),
            capture_base_url,
            api_exchanges: artifact_directory.join(DIFF_API_EXCHANGES_FILENAME),
            version,
            progress,
        })
    }

    fn model_catalog_override(
        mut self,
        model_catalog_override: Option<ResponsesModelCatalogOverride>,
    ) -> Self {
        self.model_catalog_override = model_catalog_override;
        self
    }

    fn api_base_url(&self) -> &str {
        &self.capture_base_url
    }

    async fn prepare(&self) -> InternalResult<(), VmAttemptError> {
        self.session.ready().await?;
        self.session
            .create_directory(DIFF_CODEX_SHARE_MOUNT, 0o755, None)
            .await?;
        let mount = self
            .session
            .command(
                VmCommand::new("/bin/mount")
                    .arg("-t")
                    .arg("virtiofs")
                    .arg("-o")
                    .arg("ro")
                    .arg(DIFF_CODEX_SHARE_TAG)
                    .arg(DIFF_CODEX_SHARE_MOUNT)
                    .environment(self.environment.clone())
                    .timeout(DIFF_CODEX_VERSION_TIMEOUT),
            )
            .await?;
        if mount.exit_code != 0 {
            return Err(io::Error::other(format!(
                "failed to mount the pinned Codex release in the guest (exit {}): {}",
                mount.exit_code,
                String::from_utf8_lossy(&mount.stderr).trim()
            ))
            .into());
        }
        self.session
            .create_directory(DIFF_CODEX_HOME, 0o700, None)
            .await?;
        if let Some(auth_file) = &self.auth_file {
            self.session
                .write_file(DIFF_CODEX_AUTH_FILE, auth_file.clone(), 0o600)
                .await?;
        }
        if let Some(cloud_config_cache) = &self.cloud_config_cache {
            self.session
                .write_file(
                    DIFF_CODEX_CLOUD_CONFIG_CACHE_FILE,
                    cloud_config_cache.clone(),
                    0o600,
                )
                .await?;
        }
        let version = self
            .session
            .command(
                VmCommand::new(DIFF_CODEX_GUEST_BINARY)
                    .arg("--version")
                    .current_directory(&self.workspace)
                    .environment(self.environment.clone())
                    .timeout(DIFF_CODEX_VERSION_TIMEOUT),
            )
            .await?;
        if version.exit_code != 0 {
            return Err(io::Error::other(format!(
                "pinned guest Codex --version exited {}: {}",
                version.exit_code,
                String::from_utf8_lossy(&version.stderr).trim()
            ))
            .into());
        }
        let version = String::from_utf8(version.stdout)
            .map_err(io::Error::other)?
            .trim()
            .to_owned();
        if version.is_empty() {
            return Err(
                io::Error::other("pinned guest Codex --version returned no version").into(),
            );
        }
        if let Some(existing) = self.version.get() {
            if existing != &version {
                return Err(io::Error::other(format!(
                    "pinned guest Codex version changed from {existing} to {version}"
                ))
                .into());
            }
        } else {
            self.version
                .set(version)
                .map_err(|_| io::Error::other("failed to retain pinned guest Codex version"))?;
        }
        Ok(())
    }

    async fn start_capture_proxy(
        &self,
    ) -> InternalResult<ResponsesCaptureProxy, CodexCommandRunnerError> {
        let listener = {
            let mut listener = self.capture_listener.lock().map_err(|_| {
                CodexCommandRunnerError::new("Responses capture listener lock was poisoned")
            })?;
            listener.take().ok_or_else(|| {
                CodexCommandRunnerError::new(
                    "Responses capture proxy was already started for this attempt",
                )
            })?
        };
        let proxy = ResponsesCaptureProxy::start(
            listener,
            ResponsesCaptureProxyConfig {
                upstream: self.capture_upstream.to_owned(),
                output: self.api_exchanges.clone(),
                model_catalog_override: self.model_catalog_override.clone(),
            },
        )
        .await
        .map_err(|error| CodexCommandRunnerError::new(error.to_string()))?;
        self.progress.emit(
            "codex",
            "api.capture.started",
            format!("{} → {}", self.capture_base_url, self.capture_upstream),
        );
        Ok(proxy)
    }

    async fn stop_capture_proxy(
        &self,
        proxy: ResponsesCaptureProxy,
    ) -> InternalResult<(), CodexCommandRunnerError> {
        match tokio::time::timeout(DIFF_CAPTURE_PROXY_STOP_TIMEOUT, proxy.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(CodexCommandRunnerError::new(error.to_string())),
            Err(_) => {
                return Err(CodexCommandRunnerError::new(format!(
                    "Responses capture proxy did not stop within {:?}",
                    DIFF_CAPTURE_PROXY_STOP_TIMEOUT
                )));
            }
        }
        self.progress.emit(
            "codex",
            "api.capture.completed",
            self.api_exchanges.display().to_string(),
        );
        Ok(())
    }
}

fn read_optional_codex_cloud_config_cache(
    auth_file: &Path,
) -> InternalResult<Option<Vec<u8>>, io::Error> {
    let Some(codex_home) = auth_file.parent() else {
        return Ok(None);
    };
    let cache = codex_home.join(DIFF_CODEX_CLOUD_CONFIG_CACHE_FILENAME);
    match fs::read(cache) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

impl CodexCommandRunner for DiffVmCodexRunner {
    fn run<'a>(
        &'a self,
        arguments: Vec<String>,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<Output = InternalResult<CodexCommandOutput, CodexCommandRunnerError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let capture_proxy = self.start_capture_proxy().await?;
            let mut command = VmCommand::new(DIFF_CODEX_GUEST_BINARY)
                .current_directory(&self.workspace)
                .environment(self.environment.clone())
                .timeout(timeout)
                .max_output_bytes(DIFF_CODEX_OUTPUT_BYTES)
                .mirror_output(DIFF_CODEX_LIVE_STDOUT_FILE, DIFF_CODEX_LIVE_STDERR_FILE);
            for argument in arguments {
                command = command.arg(argument);
            }
            let session = self.session.clone();
            let command = async move { session.command(command).await };
            tokio::pin!(command);
            let mut progress =
                DiffCodexProgress::new(self.progress.clone(), self.api_exchanges.clone());
            let mut progress_interval = tokio::time::interval_at(
                tokio::time::Instant::now() + DIFF_CODEX_PROGRESS_POLL,
                DIFF_CODEX_PROGRESS_POLL,
            );
            progress_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let result = loop {
                tokio::select! {
                    result = &mut command => break result,
                    _ = progress_interval.tick() => {
                        progress.poll(&self.session).await;
                    }
                }
            };
            progress.poll_api(true).await;
            self.stop_capture_proxy(capture_proxy).await?;
            match result {
                Ok(output) => Ok(CodexCommandOutput {
                    status: CodexCommandStatus::Exited(output.exit_code),
                    stdout: {
                        progress.observe_stdout(&output.stdout, true);
                        output.stdout
                    },
                    stderr: {
                        progress.observe_stderr(&output.stderr, true);
                        output.stderr
                    },
                }),
                Err(VmToolSessionError::GuestTimeout { output, .. }) => Ok(CodexCommandOutput {
                    status: CodexCommandStatus::TimedOut,
                    stdout: {
                        progress.observe_stdout(&output.stdout, true);
                        output.stdout
                    },
                    stderr: {
                        progress.observe_stderr(&output.stderr, true);
                        output.stderr
                    },
                }),
                Err(error) => Err(CodexCommandRunnerError::new(error.to_string())),
            }
        })
    }
}

struct DiffCodexProgress {
    reporter: DiffProgress,
    api_exchanges: PathBuf,
    api_offset: u64,
    stdout_offset: usize,
    stderr_offset: usize,
    api_read_failed: bool,
    stdout_read_failed: bool,
    stderr_read_failed: bool,
}

impl DiffCodexProgress {
    const fn new(reporter: DiffProgress, api_exchanges: PathBuf) -> Self {
        Self {
            reporter,
            api_exchanges,
            api_offset: 0,
            stdout_offset: 0,
            stderr_offset: 0,
            api_read_failed: false,
            stdout_read_failed: false,
            stderr_read_failed: false,
        }
    }

    async fn poll(&mut self, session: &VmToolSessionHandle) {
        self.poll_api(false).await;
        if !self.stdout_read_failed {
            match session.read_file(DIFF_CODEX_LIVE_STDOUT_FILE).await {
                Ok(contents) => self.observe_stdout(&contents, false),
                Err(error) if progress_file_is_not_ready(&error) => {}
                Err(error) => {
                    self.stdout_read_failed = true;
                    warn!(
                        target: "nanocodex_eval",
                        error = %error,
                        "stopped polling the live stock-Codex stdout mirror"
                    );
                }
            }
        }
        if !self.stderr_read_failed {
            match session.read_file(DIFF_CODEX_LIVE_STDERR_FILE).await {
                Ok(contents) => self.observe_stderr(&contents, false),
                Err(error) if progress_file_is_not_ready(&error) => {}
                Err(error) => {
                    self.stderr_read_failed = true;
                    warn!(
                        target: "nanocodex_eval",
                        error = %error,
                        "stopped polling the live stock-Codex stderr mirror"
                    );
                }
            }
        }
    }

    async fn poll_api(&mut self, terminal: bool) {
        if self.api_read_failed {
            return;
        }
        let mut input = match tokio::fs::File::open(&self.api_exchanges).await {
            Ok(input) => input,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return,
            Err(error) => {
                self.api_read_failed = true;
                warn!(
                    target: "nanocodex_eval",
                    path = %self.api_exchanges.display(),
                    %error,
                    "stopped polling the live stock-Codex API exchange log"
                );
                return;
            }
        };
        let length = match input.metadata().await {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                self.api_read_failed = true;
                warn!(
                    target: "nanocodex_eval",
                    path = %self.api_exchanges.display(),
                    %error,
                    "stopped polling stock-Codex API exchange metadata"
                );
                return;
            }
        };
        if length < self.api_offset {
            self.api_offset = 0;
        }
        if let Err(error) = input.seek(io::SeekFrom::Start(self.api_offset)).await {
            self.api_read_failed = true;
            warn!(
                target: "nanocodex_eval",
                path = %self.api_exchanges.display(),
                %error,
                "stopped seeking in the stock-Codex API exchange log"
            );
            return;
        }
        let mut pending = Vec::new();
        if let Err(error) = input.read_to_end(&mut pending).await {
            self.api_read_failed = true;
            warn!(
                target: "nanocodex_eval",
                path = %self.api_exchanges.display(),
                %error,
                "stopped reading the stock-Codex API exchange log"
            );
            return;
        }
        let (lines, consumed) = newly_completed_lines(&pending, 0, terminal);
        for line in lines {
            match serde_json::from_slice::<serde_json::Value>(line) {
                Ok(exchange) => self.reporter.observe_api_exchange("codex", &exchange),
                Err(error) => warn!(
                    target: "nanocodex_eval",
                    comparison_arm = "codex",
                    event_bytes = line.len(),
                    %error,
                    "live stock-Codex API exchange was not JSON"
                ),
            }
        }
        self.api_offset = self
            .api_offset
            .saturating_add(u64::try_from(consumed).unwrap_or(u64::MAX));
    }

    fn observe_stdout(&mut self, contents: &[u8], terminal: bool) {
        let (lines, next_offset) = newly_completed_lines(contents, self.stdout_offset, terminal);
        for line in lines {
            match serde_json::from_slice::<serde_json::Value>(line) {
                Ok(event) => self.reporter.observe_codex(&event),
                Err(error) => warn!(
                    target: "nanocodex_eval",
                    comparison_arm = "codex",
                    event_bytes = line.len(),
                    error = %error,
                    "live stock-Codex output was not a JSON event"
                ),
            }
        }
        self.stdout_offset = next_offset;
    }

    fn observe_stderr(&mut self, contents: &[u8], terminal: bool) {
        let (lines, next_offset) = newly_completed_lines(contents, self.stderr_offset, terminal);
        for line in lines {
            self.reporter.observe_codex_diagnostic(line);
        }
        self.stderr_offset = next_offset;
    }
}

fn progress_file_is_not_ready(error: &VmToolSessionError) -> bool {
    matches!(error, VmToolSessionError::Guest(message) if message.contains("No such file"))
}

fn newly_completed_lines(contents: &[u8], offset: usize, terminal: bool) -> (Vec<&[u8]>, usize) {
    if offset > contents.len() {
        return (Vec::new(), 0);
    }
    let pending = &contents[offset..];
    let end = if terminal {
        contents.len()
    } else {
        pending
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(offset, |line_end| offset + line_end + 1)
    };
    let lines = contents[offset..end]
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    (lines, end)
}

fn validate_vm_guest_elf(bytes: &[u8], path: &Path) -> InternalResult<()> {
    let header = bytes.get(..20).ok_or_else(|| {
        diff_error!(
            "VM guest executable is too short to contain an ELF header: {}",
            path.display()
        )
    })?;
    if &header[..4] != b"\x7fELF" {
        return Err(diff_error!(
            "VM guest executable is not an ELF executable: {}",
            path.display()
        ));
    }
    let class = header[4];
    let byte_order = header[5];
    let machine = u16::from_le_bytes([header[18], header[19]]);
    if class != 2 || byte_order != 1 || machine != VM_GUEST_ELF_MACHINE {
        return Err(diff_error!(
            "VM guest executable {} has ELF class {class}, byte order {byte_order}, and e_machine \
             {machine}; target {VM_GUEST_TARGET} requires 64-bit little-endian e_machine \
             {VM_GUEST_ELF_MACHINE}",
            path.display()
        ));
    }
    Ok(())
}

impl DifferentialEval {
    /// Starts a matched differential-evaluation recipe.
    #[must_use]
    pub fn builder(task: Task, nanocodex: NanocodexBuilder) -> DifferentialEvalBuilder {
        DifferentialEvalBuilder {
            task,
            nanocodex,
            codex: None,
            vm: None,
            output: PathBuf::from(DEFAULT_OUTPUT_DIRECTORY),
            thinking: Thinking::Medium,
            web_search: false,
            nanocodex_build: None,
        }
    }

    /// Runs both agents concurrently and retains one complete comparison.
    ///
    /// An incomplete arm remains a successful, inspectable report. This method
    /// returns an error only when the comparison itself cannot be prepared or
    /// retained.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid executable inputs, VM preparation failure,
    /// artifact I/O failure, or evaluator setup that prevents a report.
    pub async fn run(self) -> DifferentialResult<DifferentialReport> {
        self.run_inner().await.map_err(DifferentialError::new)
    }

    async fn run_inner(self) -> InternalResult<DifferentialReport> {
        let Self {
            task,
            nanocodex,
            codex_binary,
            codex_auth,
            vm,
            output,
            thinking,
            web_search,
            mut nanocodex_build,
        } = self;
        let started_at = Utc::now();
        let started = Instant::now();
        let codex_path = codex_binary.canonicalize().wrap_err_with(|| {
            format!(
                "failed to resolve Codex executable {}",
                codex_binary.display(),
            )
        })?;
        if !codex_path.is_file() {
            return Err(diff_error!(
                "Codex executable is not a regular file: {}",
                codex_path.display()
            ));
        }
        let codex_sha256 = file_sha256(&codex_path)?;
        nanocodex_build.path = nanocodex_build.path.canonicalize().wrap_err_with(|| {
            format!(
                "failed to resolve Nanocodex executable {}",
                nanocodex_build.path.display(),
            )
        })?;
        if !nanocodex_build.path.is_file() {
            return Err(diff_error!(
                "Nanocodex executable is not a regular file: {}",
                nanocodex_build.path.display()
            ));
        }
        nanocodex_build.sha256 = file_sha256(&nanocodex_build.path)?;
        let output_parent = prepare_output_parent(&output)?;
        let comparison_id = Uuid::now_v7();
        let comparison_directory = output_parent.join(comparison_id.to_string());
        fs::create_dir(&comparison_directory).wrap_err_with(|| {
            format!(
                "failed to create comparison directory {}",
                comparison_directory.display()
            )
        })?;
        let progress_path = comparison_directory.join(PROGRESS_FILE);
        let (progress, progress_recorder) =
            DiffProgress::start(progress_path.clone(), started).await?;
        progress.emit(
            "runner",
            "comparison.started",
            format!("{} · {MODEL} / {thinking}", task.name()),
        );

        let guest_codex_version = Arc::new(OnceLock::new());
        let vm_resources = Arc::new(
            prepare_diff_vm_resources(&task, &comparison_directory, &vm, web_search, &codex_path)
                .await?,
        );
        let codex = CodexExec::new(&codex_path, MODEL, thinking.as_str())?
            .web_search(web_search)
            .code_mode_only();

        let nanocodex_evaluator = Evaluator::builder(nanocodex.clone())
            .output_directory(comparison_directory.join("nanocodex"))
            .vm(vm_resources.nanocodex_backend());
        let codex_backend = vm_resources.codex_backend();
        let codex_resources = Arc::clone(&vm_resources);
        let codex_config = codex.clone();
        let codex_auth = codex_auth.clone();
        let version = Arc::clone(&guest_codex_version);
        let codex_progress = progress.clone();
        let codex_evaluator = Evaluator::builder(nanocodex)
            .output_directory(comparison_directory.join("codex"))
            .vm_with(codex_backend, move |attempt, _builder, runtime| {
                codex_resources.codex_attempt(
                    runtime,
                    attempt,
                    codex_config.clone(),
                    codex_auth.clone(),
                    Arc::clone(&version),
                    codex_progress.clone(),
                )
            });
        let projection = TrajectoryProjection::Codex {
            version: CodexVersion::Guest(Arc::clone(&guest_codex_version)),
        };
        let (nanocodex_arm, codex_arm) = tokio::join!(
            run_arm(
                task.clone(),
                nanocodex_evaluator,
                TrajectoryProjection::Nanocodex,
                true,
                progress.clone(),
            ),
            run_arm(
                task.clone(),
                codex_evaluator,
                projection,
                true,
                progress.clone(),
            ),
        );
        let codex_version = guest_codex_version
            .get()
            .cloned()
            .unwrap_or_else(|| "unavailable".to_owned());

        let classification = DifferentialClassification::from_arms(&nanocodex_arm, &codex_arm);
        let trajectory_comparison = TrajectoryComparison::from_arms(&nanocodex_arm, &codex_arm);
        let api_comparison_path = comparison_directory.join(API_COMPARISON_FILE);
        let (api_comparison, retained_api_comparison, api_comparison_error) =
            match retain_api_comparison(&api_comparison_path, &nanocodex_arm, &codex_arm) {
                Ok(summary) => (summary, Some(api_comparison_path), None),
                Err(error) => (
                    ApiComparisonSummary::unavailable(),
                    None,
                    Some(format!("{error:#}")),
                ),
            };
        let profile_validation_error =
            validate_matched_code_mode_only_profile(&api_comparison, MODEL, thinking.as_str());
        progress.emit("runner", "comparison.completed", classification.as_str());
        let progress_error = progress_recorder
            .finish(progress)
            .await
            .err()
            .map(|error| format!("{error:#}"));
        let comparison_path = comparison_directory.join(COMPARISON_FILE);
        let report = DifferentialReport {
            schema_version: COMPARISON_SCHEMA_VERSION,
            id: comparison_id,
            task: TaskIdentity {
                name: task.name().to_owned(),
                root: task.root().to_path_buf(),
            },
            model: MODEL.to_owned(),
            thinking: thinking.to_string(),
            policy: ComparisonPolicy {
                runner: "nanocodex_eval",
                environment: "micro_vm",
                attempts_per_agent: 1,
                execution_mode: "concurrent",
                web_search,
                codex_ephemeral: true,
                codex_approval_policy: "never",
                codex_sandbox: "danger_full_access",
                tool_mode: "code_mode_only",
                multi_agent: "disabled",
                reasoning_summary: "auto",
                expected_visible_tools: ["exec", "wait"],
            },
            started_at,
            finished_at: Utc::now(),
            duration_ms: elapsed_ms(started),
            classification,
            trajectory_comparison,
            api_comparison,
            nanocodex_build,
            codex_build: ExecutableIdentity {
                path: codex_path,
                version: codex_version,
                git_sha: None,
                built_at: None,
                sha256: codex_sha256,
            },
            nanocodex: nanocodex_arm,
            codex: codex_arm,
            artifacts: ComparisonArtifacts {
                directory: comparison_directory,
                comparison: comparison_path.clone(),
                progress: progress_path,
                progress_error,
                api_comparison: retained_api_comparison,
                api_comparison_error,
                profile_validation_error,
            },
        };
        write_json_atomic(&comparison_path, &report)?;
        Ok(report)
    }
}

impl DifferentialEvalBuilder {
    /// Selects the pinned stock-Codex executable and its guest auth.
    #[must_use]
    pub fn codex(mut self, executable: impl Into<PathBuf>, auth: CodexAuth) -> Self {
        self.codex = Some((executable.into(), auth));
        self
    }

    /// Selects the prepared, matched VM resources used by both arms.
    #[must_use]
    pub fn vm(mut self, vm: VmResources) -> Self {
        self.vm = Some(vm);
        self
    }

    /// Selects the parent directory for retained comparisons.
    #[must_use]
    pub fn output_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.output = directory.into();
        self
    }

    /// Pins the shared reasoning effort used by both agents.
    #[must_use]
    pub const fn thinking(mut self, thinking: Thinking) -> Self {
        self.thinking = thinking;
        self
    }

    /// Selects whether both agents expose standalone web search.
    #[must_use]
    pub const fn web_search(mut self, enabled: bool) -> Self {
        self.web_search = enabled;
        self
    }

    /// Records the embedding Nanocodex executable used as the VMM entrypoint.
    #[must_use]
    pub fn nanocodex_executable(mut self, identity: ExecutableIdentity) -> Self {
        self.nanocodex_build = Some(identity);
        self
    }

    /// Validates required components and builds one owned evaluation.
    ///
    /// # Errors
    ///
    /// Returns an error when Codex, VM resources, or executable identity is
    /// missing.
    pub fn build(self) -> std::result::Result<DifferentialEval, DifferentialBuildError> {
        let (codex_binary, codex_auth) = self.codex.ok_or(DifferentialBuildError::MissingCodex)?;
        Ok(DifferentialEval {
            task: self.task,
            nanocodex: self.nanocodex,
            codex_binary,
            codex_auth,
            vm: self.vm.ok_or(DifferentialBuildError::MissingVm)?,
            output: self.output,
            thinking: self.thinking,
            web_search: self.web_search,
            nanocodex_build: self
                .nanocodex_build
                .ok_or(DifferentialBuildError::MissingNanocodexIdentity)?,
        })
    }
}

impl DifferentialReport {
    /// Returns the matched verifier classification.
    #[must_use]
    pub const fn classification(&self) -> DifferentialClassification {
        self.classification
    }

    /// Returns the retained task name.
    #[must_use]
    pub fn task_name(&self) -> &str {
        &self.task.name
    }

    /// Returns the durable comparison record path.
    #[must_use]
    pub fn comparison_path(&self) -> &Path {
        &self.artifacts.comparison
    }

    /// Returns whether either arm or a derived comparison failed operationally.
    #[must_use]
    pub fn has_operational_error(&self) -> bool {
        self.artifacts.progress_error.is_some()
            || self.artifacts.api_comparison_error.is_some()
            || self.artifacts.profile_validation_error.is_some()
            || [&self.nanocodex, &self.codex].into_iter().any(|arm| {
                arm.operational_error.is_some()
                    || arm.event_error.is_some()
                    || arm.trajectory_error.is_some()
                    || arm.api_capture_error.is_some()
            })
    }

    /// Renders the stable plain-text summary used by command-line consumers.
    #[must_use]
    pub fn human_summary(&self) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "{}", self.classification.as_str());
        let _ = writeln!(output, "task: {}", self.task.name);
        append_arm_summary(&mut output, "nanocodex", &self.nanocodex);
        append_arm_summary(&mut output, "codex", &self.codex);
        append_model_visible_tool_summary(&mut output, &self.api_comparison.event_loop);
        append_first_generation_divergence(&mut output, &self.api_comparison.event_loop);
        append_unpaired_tail_summary(&mut output, &self.api_comparison.event_loop);
        let _ = writeln!(
            output,
            "live progress: {}",
            self.artifacts.progress.display()
        );
        if let Some(error) = &self.artifacts.progress_error {
            let _ = writeln!(output, "live progress error: {error}");
        }
        if let Some(path) = &self.artifacts.api_comparison {
            let _ = writeln!(output, "API comparison: {}", path.display());
        }
        if let Some(error) = &self.artifacts.api_comparison_error {
            let _ = writeln!(output, "API comparison error: {error}");
        }
        if let Some(error) = &self.artifacts.profile_validation_error {
            let _ = writeln!(output, "matched-profile error: {error}");
        }
        let _ = writeln!(
            output,
            "comparison: {}",
            self.artifacts.comparison.display()
        );
        output
    }
}

impl DifferentialReanalysis {
    /// Returns the rebuilt JSON report.
    #[must_use]
    pub const fn comparison(&self) -> &serde_json::Value {
        &self.comparison
    }

    /// Returns the durable comparison record that was updated.
    #[must_use]
    pub fn comparison_path(&self) -> &Path {
        &self.comparison_path
    }

    /// Returns the derived API comparison path when raw captures were available.
    #[must_use]
    pub fn api_comparison_path(&self) -> Option<&Path> {
        self.api_comparison_path.as_deref()
    }

    /// Returns a stable plain-text summary for command-line consumers.
    #[must_use]
    pub fn human_summary(&self) -> &str {
        &self.human_summary
    }
}

impl DifferentialClassification {
    const fn from_arms(nanocodex: &ArmReport, codex: &ArmReport) -> Self {
        if nanocodex.operational_error.is_some()
            || nanocodex.event_error.is_some()
            || nanocodex.trajectory_error.is_some()
            || nanocodex.api_capture_error.is_some()
            || codex.operational_error.is_some()
            || codex.event_error.is_some()
            || codex.trajectory_error.is_some()
            || codex.api_capture_error.is_some()
        {
            return Self::Incomplete;
        }
        match (
            matches!(nanocodex.summary.status, ArmStatus::Passed),
            matches!(codex.summary.status, ArmStatus::Passed),
        ) {
            (true, true) => Self::BothPassed,
            (false, true) => Self::CodexOnlyPassed,
            (true, false) => Self::NanocodexOnlyPassed,
            (false, false) => Self::NeitherPassed,
        }
    }

    /// Returns the stable serialized spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BothPassed => "both_passed",
            Self::CodexOnlyPassed => "codex_only_passed",
            Self::NanocodexOnlyPassed => "nanocodex_only_passed",
            Self::NeitherPassed => "neither_passed",
            Self::Incomplete => "incomplete",
        }
    }
}

impl ArmStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::VerifierFailed => "verifier_failed",
            Self::Unscored => "unscored",
            Self::RunnerError => "runner_error",
        }
    }
}

impl TrajectorySummary {
    fn new(trajectory: &AtifTrajectory) -> Self {
        let mut agent_steps = 0_usize;
        let mut message_steps = 0_usize;
        let mut reasoning_steps = 0_usize;
        let mut tool_sequence = Vec::new();
        for step in &trajectory.steps {
            if matches!(step.source, AtifSource::Agent) {
                agent_steps = agent_steps.saturating_add(1);
            }
            if !step.message.is_empty() {
                message_steps = message_steps.saturating_add(1);
            }
            if step
                .reasoning_content
                .as_ref()
                .is_some_and(|reasoning| !reasoning.is_empty())
            {
                reasoning_steps = reasoning_steps.saturating_add(1);
            }
            if let Some(tool_calls) = &step.tool_calls {
                tool_sequence.extend(
                    tool_calls
                        .iter()
                        .map(|tool_call| tool_call.function_name.clone()),
                );
            }
        }
        Self {
            total_steps: count_u32(trajectory.steps.len()),
            agent_steps: count_u32(agent_steps),
            message_steps: count_u32(message_steps),
            reasoning_steps: count_u32(reasoning_steps),
            tool_calls: count_u32(trajectory.tool_call_count()),
            observations: count_u32(trajectory.observation_count()),
            model_calls: trajectory
                .steps
                .iter()
                .filter(|step| matches!(step.source, AtifSource::Agent))
                .try_fold(0_u32, |total, step| {
                    step.llm_call_count.map(|count| total.saturating_add(count))
                }),
            tool_projection: match trajectory.agent.name.as_str() {
                "nanocodex" => "lifecycle_outer_and_nested_tools",
                "codex" => "stock_cli_completed_items",
                _ => "atif_tool_calls",
            },
            tool_sequence,
            shell_polling: ShellPollingSummary::new(&trajectory.steps),
            usage_completeness: trajectory.final_metrics.extra.usage_completeness,
            runtime_completeness: trajectory.final_metrics.extra.runtime_completeness,
        }
    }
}

impl ShellPollingSummary {
    fn new(steps: &[AtifStep]) -> Self {
        let mut poll_only_steps = 0_usize;
        let model_call_attribution_complete = steps
            .iter()
            .filter(|step| matches!(step.source, AtifSource::Agent))
            .all(|step| step.llm_call_count.is_some());
        let mut confirmed_model_calls = model_call_attribution_complete.then_some(0_u32);
        let mut empty_stdin_tool_calls = 0_usize;
        let mut sessions = BTreeSet::new();
        let mut explicit_requested_yield_ms = 0_u64;
        let mut tool_wait_duration_ns = 0_u64;
        let mut model_duration_ns = 0_u64;
        let mut prompt_tokens = 0_u64;
        let mut cached_tokens = 0_u64;
        let mut completion_tokens = 0_u64;

        for step in steps {
            let Some(tool_calls) = step.tool_calls.as_deref() else {
                continue;
            };
            let polling_calls = tool_calls
                .iter()
                .filter_map(|tool_call| {
                    empty_write_stdin_arguments(tool_call).map(|arguments| (tool_call, arguments))
                })
                .collect::<Vec<_>>();
            if polling_calls.is_empty()
                || !tool_calls.iter().all(|tool_call| {
                    tool_call.function_name == "exec"
                        || empty_write_stdin_arguments(tool_call).is_some()
                })
            {
                continue;
            }

            poll_only_steps = poll_only_steps.saturating_add(1);
            confirmed_model_calls = confirmed_model_calls
                .zip(step.llm_call_count)
                .map(|(total, count)| total.saturating_add(count));
            empty_stdin_tool_calls = empty_stdin_tool_calls.saturating_add(polling_calls.len());

            for (tool_call, arguments) in polling_calls {
                if let Some(session_id) = arguments.get("session_id") {
                    sessions.insert(session_id.to_string());
                }
                explicit_requested_yield_ms = explicit_requested_yield_ms.saturating_add(
                    arguments
                        .get("yield_time_ms")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default(),
                );
                let duration_ns = step
                    .observation
                    .as_ref()
                    .and_then(|observation| {
                        observation
                            .results
                            .iter()
                            .find(|result| result.source_call_id == tool_call.tool_call_id)
                    })
                    .map_or(0, |result| result.extra.duration_ns);
                tool_wait_duration_ns = tool_wait_duration_ns.saturating_add(duration_ns);
            }

            if let Some(metrics) = &step.metrics {
                model_duration_ns = model_duration_ns.saturating_add(metrics.extra.duration_ns);
                prompt_tokens = prompt_tokens.saturating_add(metrics.prompt_tokens);
                cached_tokens = cached_tokens.saturating_add(metrics.cached_tokens);
                completion_tokens = completion_tokens.saturating_add(metrics.completion_tokens);
            }
        }

        Self {
            poll_only_steps: count_u32(poll_only_steps),
            model_call_attribution_complete,
            confirmed_model_calls,
            empty_stdin_tool_calls: count_u32(empty_stdin_tool_calls),
            sessions: count_u32(sessions.len()),
            explicit_requested_yield_ms,
            tool_wait_duration_ns,
            model_duration_ns,
            prompt_tokens,
            cached_tokens,
            completion_tokens,
        }
    }
}

fn empty_write_stdin_arguments(tool_call: &AtifToolCall) -> Option<serde_json::Value> {
    if tool_call.function_name != "write_stdin" {
        return None;
    }
    let arguments = serde_json::from_str::<serde_json::Value>(tool_call.arguments.get()).ok()?;
    match arguments.get("chars") {
        Some(serde_json::Value::String(chars)) if chars.is_empty() => Some(arguments),
        None => Some(arguments),
        _ => None,
    }
}

impl TrajectoryComparison {
    const fn unavailable() -> Self {
        Self {
            comparable: false,
            tool_sequence_comparable: false,
            tool_sequence_equal: None,
            codex_minus_nanocodex: None,
        }
    }

    fn from_arms(nanocodex: &ArmReport, codex: &ArmReport) -> Self {
        let (Some(nanocodex), Some(codex)) = (
            nanocodex.trajectory_summary.as_ref(),
            codex.trajectory_summary.as_ref(),
        ) else {
            return Self::unavailable();
        };
        Self::from_summaries(nanocodex, codex)
    }

    fn from_summaries(nanocodex: &TrajectorySummary, codex: &TrajectorySummary) -> Self {
        let tool_sequence_comparable = codex.tool_projection == nanocodex.tool_projection;
        Self {
            comparable: true,
            tool_sequence_comparable,
            tool_sequence_equal: tool_sequence_comparable
                .then(|| codex.tool_sequence == nanocodex.tool_sequence),
            codex_minus_nanocodex: Some(TrajectoryDelta {
                total_steps: i64::from(codex.total_steps) - i64::from(nanocodex.total_steps),
                agent_steps: i64::from(codex.agent_steps) - i64::from(nanocodex.agent_steps),
                message_steps: i64::from(codex.message_steps) - i64::from(nanocodex.message_steps),
                reasoning_steps: i64::from(codex.reasoning_steps)
                    - i64::from(nanocodex.reasoning_steps),
                tool_calls: tool_sequence_comparable
                    .then(|| i64::from(codex.tool_calls) - i64::from(nanocodex.tool_calls)),
                observations: tool_sequence_comparable
                    .then(|| i64::from(codex.observations) - i64::from(nanocodex.observations)),
                model_calls: codex
                    .model_calls
                    .zip(nanocodex.model_calls)
                    .map(|(codex, nanocodex)| i64::from(codex) - i64::from(nanocodex)),
                shell_polling: ShellPollingDelta::between(
                    &codex.shell_polling,
                    &nanocodex.shell_polling,
                ),
            }),
        }
    }
}

impl ShellPollingDelta {
    fn between(codex: &ShellPollingSummary, nanocodex: &ShellPollingSummary) -> Self {
        Self {
            poll_only_steps: i64::from(codex.poll_only_steps)
                - i64::from(nanocodex.poll_only_steps),
            confirmed_model_calls: codex
                .confirmed_model_calls
                .zip(nanocodex.confirmed_model_calls)
                .map(|(codex, nanocodex)| i64::from(codex) - i64::from(nanocodex)),
            empty_stdin_tool_calls: i64::from(codex.empty_stdin_tool_calls)
                - i64::from(nanocodex.empty_stdin_tool_calls),
            sessions: i64::from(codex.sessions) - i64::from(nanocodex.sessions),
            explicit_requested_yield_ms: signed_u64_delta(
                codex.explicit_requested_yield_ms,
                nanocodex.explicit_requested_yield_ms,
            ),
            tool_wait_duration_ns: signed_u64_delta(
                codex.tool_wait_duration_ns,
                nanocodex.tool_wait_duration_ns,
            ),
            model_duration_ns: signed_u64_delta(
                codex.model_duration_ns,
                nanocodex.model_duration_ns,
            ),
            prompt_tokens: signed_u64_delta(codex.prompt_tokens, nanocodex.prompt_tokens),
            cached_tokens: signed_u64_delta(codex.cached_tokens, nanocodex.cached_tokens),
            completion_tokens: signed_u64_delta(
                codex.completion_tokens,
                nanocodex.completion_tokens,
            ),
        }
    }
}

impl ArmReport {
    fn from_outcome(
        evaluator_directory: PathBuf,
        event_log: PathBuf,
        outcome: EvalAttemptOutcome,
        event_error: Option<String>,
        trajectory: InternalResult<TrajectoryArtifact, String>,
        codex_artifacts: bool,
        api_capture_required: bool,
    ) -> Self {
        let attempt_directory = outcome_directory(&outcome);
        let (codex_events, codex_stderr, codex_summary) = if codex_artifacts {
            (
                retained_file(attempt_directory.join("agent/codex-events.jsonl")),
                retained_file(attempt_directory.join("agent/codex-stderr.log")),
                retained_file(attempt_directory.join("agent/codex-summary.json")),
            )
        } else {
            (None, None, None)
        };
        let (trajectory, trajectory_summary, trajectory_error) = match trajectory {
            Ok(artifact) => (Some(artifact.path), Some(artifact.summary), None),
            Err(error) => (None, None, Some(error)),
        };
        let api_capture = retain_arm_api_exchanges(
            &event_log,
            attempt_directory,
            codex_artifacts,
            api_capture_required,
        );
        let (api_exchanges, api_capture, api_capture_error) = match api_capture {
            Ok(Some(artifact)) => (Some(artifact.path), Some(artifact.summary), None),
            Ok(None) => (None, None, None),
            Err(error) => (None, None, Some(format!("{error:#}"))),
        };
        Self {
            summary: ArmSummary::from_outcome(&outcome),
            evaluator_directory: Some(evaluator_directory),
            event_log: retained_file(event_log),
            trajectory,
            trajectory_summary,
            trajectory_error,
            api_exchanges,
            api_capture,
            api_capture_error,
            codex_events,
            codex_stderr,
            codex_summary,
            operational_error: None,
            event_error,
            outcome: Some(outcome),
        }
    }

    fn runner_error(
        evaluator_directory: PathBuf,
        event_log: PathBuf,
        error: String,
        event_error: Option<String>,
    ) -> Self {
        Self {
            summary: ArmSummary::runner_error(),
            evaluator_directory: Some(evaluator_directory),
            event_log: retained_file(event_log),
            trajectory: None,
            trajectory_summary: None,
            trajectory_error: None,
            api_exchanges: None,
            api_capture: None,
            api_capture_error: None,
            codex_events: None,
            codex_stderr: None,
            codex_summary: None,
            operational_error: Some(error),
            event_error,
            outcome: None,
        }
    }

    const fn setup_error(error: String) -> Self {
        Self {
            summary: ArmSummary::runner_error(),
            evaluator_directory: None,
            event_log: None,
            trajectory: None,
            trajectory_summary: None,
            trajectory_error: None,
            api_exchanges: None,
            api_capture: None,
            api_capture_error: None,
            codex_events: None,
            codex_stderr: None,
            codex_summary: None,
            operational_error: Some(error),
            event_error: None,
            outcome: None,
        }
    }
}

impl ArmSummary {
    fn from_outcome(outcome: &EvalAttemptOutcome) -> Self {
        match outcome {
            EvalAttemptOutcome::Scored(result) => Self {
                status: match result.status {
                    EvalStatus::Passed => ArmStatus::Passed,
                    EvalStatus::Failed => ArmStatus::VerifierFailed,
                },
                outcome: Some(result.outcome),
                exception: result.exception.as_ref().map(|exception| exception.kind),
                verifier_exit_code: Some(result.verifier.exit_code),
                rewards: result.verifier.rewards.clone(),
                model: result.agent.as_ref().map(|agent| agent.model.clone()),
                tool_calls: result.agent.as_ref().map(|agent| agent.tool_calls),
                usage: result.agent.as_ref().map(|agent| agent.usage.clone()),
                duration_ms: result.agent.as_ref().map(agent_duration_ms),
            },
            EvalAttemptOutcome::Unscored(failure) => Self {
                status: ArmStatus::Unscored,
                outcome: Some(failure.exception.outcome),
                exception: Some(failure.exception.kind),
                verifier_exit_code: failure.verifier.as_ref().map(|verifier| verifier.exit_code),
                rewards: failure
                    .verifier
                    .as_ref()
                    .map_or_else(BTreeMap::new, |verifier| verifier.rewards.clone()),
                model: failure.agent.as_ref().map(|agent| agent.model.clone()),
                tool_calls: failure.agent.as_ref().map(|agent| agent.tool_calls),
                usage: failure.agent.as_ref().map(|agent| agent.usage.clone()),
                duration_ms: failure.agent.as_ref().map(agent_duration_ms),
            },
        }
    }

    const fn runner_error() -> Self {
        Self {
            status: ArmStatus::RunnerError,
            outcome: None,
            exception: None,
            verifier_exit_code: None,
            rewards: BTreeMap::new(),
            model: None,
            tool_calls: None,
            usage: None,
            duration_ms: None,
        }
    }
}

async fn run_arm(
    task: Task,
    evaluator: EvaluatorBuilder,
    projection: TrajectoryProjection,
    api_capture_required: bool,
    progress: DiffProgress,
) -> ArmReport {
    let codex_artifacts = matches!(&projection, TrajectoryProjection::Codex { .. });
    let arm_name = if codex_artifacts {
        "codex"
    } else {
        "nanocodex"
    };
    progress.emit(arm_name, "attempt.started", task.name());
    let (evaluator, events) = match evaluator.build() {
        Ok(built) => built,
        Err(error) => {
            let report = ArmReport::setup_error(format!("{error:#}"));
            progress.emit(
                arm_name,
                "attempt.failed",
                report
                    .operational_error
                    .as_deref()
                    .unwrap_or("evaluator setup failed"),
            );
            return report;
        }
    };
    let evaluator_directory = evaluator.directory().to_path_buf();
    let event_log = evaluator_directory.join("events.jsonl");
    let stream = events.subscribe();
    drop(events);
    let event_path = event_log.clone();
    let event_progress = progress.clone();
    let event_recorder =
        tokio::spawn(
            async move { record_events(stream, &event_path, arm_name, event_progress).await },
        );
    let outcome = evaluator.task(task).await;
    drop(evaluator);
    let (recording, event_error) = match event_recorder.await {
        Ok(Ok(recording)) => (Some(recording), None),
        Ok(Err(error)) => (None, Some(format!("{error:#}"))),
        Err(error) => (None, Some(format!("event recorder task failed: {error}"))),
    };
    let report = match outcome {
        Ok(outcome) => {
            let trajectory = recording.map_or_else(
                || {
                    Err("trajectory projection unavailable because evaluator event recording failed"
                        .to_owned())
                },
                |recording| {
                    retain_trajectory(&outcome, recording, projection)
                        .map_err(|error| format!("{error:#}"))
                },
            );
            ArmReport::from_outcome(
                evaluator_directory,
                event_log,
                outcome,
                event_error,
                trajectory,
                codex_artifacts,
                api_capture_required,
            )
        }
        Err(error) => ArmReport::runner_error(
            evaluator_directory,
            event_log,
            format!("{error:#}"),
            event_error,
        ),
    };
    progress.emit(
        arm_name,
        "attempt.completed",
        format!(
            "{} · reward {}",
            report.summary.status.as_str(),
            report
                .summary
                .rewards
                .values()
                .next()
                .map_or_else(|| "unscored".to_owned(), ToString::to_string)
        ),
    );
    report
}

async fn record_events(
    mut stream: EvalEventStream,
    path: &Path,
    arm_name: &'static str,
    progress: DiffProgress,
) -> InternalResult<EventRecording> {
    let mut output = tokio::fs::File::create(path)
        .await
        .wrap_err_with(|| format!("failed to create evaluator event log {}", path.display()))?;
    let mut atif = AtifBuilder::default();
    let mut atif_error = None;
    while let Some(event) = stream.recv().await? {
        progress.observe_evaluator(arm_name, &event.kind);
        if let EvalEventKind::Agent(agent_event) = &event.kind {
            let payload = serde_json::from_str(agent_event.payload.get()).unwrap_or_default();
            if matches!(agent_event.kind, AgentEventKind::ApiEvent) && arm_name == "nanocodex" {
                progress.observe_nanocodex_api(&payload);
            } else if !matches!(
                agent_event.kind,
                AgentEventKind::AssistantDelta | AgentEventKind::ReasoningSummaryDelta
            ) && arm_name == "nanocodex"
            {
                progress.observe_nanocodex(agent_event);
            }
            if atif_error.is_none()
                && let Err(error) = atif.apply(agent_event)
            {
                atif_error = Some(format!(
                    "failed to project agent event sequence {} into ATIF: {error}",
                    event.sequence
                ));
            }
        }
        let mut encoded = serde_json::to_vec(event.as_ref())?;
        encoded.push(b'\n');
        output.write_all(&encoded).await?;
    }
    output.flush().await?;
    output.sync_all().await?;
    Ok(EventRecording { atif, atif_error })
}

fn retain_trajectory(
    outcome: &EvalAttemptOutcome,
    recording: EventRecording,
    projection: TrajectoryProjection,
) -> InternalResult<TrajectoryArtifact> {
    let task = outcome_task(outcome);
    let trajectory = match projection {
        TrajectoryProjection::Nanocodex => {
            if let Some(error) = recording.atif_error {
                return Err(diff_error!(error));
            }
            match outcome_agent(outcome) {
                Some(agent) => recording.atif.finish(task, agent),
                None => recording.atif.finish_failure(task),
            }
        }
        TrajectoryProjection::Codex { version } => {
            let agent = outcome_agent(outcome).ok_or_else(|| {
                diff_error!("stock Codex attempt retained no terminal agent result")
            })?;
            let events = outcome_directory(outcome).join("agent/codex-events.jsonl");
            let version = version.resolve()?;
            project_codex_atif(&events, task.prompt(), agent, &version).wrap_err_with(|| {
                format!("failed to project stock Codex stream {}", events.display())
            })?
        }
    };
    let path = outcome_directory(outcome).join(TRAJECTORY_FILE);
    let parent = path
        .parent()
        .ok_or_else(|| diff_error!("trajectory path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent)
        .wrap_err_with(|| format!("failed to create trajectory directory {}", parent.display()))?;
    let summary = TrajectorySummary::new(&trajectory);
    write_json_atomic(&path, &trajectory)?;
    Ok(TrajectoryArtifact { path, summary })
}

fn retain_arm_api_exchanges(
    event_log: &Path,
    attempt_directory: &Path,
    codex_arm: bool,
    required: bool,
) -> InternalResult<Option<ApiCaptureArtifact>> {
    let path = attempt_directory.join(API_EXCHANGES_FILE);
    if codex_arm {
        if !path.is_file() {
            if required {
                return Err(diff_error!(
                    "stock Codex retained no API exchange capture at {}",
                    path.display()
                ));
            }
            return Ok(None);
        }
        return Ok(Some(inspect_api_exchanges(
            path,
            "all_api_payloads_routed_through_configured_base_url",
            "exact_wire_payload_bytes",
        )?));
    }
    project_nanocodex_api_exchanges(event_log, &path)?;
    Ok(Some(inspect_api_exchanges(
        path,
        "responses_request_and_response_payloads",
        "complete_observed_json_values",
    )?))
}

fn project_nanocodex_api_exchanges(event_log: &Path, output: &Path) -> InternalResult<()> {
    let parent = output
        .parent()
        .ok_or_else(|| diff_error!("API exchange path has no parent: {}", output.display()))?;
    fs::create_dir_all(parent)?;
    let input =
        BufReader::new(File::open(event_log).wrap_err_with(|| {
            format!("failed to open evaluator event log {}", event_log.display())
        })?);
    let mut output_file = File::create(output)
        .wrap_err_with(|| format!("failed to create API exchange log {}", output.display()))?;
    let mut sequence = 0_u64;
    let mut request_index = 0_u64;
    for (line_index, line) in input.lines().enumerate() {
        let line = line.wrap_err_with(|| {
            format!(
                "failed to read evaluator event line {} from {}",
                line_index.saturating_add(1),
                event_log.display()
            )
        })?;
        let envelope: serde_json::Value = serde_json::from_str(&line).wrap_err_with(|| {
            format!(
                "invalid evaluator event JSON at {}:{}",
                event_log.display(),
                line_index.saturating_add(1)
            )
        })?;
        if envelope.get("type").and_then(serde_json::Value::as_str) != Some("agent")
            || envelope
                .pointer("/payload/type")
                .and_then(serde_json::Value::as_str)
                != Some("api.event")
        {
            continue;
        }
        let api = envelope
            .pointer("/payload/payload")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| {
                diff_error!(
                    "Nanocodex API event has no object payload at {}:{}",
                    event_log.display(),
                    line_index.saturating_add(1)
                )
            })?;
        let direction = api
            .get("direction")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        if direction == "outbound" {
            request_index = request_index.saturating_add(1);
        }
        let event = api.get("event").cloned().unwrap_or(serde_json::Value::Null);
        let payload_bytes = serde_json::to_vec(&event)?.len();
        sequence = sequence.saturating_add(1);
        let record = serde_json::json!({
            "schema_version": API_CAPTURE_SCHEMA_VERSION,
            "sequence": sequence,
            "direction": direction,
            "transport": api
                .get("transport")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            "request_index": request_index,
            "model_call_index": api.get("model_call_index"),
            "phase": api
                .get("phase")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            "kind": "message",
            "payload_bytes": payload_bytes,
            "payload": {
                "encoding": "json",
                "event": event,
            },
        });
        serde_json::to_writer(&mut output_file, &record)?;
        output_file.write_all(b"\n")?;
    }
    output_file.flush()?;
    output_file.sync_all()?;
    Ok(())
}

fn inspect_api_exchanges(
    path: PathBuf,
    payload_scope: &'static str,
    payload_fidelity: &'static str,
) -> InternalResult<ApiCaptureArtifact> {
    let input = BufReader::new(
        File::open(&path)
            .wrap_err_with(|| format!("failed to open API exchange log {}", path.display()))?,
    );
    let mut summary = ApiCaptureSummary {
        schema_version: API_CAPTURE_SCHEMA_VERSION,
        payload_scope,
        header_scope: "forwarded_not_retained",
        payload_fidelity,
        records: 0,
        requests: 0,
        response_requests: 0,
        auxiliary_requests: 0,
        inbound_events: 0,
        terminal_events: 0,
        http_responses_completed: 0,
        payload_bytes: 0,
        exchange_complete: false,
        transports: BTreeMap::new(),
        phases: BTreeMap::new(),
    };
    let mut outbound_requests = BTreeSet::new();
    let mut response_requests = BTreeSet::new();
    let mut http_requests = BTreeSet::new();
    let mut terminal_response_requests = BTreeSet::new();
    let mut completed_http_requests = BTreeSet::new();
    for (line_index, line) in input.lines().enumerate() {
        let line = line?;
        let record: serde_json::Value = serde_json::from_str(&line).wrap_err_with(|| {
            format!(
                "invalid API exchange JSON at {}:{}",
                path.display(),
                line_index.saturating_add(1)
            )
        })?;
        summary.records = summary.records.saturating_add(1);
        summary.payload_bytes = summary.payload_bytes.saturating_add(
            record
                .get("payload_bytes")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
        );
        if let Some(transport) = record.get("transport").and_then(serde_json::Value::as_str) {
            increment(&mut summary.transports, transport);
        }
        if let Some(phase) = record.get("phase").and_then(serde_json::Value::as_str) {
            increment(&mut summary.phases, phase);
        }
        let request_index = record
            .get("request_index")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                diff_error!(
                    "API exchange has no request index at {}:{}",
                    path.display(),
                    line_index.saturating_add(1)
                )
            })?;
        match record.get("direction").and_then(serde_json::Value::as_str) {
            Some("outbound") => {
                outbound_requests.insert(request_index);
                if record_api_event_type(&record).as_deref() == Some("response.create") {
                    response_requests.insert(request_index);
                }
                if record.get("transport").and_then(serde_json::Value::as_str)
                    == Some("responses_https")
                {
                    http_requests.insert(request_index);
                }
            }
            Some("inbound") => {
                summary.inbound_events = summary.inbound_events.saturating_add(1);
                if record_api_event_type(&record).is_some_and(|event_type| {
                    matches!(
                        event_type.as_str(),
                        "response.completed" | "response.failed" | "error"
                    )
                }) {
                    summary.terminal_events = summary.terminal_events.saturating_add(1);
                    terminal_response_requests.insert(request_index);
                }
                if record.get("kind").and_then(serde_json::Value::as_str)
                    == Some("response_completed")
                {
                    completed_http_requests.insert(request_index);
                }
            }
            _ => {}
        }
    }
    summary.requests = u64::try_from(outbound_requests.len()).unwrap_or(u64::MAX);
    summary.response_requests = u64::try_from(response_requests.len()).unwrap_or(u64::MAX);
    summary.auxiliary_requests = summary.requests.saturating_sub(summary.response_requests);
    summary.http_responses_completed =
        u64::try_from(completed_http_requests.len()).unwrap_or(u64::MAX);
    summary.exchange_complete = !outbound_requests.is_empty()
        && outbound_requests.iter().all(|request_index| {
            (response_requests.contains(request_index) || http_requests.contains(request_index))
                && (!response_requests.contains(request_index)
                    || terminal_response_requests.contains(request_index))
                && (!http_requests.contains(request_index)
                    || completed_http_requests.contains(request_index))
        });
    Ok(ApiCaptureArtifact { path, summary })
}

fn increment(counts: &mut BTreeMap<String, u64>, key: &str) {
    let count = counts.entry(key.to_owned()).or_default();
    *count = count.saturating_add(1);
}

fn record_api_event_type(record: &serde_json::Value) -> Option<String> {
    record_api_events(record)
        .into_iter()
        .find_map(|event| api_event_type(&event))
        .or_else(|| {
            let payload = record.get("payload")?;
            api_event_type(
                payload
                    .get("event")
                    .or_else(|| payload.get("text"))
                    .unwrap_or(payload),
            )
        })
}

/// Rebuilds derived trajectory and API comparisons from retained raw evidence.
///
/// This performs no agent, model, VM, or verifier work.
///
/// # Errors
///
/// Returns an error when the retained comparison is malformed, a referenced
/// artifact is missing, or the rebuilt evidence cannot be published.
pub fn reanalyze(path: impl AsRef<Path>) -> DifferentialResult<DifferentialReanalysis> {
    reanalyze_inner(path.as_ref()).map_err(DifferentialError::new)
}

fn reanalyze_inner(path: &Path) -> InternalResult<DifferentialReanalysis> {
    let requested = path
        .canonicalize()
        .wrap_err_with(|| format!("failed to resolve retained comparison {}", path.display()))?;
    let comparison_path = if requested.is_dir() {
        requested.join(COMPARISON_FILE)
    } else {
        requested
    };
    let directory = comparison_path.parent().ok_or_else(|| {
        diff_error!(
            "retained comparison has no parent directory: {}",
            comparison_path.display()
        )
    })?;
    let mut comparison: serde_json::Value =
        serde_json::from_reader(File::open(&comparison_path).wrap_err_with(|| {
            format!(
                "failed to open retained comparison {}",
                comparison_path.display()
            )
        })?)
        .wrap_err_with(|| {
            format!(
                "retained comparison is not valid JSON: {}",
                comparison_path.display()
            )
        })?;
    let nanocodex_trajectory_summary =
        retained_trajectory_summary(&comparison, directory, "/nanocodex/trajectory", "Nanocodex")?;
    let codex_trajectory_summary =
        retained_trajectory_summary(&comparison, directory, "/codex/trajectory", "Codex")?;
    let trajectory_comparison = match (
        nanocodex_trajectory_summary.as_ref(),
        codex_trajectory_summary.as_ref(),
    ) {
        (Some(nanocodex), Some(codex)) => TrajectoryComparison::from_summaries(nanocodex, codex),
        _ => TrajectoryComparison::unavailable(),
    };

    let nanocodex_api_path = retained_artifact_path(
        &comparison,
        directory,
        "/nanocodex/api_exchanges",
        "Nanocodex",
        "API exchange capture",
    )?;
    let codex_api_path = retained_artifact_path(
        &comparison,
        directory,
        "/codex/api_exchanges",
        "Codex",
        "API exchange capture",
    )?;
    let api_comparison_path = comparison
        .pointer("/artifacts/api_comparison")
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from)
        .map_or_else(
            || directory.join(API_COMPARISON_FILE),
            |path| {
                if path.is_absolute() {
                    path
                } else {
                    directory.join(path)
                }
            },
        );
    let api_summary = match (nanocodex_api_path.as_ref(), codex_api_path.as_ref()) {
        (Some(nanocodex_path), Some(codex_path)) => {
            let nanocodex_capture = inspect_api_exchanges(
                nanocodex_path.clone(),
                "responses_request_and_response_payloads",
                "complete_observed_json_values",
            )?
            .summary;
            let codex_capture = inspect_api_exchanges(
                codex_path.clone(),
                "all_api_payloads_routed_through_configured_base_url",
                "exact_wire_payload_bytes",
            )?
            .summary;
            Some(compare_api_exchanges(
                &api_comparison_path,
                Some(nanocodex_path),
                Some(codex_path),
                Some(nanocodex_capture),
                Some(codex_capture),
            )?)
        }
        _ => None,
    };
    let expected_model = comparison
        .get("model")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let expected_effort = comparison
        .get("thinking")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let profile_validation_error = api_summary.as_ref().and_then(|summary| {
        expected_model
            .as_deref()
            .zip(expected_effort.as_deref())
            .and_then(|(model, effort)| {
                validate_matched_code_mode_only_profile(summary, model, effort)
            })
    });

    let comparison_object = comparison
        .as_object_mut()
        .ok_or_else(|| diff_error!("retained comparison root is not an object"))?;
    comparison_object.insert(
        "schema_version".to_owned(),
        serde_json::json!(COMPARISON_SCHEMA_VERSION),
    );
    comparison_object.insert(
        "trajectory_comparison".to_owned(),
        serde_json::to_value(&trajectory_comparison)?,
    );
    let nanocodex = comparison_object
        .get_mut("nanocodex")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| diff_error!("retained comparison has no Nanocodex arm object"))?;
    nanocodex.insert(
        "trajectory_summary".to_owned(),
        serde_json::to_value(&nanocodex_trajectory_summary)?,
    );
    let codex = comparison_object
        .get_mut("codex")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| diff_error!("retained comparison has no Codex arm object"))?;
    codex.insert(
        "trajectory_summary".to_owned(),
        serde_json::to_value(&codex_trajectory_summary)?,
    );
    comparison_object.insert(
        "api_comparison".to_owned(),
        serde_json::to_value(
            api_summary
                .clone()
                .unwrap_or_else(ApiComparisonSummary::unavailable),
        )?,
    );
    let artifacts = comparison_object
        .get_mut("artifacts")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| diff_error!("retained comparison has no artifacts object"))?;
    artifacts.insert(
        "profile_validation_error".to_owned(),
        serde_json::to_value(&profile_validation_error)?,
    );
    write_json_atomic(&comparison_path, &comparison)?;

    let rebuilt = if api_summary.is_some() {
        serde_json::from_reader(File::open(&api_comparison_path)?)?
    } else {
        comparison
    };
    let mut human_summary = String::new();
    let _ = writeln!(
        human_summary,
        "reanalyzed retained evidence{} without running either agent",
        if api_summary.is_some() {
            " and API captures"
        } else {
            "; API captures unavailable"
        }
    );
    if let Some(summary) = &nanocodex_trajectory_summary {
        append_shell_polling_summary(&mut human_summary, "nanocodex", &summary.shell_polling);
    } else {
        let _ = writeln!(human_summary, "nanocodex trajectory: unavailable");
    }
    if let Some(summary) = &codex_trajectory_summary {
        append_shell_polling_summary(&mut human_summary, "codex", &summary.shell_polling);
    } else {
        let _ = writeln!(human_summary, "codex trajectory: unavailable");
    }
    if let Some(summary) = &api_summary {
        let _ = writeln!(
            human_summary,
            "event loop: chain invariants {} · {} aligned, {} matching, {} differing · unpaired nanocodex {} / codex {}",
            summary
                .event_loop
                .chain_invariants_equal
                .map_or("unavailable", |equal| if equal {
                    "match"
                } else {
                    "differ"
                }),
            summary.event_loop.aligned_turns,
            summary.event_loop.equal_turns,
            summary.event_loop.differing_turns,
            summary.event_loop.nanocodex_unpaired_turns,
            summary.event_loop.codex_unpaired_turns,
        );
        if let Some(divergence) = &summary.event_loop.first_divergence {
            let _ = writeln!(
                human_summary,
                "first event-loop divergence: turn {} · {} · {}",
                divergence.request_index,
                divergence.categories.join(","),
                divergence.pointer
            );
        }
        append_first_generation_divergence(&mut human_summary, &summary.event_loop);
        append_event_loop_arm_summary(
            &mut human_summary,
            "nanocodex",
            summary.event_loop.nanocodex.as_ref(),
        );
        append_event_loop_arm_summary(
            &mut human_summary,
            "codex",
            summary.event_loop.codex.as_ref(),
        );
        append_unpaired_tail_summary(&mut human_summary, &summary.event_loop);
        if let Some(error) = &profile_validation_error {
            let _ = writeln!(human_summary, "matched-profile error: {error}");
        }
        let _ = writeln!(
            human_summary,
            "API comparison: {}",
            api_comparison_path.display()
        );
    }
    let _ = writeln!(human_summary, "comparison: {}", comparison_path.display());
    Ok(DifferentialReanalysis {
        comparison: rebuilt,
        comparison_path,
        api_comparison_path: api_summary.is_some().then_some(api_comparison_path),
        human_summary,
    })
}

fn retained_artifact_path(
    comparison: &serde_json::Value,
    directory: &Path,
    pointer: &str,
    arm: &str,
    artifact: &str,
) -> InternalResult<Option<PathBuf>> {
    let Some(retained) = comparison
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(None);
    };
    let retained = PathBuf::from(retained);
    let resolved = if retained.is_absolute() {
        retained
    } else {
        directory.join(retained)
    };
    if !resolved.is_file() {
        return Err(diff_error!(
            "{arm} {artifact} is not a file: {}",
            resolved.display()
        ));
    }
    Ok(Some(resolved))
}

fn retained_trajectory_summary(
    comparison: &serde_json::Value,
    directory: &Path,
    pointer: &str,
    arm: &str,
) -> InternalResult<Option<TrajectorySummary>> {
    let Some(path) = retained_artifact_path(comparison, directory, pointer, arm, "trajectory")?
    else {
        return Ok(None);
    };
    let trajectory: AtifTrajectory =
        serde_json::from_reader(File::open(&path).wrap_err_with(|| {
            format!(
                "failed to open retained {arm} trajectory {}",
                path.display()
            )
        })?)?;
    Ok(Some(TrajectorySummary::new(&trajectory)))
}

fn append_shell_polling_summary(output: &mut String, name: &str, summary: &ShellPollingSummary) {
    let _ = writeln!(
        output,
        "{name} shell polling: {} observed poll-only steps · {} confirmed poll-only model calls · {} sessions · {} input/{} output tokens · {:.1}s model time",
        summary.poll_only_steps,
        summary
            .confirmed_model_calls
            .map_or_else(|| "unavailable".to_owned(), |calls| calls.to_string()),
        summary.sessions,
        summary.prompt_tokens,
        summary.completion_tokens,
        Duration::from_nanos(summary.model_duration_ns).as_secs_f64(),
    );
}

fn append_event_loop_arm_summary(
    output: &mut String,
    name: &str,
    summary: Option<&ApiEventLoopArmSummary>,
) {
    let Some(summary) = summary else {
        let _ = writeln!(output, "{name} event loop: unavailable");
        return;
    };
    let _ = writeln!(
        output,
        "{name} event loop: {}/{} terminal · {} generation turns · model-visible calls {} [{}] · profile {}/{}/summary={} · visible tools [{}] · {} detected poll-only · previous links {} direct/{} replay ({} after nonterminal)/{} broken · tool-result links {} valid/{} replayed/{} broken · cache stable {}",
        summary.terminal_turns,
        summary.turns,
        summary.generation_turns,
        summary.model_visible_tool_calls,
        summary.model_visible_tool_sequence.join(", "),
        summary.initial_model.as_deref().unwrap_or("unobserved"),
        summary
            .initial_reasoning_effort
            .as_deref()
            .unwrap_or("unobserved"),
        summary
            .initial_reasoning_summary
            .as_deref()
            .unwrap_or("unobserved"),
        summary.initial_visible_tools.join(", "),
        summary.detected_poll_only_turns,
        summary.previous_response_links,
        summary.full_history_replays,
        summary.full_history_replays_after_nonterminal_turn,
        summary.broken_previous_response_links,
        summary.tool_result_links,
        summary.replayed_tool_result_links,
        summary.broken_tool_result_links,
        summary
            .prompt_cache_key_stable
            .map_or("unobserved", |stable| if stable { "yes" } else { "no" }),
    );
}

fn append_model_visible_tool_summary(output: &mut String, comparison: &ApiEventLoopComparison) {
    let (Some(nanocodex), Some(codex)) = (comparison.nanocodex.as_ref(), comparison.codex.as_ref())
    else {
        return;
    };
    let format_input_text_sections = |sections: &[ApiInputTextSectionSummary]| {
        sections
            .iter()
            .map(|section| format!("{}/{}:{}B", section.role, section.label, section.text_bytes))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let _ = writeln!(
        output,
        "initial model input text: nanocodex [{}] · codex [{}] · match {}",
        format_input_text_sections(&nanocodex.initial_input_text_sections),
        format_input_text_sections(&codex.initial_input_text_sections),
        comparison
            .initial_input_text_sections_equal
            .map_or("unavailable", |equal| if equal { "yes" } else { "no" }),
    );
    let _ = writeln!(
        output,
        "initial generation input text: nanocodex [{}] · codex [{}] · match {}",
        format_input_text_sections(&nanocodex.initial_generation_input_text_sections),
        format_input_text_sections(&codex.initial_generation_input_text_sections),
        comparison
            .initial_generation_input_text_sections_equal
            .map_or("unavailable", |equal| if equal { "yes" } else { "no" }),
    );
    let _ = writeln!(
        output,
        "model-visible tool sequence: nanocodex [{}] · codex [{}] · match {}",
        nanocodex.model_visible_tool_sequence.join(", "),
        codex.model_visible_tool_sequence.join(", "),
        comparison
            .model_visible_tool_sequence_equal
            .map_or("unavailable", |equal| if equal { "yes" } else { "no" }),
    );
    let format_code_mode_tools = |tools: Option<&[String]>| {
        tools.map_or_else(
            || "unavailable".to_owned(),
            |tools| format!("[{}]", tools.join(", ")),
        )
    };
    let _ = writeln!(
        output,
        "nested Code Mode tool catalog: nanocodex {} · codex {} · match {}",
        format_code_mode_tools(nanocodex.initial_code_mode_tools.as_deref()),
        format_code_mode_tools(codex.initial_code_mode_tools.as_deref()),
        comparison
            .initial_code_mode_tool_names_equal
            .map_or("unavailable", |equal| if equal { "yes" } else { "no" }),
    );
    let _ = writeln!(
        output,
        "nested Code Mode tool definitions match: {}",
        comparison
            .initial_code_mode_tool_definitions_equal
            .map_or("unavailable", |equal| if equal { "yes" } else { "no" }),
    );
}

fn append_first_generation_divergence(output: &mut String, comparison: &ApiEventLoopComparison) {
    let Some(divergence) = &comparison.first_generation_divergence else {
        return;
    };
    let _ = writeln!(
        output,
        "first generation divergence: turn {} · {} · {}",
        divergence.request_index,
        divergence.categories.join(","),
        divergence.pointer,
    );
}

fn append_unpaired_tail_summary(output: &mut String, comparison: &ApiEventLoopComparison) {
    let (Some(nanocodex), Some(codex)) = (
        comparison.nanocodex_unpaired_tail.as_ref(),
        comparison.codex_unpaired_tail.as_ref(),
    ) else {
        return;
    };
    if nanocodex.turns == 0 && codex.turns == 0 {
        return;
    }
    let format = |tail: &ApiEventLoopTailSummary| {
        format!(
            "{} turns/{} generation/{} poll-only · {} total tokens ({} cached + {} uncached input, {} output, {} reasoning) · usage {}/{}",
            tail.turns,
            tail.generation_turns,
            tail.detected_poll_only_turns,
            tail.usage.total_tokens,
            tail.usage.cached_input_tokens,
            tail.usage.uncached_input_tokens,
            tail.usage.output_tokens,
            tail.usage.reasoning_output_tokens,
            tail.turns_with_usage,
            tail.turns,
        )
    };
    let _ = writeln!(
        output,
        "unpaired API tail: nanocodex [{}] · codex [{}]",
        format(nanocodex),
        format(codex),
    );
}

fn retain_api_comparison(
    path: &Path,
    nanocodex: &ArmReport,
    codex: &ArmReport,
) -> InternalResult<ApiComparisonSummary> {
    compare_api_exchanges(
        path,
        nanocodex.api_exchanges.as_deref(),
        codex.api_exchanges.as_deref(),
        nanocodex.api_capture.clone(),
        codex.api_capture.clone(),
    )
}

fn validate_matched_code_mode_only_profile(
    summary: &ApiComparisonSummary,
    expected_model: &str,
    expected_effort: &str,
) -> Option<String> {
    if !summary.comparable {
        return None;
    }
    let expected = ["exec", "wait"];
    let nanocodex = summary.event_loop.nanocodex.as_ref()?;
    let codex = summary.event_loop.codex.as_ref()?;
    let arm_matches = |arm: &ApiEventLoopArmSummary| {
        arm.initial_model.as_deref() == Some(expected_model)
            && arm.initial_reasoning_effort.as_deref() == Some(expected_effort)
            && arm.initial_reasoning_summary.as_deref() == Some("auto")
            && arm
                .initial_visible_tools
                .iter()
                .map(String::as_str)
                .eq(expected)
    };
    let nanocodex_matches = arm_matches(nanocodex);
    let codex_matches = arm_matches(codex);
    let model_input_matches = summary.event_loop.initial_input_text_sections_equal == Some(true)
        && summary
            .event_loop
            .initial_generation_input_text_sections_equal
            == Some(true);
    let code_mode_catalog_matches = summary.event_loop.initial_code_mode_tool_names_equal
        == Some(true)
        && summary.event_loop.initial_code_mode_tool_definitions_equal == Some(true);
    if nanocodex_matches && codex_matches && model_input_matches && code_mode_catalog_matches {
        return None;
    }
    Some(format!(
        "expected both first API requests to use model={expected_model}, effort={expected_effort}, reasoning.summary=auto, only [exec, wait], and identical initial input text plus nested Code Mode definitions for the pinned code_mode_only profile; nanocodex={}/{}/summary={}/[{}], codex={}/{}/summary={}/[{}], initial_input_text_equal={:?}, initial_generation_input_text_equal={:?}, nested_tool_names_equal={:?}, nested_tool_definitions_equal={:?}",
        nanocodex.initial_model.as_deref().unwrap_or("unobserved"),
        nanocodex
            .initial_reasoning_effort
            .as_deref()
            .unwrap_or("unobserved"),
        nanocodex
            .initial_reasoning_summary
            .as_deref()
            .unwrap_or("unobserved"),
        nanocodex.initial_visible_tools.join(", "),
        codex.initial_model.as_deref().unwrap_or("unobserved"),
        codex
            .initial_reasoning_effort
            .as_deref()
            .unwrap_or("unobserved"),
        codex
            .initial_reasoning_summary
            .as_deref()
            .unwrap_or("unobserved"),
        codex.initial_visible_tools.join(", "),
        summary.event_loop.initial_input_text_sections_equal,
        summary
            .event_loop
            .initial_generation_input_text_sections_equal,
        summary.event_loop.initial_code_mode_tool_names_equal,
        summary.event_loop.initial_code_mode_tool_definitions_equal,
    ))
}

fn compare_api_exchanges(
    path: &Path,
    nanocodex_path: Option<&Path>,
    codex_path: Option<&Path>,
    nanocodex_capture: Option<ApiCaptureSummary>,
    codex_capture: Option<ApiCaptureSummary>,
) -> InternalResult<ApiComparisonSummary> {
    let nanocodex_requests = nanocodex_path.map(read_api_request_payloads).transpose()?;
    let codex_requests = codex_path.map(read_api_request_payloads).transpose()?;
    let comparable = nanocodex_requests.is_some() && codex_requests.is_some();
    let nanocodex_event_loop = nanocodex_requests.as_deref().map(build_event_loop_trace);
    let codex_event_loop = codex_requests.as_deref().map(build_event_loop_trace);
    let request_count_equal = nanocodex_requests
        .as_ref()
        .zip(codex_requests.as_ref())
        .map(|(nanocodex, codex)| nanocodex.len() == codex.len());
    let nanocodex_requests = nanocodex_requests.unwrap_or_default();
    let codex_requests = codex_requests.unwrap_or_default();
    let aligned_request_count = nanocodex_requests.len().min(codex_requests.len());
    let request_count = nanocodex_requests.len().max(codex_requests.len());
    let nanocodex_unpaired_request_count = nanocodex_requests
        .len()
        .saturating_sub(aligned_request_count);
    let codex_unpaired_request_count = codex_requests.len().saturating_sub(aligned_request_count);
    let mut requests = Vec::with_capacity(request_count);
    let mut first_divergence = None;
    let mut equal_requests = 0_u64;
    let mut differing_requests = 0_u64;
    let mut first_event_loop_divergence = None;
    let mut first_generation_divergence = None;
    let mut equal_event_loop_turns = 0_u64;
    let mut differing_event_loop_turns = 0_u64;
    for offset in 0..request_count {
        let nanocodex = nanocodex_requests.get(offset);
        let codex = codex_requests.get(offset);
        let nanocodex_event_loop_turn = nanocodex_event_loop
            .as_ref()
            .and_then(|trace| trace.turns.get(offset));
        let codex_event_loop_turn = codex_event_loop
            .as_ref()
            .and_then(|trace| trace.turns.get(offset));
        let request_index = u64::try_from(offset).unwrap_or(u64::MAX).saturating_add(1);
        let mut differences = Vec::new();
        match (nanocodex, codex) {
            (Some(nanocodex), Some(codex)) => diff_json(
                "",
                Some(&nanocodex.payload),
                Some(&codex.payload),
                &mut differences,
            ),
            (Some(nanocodex), None) => {
                diff_json("", Some(&nanocodex.payload), None, &mut differences)
            }
            (None, Some(codex)) => {
                diff_json("", None, Some(&codex.payload), &mut differences);
            }
            (None, None) => {}
        }
        let equal = differences.is_empty();
        if offset < aligned_request_count {
            if equal {
                equal_requests = equal_requests.saturating_add(1);
            } else {
                differing_requests = differing_requests.saturating_add(1);
            }
        }
        if !equal && first_divergence.is_none() {
            first_divergence = Some(ApiFirstDivergence {
                request_index,
                pointer: differences
                    .first()
                    .map_or_else(String::new, |difference| difference.pointer.clone()),
            });
        }
        let mut event_loop_differences = Vec::new();
        diff_json(
            "",
            nanocodex_event_loop_turn,
            codex_event_loop_turn,
            &mut event_loop_differences,
        );
        let event_loop_equal = event_loop_differences.is_empty();
        let event_loop_categories = event_loop_difference_categories(&event_loop_differences);
        if offset < aligned_request_count {
            if event_loop_equal {
                equal_event_loop_turns = equal_event_loop_turns.saturating_add(1);
            } else {
                differing_event_loop_turns = differing_event_loop_turns.saturating_add(1);
            }
        }
        if !event_loop_equal && first_event_loop_divergence.is_none() {
            first_event_loop_divergence = Some(ApiEventLoopFirstDivergence {
                request_index,
                pointer: event_loop_differences
                    .first()
                    .map_or_else(String::new, |difference| difference.pointer.clone()),
                categories: event_loop_categories.clone(),
            });
        }
        let generation_turn = [nanocodex, codex].into_iter().flatten().any(|request| {
            request.phase.as_deref() == Some("generation")
                || request
                    .payload
                    .get("generate")
                    .and_then(serde_json::Value::as_bool)
                    != Some(false)
        });
        if !event_loop_equal && generation_turn && first_generation_divergence.is_none() {
            first_generation_divergence = Some(ApiEventLoopFirstDivergence {
                request_index,
                pointer: event_loop_differences
                    .first()
                    .map_or_else(String::new, |difference| difference.pointer.clone()),
                categories: event_loop_categories.clone(),
            });
        }
        requests.push(ApiRequestComparison {
            request_index,
            nanocodex_request_index: nanocodex.map(|request| request.request_index),
            codex_request_index: codex.map(|request| request.request_index),
            nanocodex_phase: nanocodex.and_then(|request| request.phase.clone()),
            codex_phase: codex.and_then(|request| request.phase.clone()),
            equal,
            nanocodex_sha256: nanocodex.map(|request| request.sha256.clone()),
            codex_sha256: codex.map(|request| request.sha256.clone()),
            differences,
            event_loop: ApiEventLoopTurnComparison {
                equal: event_loop_equal,
                categories: event_loop_categories,
                nanocodex: nanocodex_event_loop_turn.cloned(),
                codex: codex_event_loop_turn.cloned(),
                differences: event_loop_differences,
            },
        });
    }
    let chain_invariants_equal = nanocodex_event_loop
        .as_ref()
        .zip(codex_event_loop.as_ref())
        .map(|(nanocodex, codex)| nanocodex.summary.chain_invariants_equal(&codex.summary));
    let model_visible_tool_sequence_equal = nanocodex_event_loop
        .as_ref()
        .zip(codex_event_loop.as_ref())
        .map(|(nanocodex, codex)| {
            nanocodex.summary.model_visible_tool_sequence
                == codex.summary.model_visible_tool_sequence
        });
    let initial_input_text_sections_equal = nanocodex_event_loop
        .as_ref()
        .zip(codex_event_loop.as_ref())
        .map(|(nanocodex, codex)| {
            nanocodex.summary.initial_input_text_sections
                == codex.summary.initial_input_text_sections
        });
    let initial_generation_input_text_sections_equal = nanocodex_event_loop
        .as_ref()
        .zip(codex_event_loop.as_ref())
        .map(|(nanocodex, codex)| {
            nanocodex.summary.initial_generation_input_text_sections
                == codex.summary.initial_generation_input_text_sections
        });
    let initial_code_mode_tool_names_equal = nanocodex_event_loop
        .as_ref()
        .zip(codex_event_loop.as_ref())
        .and_then(|(nanocodex, codex)| {
            nanocodex
                .summary
                .initial_code_mode_tools
                .as_ref()
                .zip(codex.summary.initial_code_mode_tools.as_ref())
                .map(|(nanocodex, codex)| nanocodex == codex)
        });
    let initial_code_mode_tool_definitions_equal = nanocodex_event_loop
        .as_ref()
        .zip(codex_event_loop.as_ref())
        .and_then(|(nanocodex, codex)| {
            nanocodex
                .summary
                .initial_code_mode_tool_definitions
                .as_ref()
                .zip(codex.summary.initial_code_mode_tool_definitions.as_ref())
                .map(|(nanocodex, codex)| nanocodex == codex)
        });
    let nanocodex_unpaired_tail = nanocodex_event_loop
        .as_ref()
        .map(|trace| trace.unpaired_tail(aligned_request_count));
    let codex_unpaired_tail = codex_event_loop
        .as_ref()
        .map(|trace| trace.unpaired_tail(aligned_request_count));
    let event_loop = ApiEventLoopComparison {
        comparable,
        request_count_equal,
        chain_invariants_equal,
        model_visible_tool_sequence_equal,
        initial_input_text_sections_equal,
        initial_generation_input_text_sections_equal,
        initial_code_mode_tool_names_equal,
        initial_code_mode_tool_definitions_equal,
        aligned_turns: u64::try_from(aligned_request_count).unwrap_or(u64::MAX),
        nanocodex_unpaired_turns: u64::try_from(nanocodex_unpaired_request_count)
            .unwrap_or(u64::MAX),
        codex_unpaired_turns: u64::try_from(codex_unpaired_request_count).unwrap_or(u64::MAX),
        equal_turns: equal_event_loop_turns,
        differing_turns: differing_event_loop_turns,
        first_divergence: first_event_loop_divergence,
        first_generation_divergence,
        nanocodex_unpaired_tail,
        codex_unpaired_tail,
        nanocodex: nanocodex_event_loop.map(|trace| trace.summary),
        codex: codex_event_loop.map(|trace| trace.summary),
    };
    let summary = ApiComparisonSummary {
        comparable,
        request_count_equal,
        aligned_requests: u64::try_from(aligned_request_count).unwrap_or(u64::MAX),
        nanocodex_unpaired_requests: u64::try_from(nanocodex_unpaired_request_count)
            .unwrap_or(u64::MAX),
        codex_unpaired_requests: u64::try_from(codex_unpaired_request_count).unwrap_or(u64::MAX),
        equal_requests,
        differing_requests,
        first_divergence: first_divergence.clone(),
        event_loop: event_loop.clone(),
    };
    let report = ApiComparisonReport {
        schema_version: API_COMPARISON_SCHEMA_VERSION,
        comparable,
        request_count_equal,
        aligned_requests: summary.aligned_requests,
        nanocodex_unpaired_requests: summary.nanocodex_unpaired_requests,
        codex_unpaired_requests: summary.codex_unpaired_requests,
        equal_requests,
        differing_requests,
        nanocodex: nanocodex_capture,
        codex: codex_capture,
        first_divergence,
        event_loop,
        requests,
    };
    write_json_atomic(path, &report)?;
    Ok(summary)
}

impl ApiComparisonSummary {
    const fn unavailable() -> Self {
        Self {
            comparable: false,
            request_count_equal: None,
            aligned_requests: 0,
            nanocodex_unpaired_requests: 0,
            codex_unpaired_requests: 0,
            equal_requests: 0,
            differing_requests: 0,
            first_divergence: None,
            event_loop: ApiEventLoopComparison::unavailable(),
        }
    }
}

impl ApiEventLoopComparison {
    const fn unavailable() -> Self {
        Self {
            comparable: false,
            request_count_equal: None,
            chain_invariants_equal: None,
            model_visible_tool_sequence_equal: None,
            initial_input_text_sections_equal: None,
            initial_generation_input_text_sections_equal: None,
            initial_code_mode_tool_names_equal: None,
            initial_code_mode_tool_definitions_equal: None,
            aligned_turns: 0,
            nanocodex_unpaired_turns: 0,
            codex_unpaired_turns: 0,
            equal_turns: 0,
            differing_turns: 0,
            first_divergence: None,
            first_generation_divergence: None,
            nanocodex_unpaired_tail: None,
            codex_unpaired_tail: None,
            nanocodex: None,
            codex: None,
        }
    }
}

fn read_api_request_payloads(path: &Path) -> InternalResult<Vec<ApiRequestPayload>> {
    let input = BufReader::new(File::open(path)?);
    let mut requests = BTreeMap::new();
    for (line_index, line) in input.lines().enumerate() {
        let line = line?;
        let record: serde_json::Value = serde_json::from_str(&line).wrap_err_with(|| {
            format!(
                "invalid API exchange JSON at {}:{}",
                path.display(),
                line_index.saturating_add(1)
            )
        })?;
        let request_index = record
            .get("request_index")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_else(|| {
                u64::try_from(requests.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(1)
            });
        match record.get("direction").and_then(serde_json::Value::as_str) {
            Some("outbound") => {
                let Some(payload) = record_api_events(&record).into_iter().next() else {
                    continue;
                };
                if api_event_type(&payload).as_deref() != Some("response.create") {
                    continue;
                }
                let encoded = serde_json::to_vec(&payload)?;
                requests.insert(
                    request_index,
                    ApiRequestPayload {
                        request_index,
                        phase: record
                            .get("phase")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned),
                        payload,
                        sha256: hex::encode(Sha256::digest(encoded)),
                        response_events: Vec::new(),
                    },
                );
            }
            Some("inbound") => {
                if let Some(request) = requests.get_mut(&request_index) {
                    request.response_events.extend(record_api_events(&record));
                }
            }
            _ => {}
        }
    }
    Ok(requests.into_values().collect())
}

fn record_api_events(record: &serde_json::Value) -> Vec<serde_json::Value> {
    let Some(payload) = record.get("payload") else {
        return Vec::new();
    };
    if let Some(event) = payload.get("event") {
        return vec![event.clone()];
    }
    let Some(text) = payload.get("text").and_then(serde_json::Value::as_str) else {
        return Vec::new();
    };
    if let Ok(event) = serde_json::from_str(text) {
        return vec![event];
    }
    text.lines()
        .filter_map(|line| {
            let data = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
            (data != "[DONE]")
                .then(|| serde_json::from_str(data).ok())
                .flatten()
        })
        .collect()
}

#[derive(Clone, Copy)]
enum EventLoopValueStage {
    Request,
    Response,
}

struct EventLoopNormalizeContext<'a> {
    stage: EventLoopValueStage,
    first_prompt_cache_key: Option<&'a str>,
    previous_response_id: Option<&'a str>,
    previous_call_ids: &'a BTreeSet<String>,
    replayed_call_ids: &'a BTreeSet<String>,
}

fn build_event_loop_trace(requests: &[ApiRequestPayload]) -> ApiEventLoopTrace {
    let initial_input_text_sections = requests
        .first()
        .map_or_else(Vec::new, |request| input_text_sections(&request.payload));
    let initial_generation_input_text_sections = requests
        .iter()
        .find(|request| request.phase.as_deref() == Some("generation"))
        .or_else(|| {
            requests.iter().find(|request| {
                request
                    .payload
                    .get("generate")
                    .and_then(serde_json::Value::as_bool)
                    != Some(false)
            })
        })
        .map_or_else(Vec::new, |request| input_text_sections(&request.payload));
    let initial_model = requests
        .first()
        .and_then(|request| request.payload.get("model"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let initial_reasoning_effort = requests
        .first()
        .and_then(|request| request.payload.pointer("/reasoning/effort"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let initial_reasoning_summary = requests
        .first()
        .and_then(|request| request.payload.pointer("/reasoning/summary"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let initial_visible_tools = requests
        .first()
        .map_or_else(Vec::new, |request| visible_tool_names(&request.payload));
    let initial_code_mode_tool_definitions = requests
        .first()
        .and_then(|request| code_mode_tool_definitions(&request.payload));
    let initial_code_mode_tools = initial_code_mode_tool_definitions
        .as_ref()
        .map(|definitions| {
            definitions
                .iter()
                .map(|definition| definition.name.clone())
                .collect()
        });
    let first_prompt_cache_key = requests
        .first()
        .and_then(|request| request.payload.get("prompt_cache_key"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let mut prompt_cache_key_stable = first_prompt_cache_key.as_ref().map(|_| true);
    let mut previous_response_id = None;
    let mut previous_call_ids = BTreeSet::new();
    let mut previous_response_links = 0_u64;
    let mut full_history_replays = 0_u64;
    let mut full_history_replays_after_nonterminal_turn = 0_u64;
    let mut broken_previous_response_links = 0_u64;
    let mut tool_result_links = 0_u64;
    let mut replayed_tool_result_links = 0_u64;
    let mut broken_tool_result_links = 0_u64;
    let mut generation_turns = 0_u64;
    let mut terminal_turns = 0_u64;
    let mut tool_call_turns = 0_u64;
    let mut model_visible_tool_sequence = Vec::new();
    let mut detected_poll_only_turns = 0_u64;
    let mut consecutive_detected_poll_only_turns = 0_u64;
    let mut max_consecutive_detected_poll_only_turns = 0_u64;
    let mut detected_empty_stdin_calls = 0_u64;
    let mut detected_poll_only_input_tokens = 0_u64;
    let mut detected_poll_only_cached_tokens = 0_u64;
    let mut detected_poll_only_output_tokens = 0_u64;
    let mut turns = Vec::with_capacity(requests.len());
    let mut turn_metrics = Vec::with_capacity(requests.len());
    let mut previous_turn_terminal = false;

    for (offset, request) in requests.iter().enumerate() {
        let generation = request
            .payload
            .get("generate")
            .and_then(serde_json::Value::as_bool)
            != Some(false);
        if generation {
            generation_turns = generation_turns.saturating_add(1);
        }
        let prompt_cache_key = request
            .payload
            .get("prompt_cache_key")
            .and_then(serde_json::Value::as_str);
        if first_prompt_cache_key.is_some() && prompt_cache_key != first_prompt_cache_key.as_deref()
        {
            prompt_cache_key_stable = Some(false);
        }

        let request_previous_response_id = request
            .payload
            .get("previous_response_id")
            .and_then(serde_json::Value::as_str);
        let replayed_call_ids = request_call_ids(&request.payload);
        let full_history_replay =
            request_previous_response_id.is_none() && request_replays_history(&request.payload);
        if offset > 0 {
            if request_previous_response_id.is_some()
                && request_previous_response_id == previous_response_id.as_deref()
            {
                previous_response_links = previous_response_links.saturating_add(1);
            } else if full_history_replay {
                full_history_replays = full_history_replays.saturating_add(1);
                if !previous_turn_terminal {
                    full_history_replays_after_nonterminal_turn =
                        full_history_replays_after_nonterminal_turn.saturating_add(1);
                }
            } else {
                broken_previous_response_links = broken_previous_response_links.saturating_add(1);
            }
        }

        for call_id in request_tool_result_call_ids(&request.payload) {
            if previous_call_ids.contains(call_id) {
                tool_result_links = tool_result_links.saturating_add(1);
            } else if replayed_call_ids.contains(call_id) {
                tool_result_links = tool_result_links.saturating_add(1);
                replayed_tool_result_links = replayed_tool_result_links.saturating_add(1);
            } else {
                broken_tool_result_links = broken_tool_result_links.saturating_add(1);
            }
        }

        let request_context = EventLoopNormalizeContext {
            stage: EventLoopValueStage::Request,
            first_prompt_cache_key: first_prompt_cache_key.as_deref(),
            previous_response_id: previous_response_id.as_deref(),
            previous_call_ids: &previous_call_ids,
            replayed_call_ids: &replayed_call_ids,
        };
        let normalized_request =
            normalize_event_loop_value(&request.payload, None, &request_context);
        let response_context = EventLoopNormalizeContext {
            stage: EventLoopValueStage::Response,
            first_prompt_cache_key: first_prompt_cache_key.as_deref(),
            previous_response_id: previous_response_id.as_deref(),
            previous_call_ids: &previous_call_ids,
            replayed_call_ids: &replayed_call_ids,
        };
        let normalized_response =
            event_loop_response_signature(&request.response_events, &response_context);
        let response_tools = response_tool_items(&request.response_events)
            .filter_map(visible_tool_name)
            .collect::<Vec<_>>();
        let response_tool_count = u64::try_from(response_tools.len()).unwrap_or(u64::MAX);
        if response_tool_count > 0 {
            tool_call_turns = tool_call_turns.saturating_add(1);
        }
        model_visible_tool_sequence.extend(response_tools);
        let detected_poll_only = if generation {
            if let Some(polling) = detected_polling_turn(&request.response_events) {
                detected_poll_only_turns = detected_poll_only_turns.saturating_add(1);
                consecutive_detected_poll_only_turns =
                    consecutive_detected_poll_only_turns.saturating_add(1);
                max_consecutive_detected_poll_only_turns = max_consecutive_detected_poll_only_turns
                    .max(consecutive_detected_poll_only_turns);
                detected_empty_stdin_calls =
                    detected_empty_stdin_calls.saturating_add(polling.empty_stdin_calls);
                detected_poll_only_input_tokens =
                    detected_poll_only_input_tokens.saturating_add(polling.input_tokens);
                detected_poll_only_cached_tokens =
                    detected_poll_only_cached_tokens.saturating_add(polling.cached_tokens);
                detected_poll_only_output_tokens =
                    detected_poll_only_output_tokens.saturating_add(polling.output_tokens);
                true
            } else {
                consecutive_detected_poll_only_turns = 0;
                false
            }
        } else {
            false
        };
        let turn_terminal = request
            .response_events
            .iter()
            .any(|event| api_event_type(event).is_some_and(|kind| is_terminal_api_event(&kind)));
        if turn_terminal {
            terminal_turns = terminal_turns.saturating_add(1);
        }
        turns.push(serde_json::json!({
            "phase": request.phase,
            "request": normalized_request,
            "response": normalized_response,
        }));
        turn_metrics.push(ApiEventLoopTurnMetrics {
            generation,
            tool_calls: response_tool_count,
            detected_poll_only,
            usage: api_response_usage(&request.response_events),
        });

        previous_response_id = response_id(&request.response_events);
        previous_call_ids = response_call_ids(&request.response_events);
        previous_turn_terminal = turn_terminal;
    }

    ApiEventLoopTrace {
        turns,
        turn_metrics,
        summary: ApiEventLoopArmSummary {
            turns: u64::try_from(requests.len()).unwrap_or(u64::MAX),
            generation_turns,
            terminal_turns,
            tool_call_turns,
            model_visible_tool_calls: u64::try_from(model_visible_tool_sequence.len())
                .unwrap_or(u64::MAX),
            model_visible_tool_sequence,
            initial_model,
            initial_reasoning_effort,
            initial_reasoning_summary,
            initial_visible_tools,
            initial_input_text_sections,
            initial_generation_input_text_sections,
            initial_code_mode_tools,
            initial_code_mode_tool_definitions,
            detected_poll_only_turns,
            max_consecutive_detected_poll_only_turns,
            detected_empty_stdin_calls,
            detected_poll_only_input_tokens,
            detected_poll_only_cached_tokens,
            detected_poll_only_output_tokens,
            prompt_cache_key_stable,
            previous_response_links,
            full_history_replays,
            full_history_replays_after_nonterminal_turn,
            broken_previous_response_links,
            tool_result_links,
            replayed_tool_result_links,
            broken_tool_result_links,
        },
    }
}

fn input_text_sections(request: &serde_json::Value) -> Vec<ApiInputTextSectionSummary> {
    request
        .get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .flat_map(|(item_ordinal, item)| {
            let role = item
                .get("role")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned();
            item.get("content")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .enumerate()
                .filter_map(move |(content_ordinal, content)| {
                    if content.get("type").and_then(serde_json::Value::as_str) != Some("input_text")
                    {
                        return None;
                    }
                    let text = content.get("text").and_then(serde_json::Value::as_str)?;
                    Some(ApiInputTextSectionSummary {
                        item_ordinal: u64::try_from(item_ordinal).unwrap_or(u64::MAX),
                        content_ordinal: u64::try_from(content_ordinal).unwrap_or(u64::MAX),
                        role: role.clone(),
                        label: input_text_label(text),
                        text_bytes: u64::try_from(text.len()).unwrap_or(u64::MAX),
                        text_sha256: hex::encode(Sha256::digest(text.as_bytes())),
                    })
                })
        })
        .collect()
}

fn input_text_label(text: &str) -> String {
    let text = text.trim_start();
    if text.starts_with("# AGENTS.md instructions") {
        return "agents_md".to_owned();
    }
    if let Some(tag) = text.strip_prefix('<').and_then(|text| {
        let end = text.find(|character: char| character == '>' || character.is_whitespace())?;
        (end > 0).then(|| &text[..end])
    }) {
        return tag.to_owned();
    }
    "plain_text".to_owned()
}

fn visible_tool_names(request: &serde_json::Value) -> Vec<String> {
    visible_tools(request)
        .filter_map(visible_tool_name)
        .collect::<Vec<_>>()
}

fn visible_tools(request: &serde_json::Value) -> impl Iterator<Item = &serde_json::Value> {
    request
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .chain(
            request
                .get("input")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter(|item| {
                    item.get("type").and_then(serde_json::Value::as_str) == Some("additional_tools")
                })
                .flat_map(|item| {
                    item.get("tools")
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                }),
        )
}

fn code_mode_tool_definitions(
    request: &serde_json::Value,
) -> Option<Vec<ApiCodeModeToolDefinitionSummary>> {
    let description = visible_tools(request)
        .find(|tool| visible_tool_name(tool).as_deref() == Some("exec"))?
        .get("description")
        .and_then(serde_json::Value::as_str)?;
    let mut definitions = Vec::new();
    let mut current_name = None::<String>;
    let mut current_section = String::new();

    for line in description.lines() {
        if let Some(name) = line
            .strip_prefix("### `")
            .and_then(|name| name.strip_suffix('`'))
        {
            if let Some(name) = current_name.take() {
                definitions.push(code_mode_tool_definition_summary(
                    name,
                    &current_section,
                    definitions.len(),
                ));
            }
            current_name = Some(name.to_owned());
            current_section.clear();
        }
        if current_name.is_some() {
            current_section.push_str(line);
            current_section.push('\n');
        }
    }
    if let Some(name) = current_name {
        definitions.push(code_mode_tool_definition_summary(
            name,
            &current_section,
            definitions.len(),
        ));
    }

    (!definitions.is_empty()).then_some(definitions)
}

fn code_mode_tool_definition_summary(
    name: String,
    section: &str,
    ordinal: usize,
) -> ApiCodeModeToolDefinitionSummary {
    ApiCodeModeToolDefinitionSummary {
        name,
        ordinal: u64::try_from(ordinal).unwrap_or(u64::MAX),
        section_bytes: u64::try_from(section.len()).unwrap_or(u64::MAX),
        section_sha256: hex::encode(Sha256::digest(section.as_bytes())),
    }
}

fn visible_tool_name(tool: &serde_json::Value) -> Option<String> {
    tool.get("name")
        .or_else(|| tool.get("type"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn response_tool_items(events: &[serde_json::Value]) -> impl Iterator<Item = &serde_json::Value> {
    events
        .iter()
        .filter(|event| api_event_type(event).as_deref() == Some("response.output_item.done"))
        .filter_map(|event| event.get("item"))
        .filter(|item| {
            item.get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|kind| kind == "function_call" || kind.ends_with("_tool_call"))
        })
}

fn api_response_usage(events: &[serde_json::Value]) -> Option<ApiTokenUsageSummary> {
    let usage = events
        .iter()
        .rev()
        .find_map(|event| event.pointer("/response/usage"))?;
    let input_tokens = usage
        .get("input_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let cached_input_tokens = usage
        .pointer("/input_tokens_details/cached_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let output_tokens = usage
        .get("output_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let reasoning_output_tokens = usage
        .pointer("/output_tokens_details/reasoning_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let total_tokens = usage
        .get("total_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| input_tokens.saturating_add(output_tokens));
    Some(ApiTokenUsageSummary {
        input_tokens,
        cached_input_tokens,
        uncached_input_tokens: input_tokens.saturating_sub(cached_input_tokens),
        output_tokens,
        reasoning_output_tokens,
        total_tokens,
    })
}

fn detected_polling_turn(events: &[serde_json::Value]) -> Option<DetectedPollingTurn> {
    let tool_items = response_tool_items(events).collect::<Vec<_>>();
    if tool_items.is_empty() {
        return None;
    }
    let empty_stdin_calls = tool_items.iter().try_fold(0_u64, |total, item| {
        detected_empty_stdin_calls(item).map(|calls| total.saturating_add(calls))
    })?;
    let usage = api_response_usage(events).unwrap_or_default();
    Some(DetectedPollingTurn {
        empty_stdin_calls,
        input_tokens: usage.input_tokens,
        cached_tokens: usage.cached_input_tokens,
        output_tokens: usage.output_tokens,
    })
}

fn detected_empty_stdin_calls(item: &serde_json::Value) -> Option<u64> {
    let name = item.get("name").and_then(serde_json::Value::as_str)?;
    let kind = item.get("type").and_then(serde_json::Value::as_str)?;
    if kind == "function_call" && name == "write_stdin" {
        let arguments = item.get("arguments")?;
        let arguments = if let Some(arguments) = arguments.as_str() {
            serde_json::from_str(arguments).ok()?
        } else {
            arguments.clone()
        };
        return match arguments.get("chars") {
            None => Some(1),
            Some(serde_json::Value::String(chars)) if chars.is_empty() => Some(1),
            _ => None,
        };
    }
    if kind != "custom_tool_call" || name != "exec" {
        return None;
    }
    let source = item.get("input").and_then(serde_json::Value::as_str)?;
    detected_code_mode_empty_stdin_calls(source)
}

fn detected_code_mode_empty_stdin_calls(source: &str) -> Option<u64> {
    let write_stdin_calls = source.matches("tools.write_stdin").count();
    if write_stdin_calls == 0 || source.matches("tools.").count() != write_stdin_calls {
        return None;
    }
    let compact = source
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>();
    if compact.contains("chars:")
        && !compact.contains("chars:\"\"")
        && !compact.contains("chars:''")
        && !compact.contains("\"chars\":\"\"")
        && !compact.contains("'chars':''")
    {
        return None;
    }
    Some(u64::try_from(write_stdin_calls).unwrap_or(u64::MAX))
}

fn request_tool_result_call_ids(request: &serde_json::Value) -> Vec<&str> {
    request
        .get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| {
            item.get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|kind| kind.ends_with("_call_output"))
        })
        .filter_map(|item| item.get("call_id").and_then(serde_json::Value::as_str))
        .collect()
}

fn request_call_ids(request: &serde_json::Value) -> BTreeSet<String> {
    request
        .get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| {
            item.get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|kind| kind.ends_with("_call") && !kind.ends_with("_call_output"))
        })
        .filter_map(|item| item.get("call_id").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect()
}

fn request_replays_history(request: &serde_json::Value) -> bool {
    request
        .get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .any(|item| {
            item.get("role").and_then(serde_json::Value::as_str) == Some("assistant")
                || item
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|kind| {
                        kind == "reasoning"
                            || (kind.ends_with("_call") && !kind.ends_with("_call_output"))
                    })
        })
}

fn response_id(events: &[serde_json::Value]) -> Option<String> {
    events
        .iter()
        .filter_map(|event| {
            event
                .pointer("/response/id")
                .and_then(serde_json::Value::as_str)
        })
        .next_back()
        .map(str::to_owned)
}

fn response_call_ids(events: &[serde_json::Value]) -> BTreeSet<String> {
    events
        .iter()
        .filter(|event| api_event_type(event).as_deref() == Some("response.output_item.done"))
        .filter_map(|event| {
            event
                .pointer("/item/call_id")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_owned)
        .collect()
}

fn event_loop_response_signature(
    events: &[serde_json::Value],
    context: &EventLoopNormalizeContext<'_>,
) -> serde_json::Value {
    let semantic_events = events
        .iter()
        .filter_map(api_event_type)
        .filter(|kind| is_semantic_response_event(kind))
        .collect::<Vec<_>>();
    let output_items = events
        .iter()
        .filter(|event| api_event_type(event).as_deref() == Some("response.output_item.done"))
        .filter_map(|event| event.get("item"))
        .map(|item| normalize_event_loop_value(item, None, context))
        .collect::<Vec<_>>();
    let terminal = events
        .iter()
        .rev()
        .find_map(|event| {
            let kind = api_event_type(event)?;
            is_terminal_api_event(&kind).then(|| {
                serde_json::json!({
                    "type": kind,
                    "status": event.pointer("/response/status"),
                    "error": event
                        .get("error")
                        .map(|error| normalize_event_loop_value(error, None, context)),
                })
            })
        })
        .unwrap_or(serde_json::Value::Null);
    serde_json::json!({
        "semantic_events": semantic_events,
        "output_items": output_items,
        "terminal": terminal,
    })
}

fn is_terminal_api_event(kind: &str) -> bool {
    matches!(kind, "response.completed" | "response.failed" | "error")
}

fn is_semantic_response_event(kind: &str) -> bool {
    !kind.ends_with(".delta")
        && !kind.ends_with(".added")
        && !matches!(
            kind,
            "response.in_progress"
                | "codex.rate_limits"
                | "codex.response.metadata"
                | "responsesapi.websocket_timing"
        )
}

fn normalize_event_loop_value(
    value: &serde_json::Value,
    key: Option<&str>,
    context: &EventLoopNormalizeContext<'_>,
) -> serde_json::Value {
    match key {
        Some("client_metadata") => return normalize_client_metadata(value),
        Some("prompt_cache_key") => {
            return serde_json::Value::String(value.as_str().map_or_else(
                || "missing".to_owned(),
                |key| {
                    if Some(key) == context.first_prompt_cache_key {
                        "stable".to_owned()
                    } else {
                        "changed".to_owned()
                    }
                },
            ));
        }
        Some("previous_response_id") => {
            return serde_json::Value::String(value.as_str().map_or_else(
                || "missing".to_owned(),
                |response_id| {
                    if Some(response_id) == context.previous_response_id {
                        "matches_previous_response".to_owned()
                    } else {
                        "present_unmatched".to_owned()
                    }
                },
            ));
        }
        Some("call_id") => {
            return serde_json::Value::String(match (context.stage, value.as_str()) {
                (EventLoopValueStage::Request, Some(call_id))
                    if context.previous_call_ids.contains(call_id) =>
                {
                    "matches_previous_output".to_owned()
                }
                (EventLoopValueStage::Request, Some(call_id))
                    if context.replayed_call_ids.contains(call_id) =>
                {
                    "matches_replayed_output".to_owned()
                }
                (EventLoopValueStage::Request, Some(_)) => "present_unmatched".to_owned(),
                (EventLoopValueStage::Response, Some(_)) => "present".to_owned(),
                (_, None) => "missing".to_owned(),
            });
        }
        Some(
            "text" | "description" | "instructions" | "arguments" | "encrypted_content"
            | "signature",
        ) if value.is_string() => return string_fingerprint(value.as_str().unwrap_or_default()),
        Some("input" | "output") if value.is_string() => {
            return string_fingerprint(value.as_str().unwrap_or_default());
        }
        _ => {}
    }

    match value {
        serde_json::Value::Object(object) => {
            let mut normalized = serde_json::Map::new();
            for (child_key, child) in object {
                if matches!(
                    child_key.as_str(),
                    "id" | "internal_chat_message_metadata_passthrough"
                ) {
                    continue;
                }
                normalized.insert(
                    child_key.clone(),
                    normalize_event_loop_value(child, Some(child_key), context),
                );
            }
            serde_json::Value::Object(normalized)
        }
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|value| normalize_event_loop_value(value, None, context))
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn normalize_client_metadata(value: &serde_json::Value) -> serde_json::Value {
    let Some(metadata) = value.as_object() else {
        return value.clone();
    };
    let mut normalized = serde_json::Map::new();
    for (key, value) in metadata {
        if matches!(
            key.as_str(),
            "session_id"
                | "thread_id"
                | "turn_id"
                | "x-codex-installation-id"
                | "x-codex-turn-metadata"
                | "x-codex-window-id"
                | "x-codex-ws-stream-request-start-ms"
        ) {
            continue;
        }
        normalized.insert(key.clone(), value.clone());
    }
    serde_json::Value::Object(normalized)
}

fn string_fingerprint(value: &str) -> serde_json::Value {
    serde_json::json!({
        "bytes": value.len(),
        "sha256": hex::encode(Sha256::digest(value.as_bytes())),
    })
}

fn event_loop_difference_categories(differences: &[ApiJsonDifference]) -> Vec<String> {
    differences
        .iter()
        .map(|difference| event_loop_difference_category(&difference.pointer))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn event_loop_difference_category(pointer: &str) -> &'static str {
    if pointer.contains("/tools/") || pointer.ends_with("/tools") {
        "tool_configuration"
    } else if pointer.starts_with("/request/reasoning") {
        "reasoning_policy"
    } else if pointer.starts_with("/request/prompt_cache_key")
        || pointer.starts_with("/request/previous_response_id")
    {
        "response_chain"
    } else if pointer.starts_with("/request/input") {
        "request_context"
    } else if pointer.starts_with("/response/semantic_events") {
        "response_event_sequence"
    } else if pointer.starts_with("/response/output_items") {
        "model_output"
    } else if pointer.starts_with("/response/terminal") {
        "terminal_response"
    } else if pointer.starts_with("/phase") {
        "turn_alignment"
    } else if pointer.starts_with("/request") {
        "request_configuration"
    } else {
        "turn_presence"
    }
}

fn diff_json(
    pointer: &str,
    nanocodex: Option<&serde_json::Value>,
    codex: Option<&serde_json::Value>,
    differences: &mut Vec<ApiJsonDifference>,
) {
    match (nanocodex, codex) {
        (Some(serde_json::Value::Object(nanocodex)), Some(serde_json::Value::Object(codex))) => {
            let keys = nanocodex
                .keys()
                .chain(codex.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            for key in keys {
                diff_json(
                    &json_pointer_child(pointer, &key),
                    nanocodex.get(&key),
                    codex.get(&key),
                    differences,
                );
            }
        }
        (Some(serde_json::Value::Array(nanocodex)), Some(serde_json::Value::Array(codex))) => {
            for index in 0..nanocodex.len().max(codex.len()) {
                diff_json(
                    &json_pointer_child(pointer, &index.to_string()),
                    nanocodex.get(index),
                    codex.get(index),
                    differences,
                );
            }
        }
        (Some(nanocodex), Some(codex)) if nanocodex == codex => {}
        (nanocodex, codex) => differences.push(ApiJsonDifference {
            pointer: pointer.to_owned(),
            nanocodex: nanocodex.map_or(ApiJsonSide::Missing, |value| ApiJsonSide::Value {
                value: value.clone(),
            }),
            codex: codex.map_or(ApiJsonSide::Missing, |value| ApiJsonSide::Value {
                value: value.clone(),
            }),
        }),
    }
}

fn json_pointer_child(parent: &str, key: &str) -> String {
    format!("{parent}/{}", key.replace('~', "~0").replace('/', "~1"))
}

fn prepare_output_parent(output: &Path) -> InternalResult<PathBuf> {
    fs::create_dir_all(output)
        .wrap_err_with(|| format!("failed to create output directory {}", output.display()))?;
    output
        .canonicalize()
        .wrap_err_with(|| format!("failed to resolve output directory {}", output.display()))
}

fn outcome_directory(outcome: &EvalAttemptOutcome) -> &Path {
    match outcome {
        EvalAttemptOutcome::Scored(result) => &result.artifacts.directory,
        EvalAttemptOutcome::Unscored(failure) => &failure.artifacts.directory,
    }
}

const fn outcome_task(outcome: &EvalAttemptOutcome) -> &Task {
    match outcome {
        EvalAttemptOutcome::Scored(result) => result.task(),
        EvalAttemptOutcome::Unscored(failure) => failure.task(),
    }
}

const fn outcome_agent(outcome: &EvalAttemptOutcome) -> Option<&AgentResult> {
    match outcome {
        EvalAttemptOutcome::Scored(result) => result.agent.as_ref(),
        EvalAttemptOutcome::Unscored(failure) => failure.agent.as_ref(),
    }
}

fn retained_file(path: PathBuf) -> Option<PathBuf> {
    path.is_file().then_some(path)
}

fn count_u32(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

fn signed_u64_delta(left: u64, right: u64) -> i64 {
    i64::try_from(left)
        .unwrap_or(i64::MAX)
        .saturating_sub(i64::try_from(right).unwrap_or(i64::MAX))
}

const fn agent_duration_ms(agent: &AgentResult) -> u64 {
    agent.metadata.duration_ms
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn file_sha256(path: &Path) -> InternalResult<String> {
    let mut file =
        File::open(path).wrap_err_with(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .wrap_err_with(|| format!("failed to hash {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> InternalResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| diff_error!("comparison path has no parent: {}", path.display()))?;
    let mut temporary = NamedTempFile::new_in(parent)
        .wrap_err_with(|| format!("failed to create temporary file in {}", parent.display()))?;
    serde_json::to_writer_pretty(temporary.as_file_mut(), value)?;
    temporary.as_file_mut().write_all(b"\n")?;
    temporary.as_file_mut().sync_all()?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .wrap_err_with(|| format!("failed to publish {}", path.display()))?;
    Ok(())
}

fn append_arm_summary(output: &mut String, name: &str, arm: &ArmReport) {
    let status = arm.summary.status.as_str();
    let reward = arm
        .summary
        .rewards
        .iter()
        .map(|(name, reward)| format!("{name}={reward}"))
        .collect::<Vec<_>>()
        .join(",");
    let tools = arm
        .summary
        .tool_calls
        .map_or_else(|| "unknown".to_owned(), |calls| calls.to_string());
    if reward.is_empty() {
        let _ = writeln!(output, "{name}: {status} observed_tool_events={tools}");
    } else {
        let _ = writeln!(
            output,
            "{name}: {status} {reward} observed_tool_events={tools}"
        );
    }
    if let Some(trajectory) = &arm.trajectory {
        let _ = writeln!(output, "{name} trajectory: {}", trajectory.display());
    }
    if let Some(summary) = &arm.trajectory_summary {
        let polling = &summary.shell_polling;
        let _ = writeln!(
            output,
            "{name} trajectory tools: {} [{}] ({})",
            summary.tool_calls,
            summary.tool_sequence.join(", "),
            summary.tool_projection,
        );
        let _ = writeln!(
            output,
            "{name} shell polling: {} observed poll-only steps · {} confirmed poll-only model calls · {} input/{} output tokens · {:.1}s model time",
            polling.poll_only_steps,
            polling
                .confirmed_model_calls
                .map_or_else(|| "unavailable".to_owned(), |calls| calls.to_string()),
            polling.prompt_tokens,
            polling.completion_tokens,
            Duration::from_nanos(polling.model_duration_ns).as_secs_f64(),
        );
    }
    if let Some(error) = &arm.operational_error {
        let _ = writeln!(output, "{name} runner error: {error}");
    }
    if let Some(error) = &arm.event_error {
        let _ = writeln!(output, "{name} event error: {error}");
    }
    if let Some(error) = &arm.trajectory_error {
        let _ = writeln!(output, "{name} trajectory error: {error}");
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::convert::Infallible;

    use std::{
        fs,
        os::unix::fs::PermissionsExt as _,
        path::{Path, PathBuf},
    };

    use nanocodex_agent::{Nanocodex, OpenAi};
    use nanocodex_oai_api::MODEL;
    use tempfile::tempdir;

    use crate::{
        AgentStatus, AtifStep, AtifTrajectory, AttemptAgent, EvalAttemptOutcome, EvalEventKind,
        EvalStatus, VerifierResult,
    };

    use super::{
        ApiEventLoopTailSummary, ApiRequestPayload, ApiTokenUsageSummary, ArmStatus, CodexExec,
        CodexVersion, DIFF_CODEX_CA_BUNDLE_FILENAME, DIFF_CODEX_CLOUD_CONFIG_CACHE_FILENAME,
        DIFF_CODEX_SSL_CERT_FILE_ENVIRONMENT, DiffCodexCaSource, DiffProgress, Evaluator,
        LaneProgressState, ShellPollingSummary, Task, TrajectoryProjection, build_event_loop_trace,
        compare_api_exchanges, detected_code_mode_empty_stdin_calls, detected_polling_turn,
        diff_json, event_loop_difference_categories, heartbeat_needed, heartbeat_summary,
        inspect_api_exchanges, newly_completed_lines, read_api_request_payloads,
        read_optional_codex_cloud_config_cache, reanalyze, run_arm, stage_diff_codex_ca_bundle,
        validate_matched_code_mode_only_profile,
    };

    #[test]
    fn reanalysis_keeps_missing_refusal_trajectories_unavailable() {
        let directory = tempdir().unwrap();
        fs::write(
            directory.path().join("comparison.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "model": "gpt-5.6-sol",
                "thinking": "medium",
                "nanocodex": {
                    "trajectory": null
                },
                "codex": {
                    "trajectory": null
                },
                "artifacts": {}
            }))
            .unwrap(),
        )
        .unwrap();

        let rebuilt = reanalyze(directory.path()).unwrap();

        assert_eq!(
            rebuilt
                .comparison()
                .pointer("/trajectory_comparison/comparable"),
            Some(&serde_json::json!(false))
        );
        assert_eq!(
            rebuilt
                .comparison()
                .pointer("/nanocodex/trajectory_summary"),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(
            rebuilt.comparison().pointer("/codex/trajectory_summary"),
            Some(&serde_json::Value::Null)
        );
        assert!(
            rebuilt
                .human_summary()
                .contains("codex trajectory: unavailable")
        );
    }

    #[test]
    fn codex_auth_stages_only_the_adjacent_cloud_config_cache() {
        let codex_home = tempfile::tempdir().unwrap();
        let auth_file = codex_home.path().join("auth.json");
        fs::write(&auth_file, b"auth").unwrap();
        assert_eq!(
            read_optional_codex_cloud_config_cache(&auth_file).unwrap(),
            None
        );

        let cache_file = codex_home
            .path()
            .join(DIFF_CODEX_CLOUD_CONFIG_CACHE_FILENAME);
        fs::write(&cache_file, b"signed cloud config").unwrap();
        fs::write(codex_home.path().join("config.toml"), b"ignored").unwrap();
        assert_eq!(
            read_optional_codex_cloud_config_cache(&auth_file).unwrap(),
            Some(b"signed cloud config".to_vec())
        );
    }

    #[test]
    fn codex_ca_bundle_is_staged_read_only() {
        let source_directory = tempfile::tempdir().unwrap();
        let source = source_directory.path().join("host-ca.pem");
        fs::write(&source, b"host CA bundle").unwrap();
        let share = tempfile::tempdir().unwrap();

        let staged = stage_diff_codex_ca_bundle(
            &DiffCodexCaSource {
                path: source,
                source_environment: "test",
                guest_environment: DIFF_CODEX_SSL_CERT_FILE_ENVIRONMENT,
            },
            share.path(),
        )
        .unwrap();
        let staged_path = share.path().join(DIFF_CODEX_CA_BUNDLE_FILENAME);
        assert_eq!(
            staged.guest_environment,
            DIFF_CODEX_SSL_CERT_FILE_ENVIRONMENT
        );
        assert_eq!(fs::read(&staged_path).unwrap(), b"host CA bundle");
        assert_eq!(
            fs::metadata(staged_path).unwrap().permissions().mode() & 0o777,
            0o444
        );
    }

    #[test]
    fn codex_progress_waits_for_complete_jsonl_records() {
        let (lines, offset) = newly_completed_lines(b"first\nsecond", 0, false);
        assert_eq!(lines, [b"first".as_slice()]);
        assert_eq!(offset, 6);

        let (lines, offset) = newly_completed_lines(b"first\nsecond\nthird\n", offset, false);
        assert_eq!(lines, [b"second".as_slice(), b"third".as_slice()]);
        assert_eq!(offset, 19);

        let (lines, offset) = newly_completed_lines(b"first\nsecond\nthird\nfinal", offset, true);
        assert_eq!(lines, [b"final".as_slice()]);
        assert_eq!(offset, 24);
    }

    #[test]
    fn heartbeat_reports_each_lanes_last_observed_state() {
        let mut lanes = std::collections::BTreeMap::new();
        lanes.insert(
            "nanocodex",
            LaneProgressState {
                elapsed_ms: 10_000,
                kind: "model.call.started".to_owned(),
                summary: Some("call 8".to_owned()),
            },
        );
        lanes.insert(
            "codex",
            LaneProgressState {
                elapsed_ms: 15_000,
                kind: "command_execution.started".to_owned(),
                summary: Some("apt-get install r-base".to_owned()),
            },
        );

        assert!(heartbeat_needed(&lanes));
        assert_eq!(
            heartbeat_summary(&lanes, 25_000),
            "nanocodex: model.call.started (call 8) for 15.0s · codex: command_execution.started (apt-get install r-base) for 10.0s"
        );

        lanes.get_mut("nanocodex").unwrap().kind = "attempt.completed".to_owned();
        lanes.get_mut("codex").unwrap().kind = "attempt.completed".to_owned();
        assert!(!heartbeat_needed(&lanes));
    }

    #[tokio::test]
    async fn progress_log_retains_interleaved_lanes_in_observation_order() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("progress.jsonl");
        let (progress, recorder) = DiffProgress::start(path.clone(), tokio::time::Instant::now())
            .await
            .unwrap();

        progress.emit("nanocodex", "model.call.started", "call 1");
        progress.observe_codex(&serde_json::json!({
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "printf hello",
                "exit_code": 0,
                "status": "completed"
            }
        }));
        progress.emit("nanocodex", "tool.call", "exec_command");
        recorder.finish(progress).await.unwrap();

        let records = fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0]["sequence"], 1);
        assert_eq!(records[0]["arm"], "nanocodex");
        assert_eq!(records[1]["sequence"], 2);
        assert_eq!(records[1]["arm"], "codex");
        assert_eq!(records[1]["kind"], "item.completed");
        assert_eq!(
            records[1]["summary"],
            "command_execution · printf hello · exit 0 · completed"
        );
        assert_eq!(records[2]["sequence"], 3);
        assert_eq!(records[2]["arm"], "nanocodex");
    }

    #[tokio::test]
    async fn progress_log_heartbeats_during_quiet_lane_work() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("progress.jsonl");
        let (progress, recorder) = DiffProgress::start_with_heartbeat(
            path.clone(),
            tokio::time::Instant::now(),
            std::time::Duration::from_millis(5),
        )
        .await
        .unwrap();
        progress.emit("nanocodex", "model.call.started", "call 8");
        progress.emit("codex", "item.started", "command_execution · apt-get");

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        let heartbeat = loop {
            let contents = fs::read_to_string(&path).unwrap();
            if let Some(record) = contents.lines().find_map(|line| {
                let record = serde_json::from_str::<serde_json::Value>(line).unwrap();
                (record["kind"] == "heartbeat").then_some(record)
            }) {
                break record;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "progress recorder did not flush a heartbeat"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        };
        recorder.finish(progress).await.unwrap();

        assert_eq!(heartbeat["arm"], "runner");
        let summary = heartbeat["summary"].as_str().unwrap();
        assert!(summary.contains("nanocodex: model.call.started (call 8)"));
        assert!(summary.contains("codex: item.started (command_execution · apt-get)"));
    }

    #[tokio::test]
    async fn progress_lane_moves_from_agent_completion_into_verifier_work() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("progress.jsonl");
        let (progress, recorder) = DiffProgress::start(path.clone(), tokio::time::Instant::now())
            .await
            .unwrap();
        progress.emit("nanocodex", "run.completed", "model calls 9");
        progress.observe_evaluator("nanocodex", &EvalEventKind::VerifierStarted);
        progress.observe_evaluator(
            "nanocodex",
            &EvalEventKind::VerifierCompleted(VerifierResult {
                exit_code: 0,
                rewards: [("task_reward".to_owned(), 1.0)].into(),
            }),
        );
        recorder.finish(progress).await.unwrap();

        let records = fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records[1]["kind"], "verifier.started");
        assert_eq!(records[1]["summary"], "canonical verifier");
        assert_eq!(records[2]["kind"], "verifier.completed");
        assert_eq!(records[2]["summary"], "exit 0 · task_reward=1");
    }

    #[tokio::test]
    async fn progress_log_reports_normalized_request_drift_and_response_match_live() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("progress.jsonl");
        let (progress, recorder) = DiffProgress::start(path.clone(), tokio::time::Instant::now())
            .await
            .unwrap();
        let nanocodex_request = serde_json::json!({
            "direction": "outbound",
            "phase": "warmup",
            "model_call_index": null,
            "event": {
                "type": "response.create",
                "client_metadata": {"session_id": "nano"},
                "prompt_cache_key": "nano-cache",
                "input": [{
                    "type": "additional_tools",
                    "tools": [{"type": "custom", "name": "exec"}]
                }]
            }
        });
        let codex_request = serde_json::json!({
            "direction": "outbound",
            "phase": "warmup",
            "request_index": 3,
            "payload": {
                "encoding": "json",
                "event": {
                    "type": "response.create",
                    "client_metadata": {"session_id": "codex"},
                    "prompt_cache_key": "codex-cache",
                    "input": [{
                        "type": "additional_tools",
                        "tools": [{"type": "custom", "name": "wait"}]
                    }]
                }
            }
        });
        progress.observe_nanocodex_api(&nanocodex_request);
        progress.observe_api_exchange("codex", &codex_request);
        progress.observe_nanocodex_api(&serde_json::json!({
            "direction": "inbound",
            "phase": "warmup",
            "model_call_index": null,
            "event": {
                "type": "response.completed",
                "response": {"id": "nano-response", "status": "completed"}
            }
        }));
        progress.observe_api_exchange(
            "codex",
            &serde_json::json!({
                "direction": "inbound",
                "phase": "warmup",
                "request_index": 3,
                "payload": {
                    "encoding": "json",
                    "event": {
                        "type": "response.completed",
                        "response": {"id": "codex-response", "status": "completed"}
                    }
                }
            }),
        );
        recorder.finish(progress).await.unwrap();

        let records = fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let request_diff = records
            .iter()
            .find(|record| record["kind"] == "api.request.diff")
            .unwrap();
        assert_eq!(request_diff["arm"], "runner");
        assert!(
            request_diff["summary"]
                .as_str()
                .unwrap()
                .contains("tool_configuration")
        );
        assert!(
            records
                .iter()
                .any(|record| record["kind"] == "api.response.match")
        );
    }

    #[test]
    fn api_capture_retains_auxiliary_requests_but_only_aligns_responses_requests() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("api-exchanges.jsonl");
        let records = [
            serde_json::json!({
                "schema_version": 1,
                "sequence": 1,
                "direction": "outbound",
                "transport": "responses_https",
                "request_index": 1,
                "phase": "unknown",
                "kind": "body",
                "method": "GET",
                "path": "/models",
                "payload_bytes": 0,
                "payload": {"encoding": "utf8", "text": ""}
            }),
            serde_json::json!({
                "schema_version": 1,
                "sequence": 2,
                "direction": "inbound",
                "transport": "responses_https",
                "request_index": 1,
                "phase": "unknown",
                "kind": "response_started",
                "status": 200,
                "payload_bytes": 0,
                "payload": {"encoding": "utf8", "text": ""}
            }),
            serde_json::json!({
                "schema_version": 1,
                "sequence": 3,
                "direction": "inbound",
                "transport": "responses_https",
                "request_index": 1,
                "phase": "unknown",
                "kind": "body_chunk",
                "payload_bytes": 13,
                "payload": {"encoding": "json", "event": {"data": []}}
            }),
            serde_json::json!({
                "schema_version": 1,
                "sequence": 4,
                "direction": "inbound",
                "transport": "responses_https",
                "request_index": 1,
                "phase": "unknown",
                "kind": "response_completed",
                "status": 200,
                "payload_bytes": 0,
                "payload": {"encoding": "utf8", "text": ""}
            }),
            serde_json::json!({
                "schema_version": 1,
                "sequence": 5,
                "direction": "outbound",
                "transport": "responses_websocket",
                "request_index": 2,
                "phase": "generation",
                "kind": "message",
                "payload_bytes": 49,
                "payload": {
                    "encoding": "json",
                    "event": {"type": "response.create", "model": "gpt-test"}
                }
            }),
            serde_json::json!({
                "schema_version": 1,
                "sequence": 6,
                "direction": "inbound",
                "transport": "responses_websocket",
                "request_index": 2,
                "phase": "generation",
                "kind": "message",
                "payload_bytes": 58,
                "payload": {
                    "encoding": "json",
                    "event": {
                        "type": "response.completed",
                        "response": {"id": "resp_test"}
                    }
                }
            }),
        ];
        let mut jsonl = records
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        jsonl.push('\n');
        fs::write(&path, jsonl).unwrap();

        let capture =
            inspect_api_exchanges(path.clone(), "all_api_payloads", "exact_wire_payload_bytes")
                .unwrap();
        assert_eq!(capture.summary.requests, 2);
        assert_eq!(capture.summary.response_requests, 1);
        assert_eq!(capture.summary.auxiliary_requests, 1);
        assert_eq!(capture.summary.terminal_events, 1);
        assert_eq!(capture.summary.http_responses_completed, 1);
        assert!(capture.summary.exchange_complete);

        let requests = read_api_request_payloads(&path).unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].request_index, 2);
        assert_eq!(requests[0].payload["type"], "response.create");
    }

    #[test]
    fn api_comparison_counts_only_paired_requests_as_aligned_or_differing() {
        let temporary = tempdir().unwrap();
        let nanocodex_path = temporary.path().join("nanocodex.jsonl");
        let codex_path = temporary.path().join("codex.jsonl");
        let report_path = temporary.path().join("comparison.json");
        let request = |request_index| {
            serde_json::json!({
                "schema_version": 1,
                "sequence": request_index,
                "direction": "outbound",
                "transport": "responses_websocket",
                "request_index": request_index,
                "phase": "generation",
                "kind": "message",
                "payload": {
                    "encoding": "json",
                    "event": {
                        "type": "response.create",
                        "model": "gpt-test",
                        "reasoning": {
                            "effort": "medium",
                            "summary": "auto"
                        },
                        "input": [{
                            "type": "additional_tools",
                            "tools": [
                                {
                                    "type": "custom",
                                    "name": "exec",
                                    "description": "execute code\n\n### `exec_command`\nRun a command.\n\n### `write_stdin`\nWrite input."
                                },
                                {"type": "function", "name": "wait"}
                            ]
                        }]
                    }
                }
            })
        };
        fs::write(&nanocodex_path, format!("{}\n", request(1))).unwrap();
        fs::write(&codex_path, format!("{}\n{}\n", request(1), request(2))).unwrap();

        let summary = compare_api_exchanges(
            &report_path,
            Some(&nanocodex_path),
            Some(&codex_path),
            None,
            None,
        )
        .unwrap();

        assert_eq!(summary.aligned_requests, 1);
        assert_eq!(summary.equal_requests, 1);
        assert_eq!(summary.differing_requests, 0);
        assert_eq!(summary.nanocodex_unpaired_requests, 0);
        assert_eq!(summary.codex_unpaired_requests, 1);
        assert_eq!(summary.event_loop.aligned_turns, 1);
        assert_eq!(summary.event_loop.equal_turns, 1);
        assert_eq!(summary.event_loop.differing_turns, 0);
        assert_eq!(summary.event_loop.nanocodex_unpaired_turns, 0);
        assert_eq!(summary.event_loop.codex_unpaired_turns, 1);
        assert_eq!(
            summary.event_loop.nanocodex_unpaired_tail.as_ref().unwrap(),
            &ApiEventLoopTailSummary::default()
        );
        assert_eq!(
            summary.event_loop.codex_unpaired_tail.as_ref().unwrap(),
            &ApiEventLoopTailSummary {
                turns: 1,
                generation_turns: 1,
                turns_without_usage: 1,
                ..ApiEventLoopTailSummary::default()
            }
        );
        assert_eq!(
            summary.event_loop.initial_code_mode_tool_names_equal,
            Some(true)
        );
        assert_eq!(
            summary.event_loop.initial_code_mode_tool_definitions_equal,
            Some(true)
        );
        assert_eq!(
            summary.event_loop.initial_input_text_sections_equal,
            Some(true)
        );
        assert_eq!(
            summary
                .event_loop
                .initial_generation_input_text_sections_equal,
            Some(true)
        );
        assert_eq!(
            summary
                .event_loop
                .nanocodex
                .as_ref()
                .unwrap()
                .initial_visible_tools,
            ["exec", "wait"]
        );
        assert!(validate_matched_code_mode_only_profile(&summary, "gpt-test", "medium").is_none());
        let mut mismatched_profile = summary.clone();
        mismatched_profile
            .event_loop
            .codex
            .as_mut()
            .unwrap()
            .initial_reasoning_effort = Some("high".to_owned());
        assert!(
            validate_matched_code_mode_only_profile(&mismatched_profile, "gpt-test", "medium")
                .is_some()
        );
        assert_eq!(
            summary
                .event_loop
                .first_divergence
                .as_ref()
                .map(|divergence| divergence.request_index),
            Some(2)
        );
        assert_eq!(
            summary
                .event_loop
                .first_generation_divergence
                .as_ref()
                .map(|divergence| divergence.request_index),
            Some(2)
        );

        let report: serde_json::Value =
            serde_json::from_reader(fs::File::open(report_path).unwrap()).unwrap();
        assert_eq!(report["schema_version"], 11);
        assert_eq!(report["aligned_requests"], 1);
        assert_eq!(report["codex_unpaired_requests"], 1);
        assert_eq!(report["equal_requests"], 1);
        assert_eq!(report["differing_requests"], 0);
        assert_eq!(report["requests"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn event_loop_normalization_ignores_volatile_identity_but_preserves_links() {
        let left = event_loop_fixture("left-session", "left-cache", "left-response");
        let right = event_loop_fixture("right-session", "right-cache", "right-response");

        let left = build_event_loop_trace(&left);
        let right = build_event_loop_trace(&right);

        assert_eq!(left.turns, right.turns);
        assert_eq!(left.summary.previous_response_links, 1);
        assert_eq!(left.summary.broken_previous_response_links, 0);
        assert_eq!(right.summary.previous_response_links, 1);
        assert_eq!(right.summary.broken_previous_response_links, 0);
        assert_eq!(left.summary.prompt_cache_key_stable, Some(true));
        assert_eq!(right.summary.prompt_cache_key_stable, Some(true));
    }

    #[test]
    fn event_loop_summary_compares_model_visible_tool_sequences() {
        let mut left = event_loop_fixture("left-session", "left-cache", "left-response");
        let mut right = event_loop_fixture("right-session", "right-cache", "right-response");
        for requests in [&mut left, &mut right] {
            requests[1].response_events.insert(
                1,
                serde_json::json!({
                    "type": "response.output_item.done",
                    "item": {
                        "type": "custom_tool_call",
                        "name": "exec",
                        "call_id": "call-1",
                        "input": "text(await tools.exec_command({cmd: \"true\"}));"
                    }
                }),
            );
        }

        let left = build_event_loop_trace(&left);
        let right = build_event_loop_trace(&right);

        assert_eq!(left.summary.model_visible_tool_calls, 1);
        assert_eq!(left.summary.model_visible_tool_sequence, ["exec"]);
        assert_eq!(
            left.summary.model_visible_tool_sequence,
            right.summary.model_visible_tool_sequence
        );
    }

    #[test]
    fn event_loop_summary_extracts_nested_code_mode_tool_catalog_in_order() {
        let mut left = event_loop_fixture("left-session", "left-cache", "left-response");
        let mut right = event_loop_fixture("right-session", "right-cache", "right-response");
        left[0].payload["input"][0]["tools"][0]["description"] = serde_json::json!(
            "execute code\n\n### `exec_command`\nRun a command.\n\n### `view_image`\nView an image."
        );
        right[0].payload["input"][0]["tools"][0]["description"] = serde_json::json!(
            "execute code\n\n### `exec_command`\nRun a command.\n\n### `write_stdin`\nWrite input."
        );

        let left = build_event_loop_trace(&left);
        let right = build_event_loop_trace(&right);

        assert_eq!(
            left.summary.initial_code_mode_tools.as_deref(),
            Some(["exec_command".to_owned(), "view_image".to_owned()].as_slice())
        );
        assert_eq!(
            right.summary.initial_code_mode_tools.as_deref(),
            Some(["exec_command".to_owned(), "write_stdin".to_owned()].as_slice())
        );
        assert_ne!(
            left.summary.initial_code_mode_tools,
            right.summary.initial_code_mode_tools
        );
        let left_definitions = left
            .summary
            .initial_code_mode_tool_definitions
            .as_ref()
            .unwrap();
        let right_definitions = right
            .summary
            .initial_code_mode_tool_definitions
            .as_ref()
            .unwrap();
        assert_eq!(left_definitions[0].name, "exec_command");
        assert_eq!(left_definitions[0].ordinal, 0);
        assert_eq!(left_definitions[0], right_definitions[0]);
        assert_ne!(left_definitions[1], right_definitions[1]);
    }

    #[test]
    fn event_loop_summary_detects_changed_nested_tool_definitions_with_equal_names() {
        let mut left = event_loop_fixture("left-session", "left-cache", "left-response");
        let mut right = event_loop_fixture("right-session", "right-cache", "right-response");
        left[0].payload["input"][0]["tools"][0]["description"] =
            serde_json::json!("execute code\n\n### `exec_command`\nRun a command.");
        right[0].payload["input"][0]["tools"][0]["description"] =
            serde_json::json!("execute code\n\n### `exec_command`\nRun a command in a PTY.");

        let left = build_event_loop_trace(&left);
        let right = build_event_loop_trace(&right);

        assert_eq!(
            left.summary.initial_code_mode_tools,
            right.summary.initial_code_mode_tools
        );
        assert_ne!(
            left.summary.initial_code_mode_tool_definitions,
            right.summary.initial_code_mode_tool_definitions
        );
    }

    #[test]
    fn event_loop_summary_fingerprints_initial_model_input_text() {
        let mut left = event_loop_fixture("left-session", "left-cache", "left-response");
        let mut right = event_loop_fixture("right-session", "right-cache", "right-response");
        for requests in [&mut left, &mut right] {
            requests[0].payload["input"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "type": "message",
                    "role": "developer",
                    "content": [{
                        "type": "input_text",
                        "text": "<permissions instructions>\nfull access\n</permissions instructions>"
                    }]
                }));
            requests[1].payload["input"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "type": "message",
                    "role": "user",
                    "content": [{
                        "type": "input_text",
                        "text": "<environment_context>\n  <shell>bash</shell>\n</environment_context>"
                    }]
                }));
        }
        right[1].payload["input"][1]["content"][0]["text"] =
            serde_json::json!("<environment_context>\n  <shell>sh</shell>\n</environment_context>");

        let left = build_event_loop_trace(&left);
        let right = build_event_loop_trace(&right);

        assert_eq!(
            left.summary.initial_input_text_sections,
            right.summary.initial_input_text_sections
        );
        assert_ne!(
            left.summary.initial_generation_input_text_sections,
            right.summary.initial_generation_input_text_sections
        );
        let section = &left.summary.initial_generation_input_text_sections[1];
        assert_eq!(section.role, "user");
        assert_eq!(section.label, "environment_context");
        assert_eq!(section.item_ordinal, 1);
        assert_eq!(section.content_ordinal, 0);
    }

    #[test]
    fn event_loop_normalization_classifies_configuration_and_chain_drift() {
        let left = event_loop_fixture("left-session", "left-cache", "left-response");
        let mut right = event_loop_fixture("right-session", "right-cache", "right-response");
        right[0]
            .payload
            .pointer_mut("/input/0/tools/0/name")
            .unwrap()
            .clone_from(&serde_json::json!("wait"));
        right[0]
            .payload
            .pointer_mut("/reasoning")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .remove("summary");
        right[1].payload["previous_response_id"] = serde_json::json!("not-the-prior-response");

        let left = build_event_loop_trace(&left);
        let right = build_event_loop_trace(&right);
        let mut differences = Vec::new();
        diff_json(
            "",
            left.turns.first(),
            right.turns.first(),
            &mut differences,
        );

        assert_eq!(
            event_loop_difference_categories(&differences),
            vec![
                "reasoning_policy".to_owned(),
                "tool_configuration".to_owned()
            ]
        );
        assert_eq!(right.summary.previous_response_links, 0);
        assert_eq!(right.summary.broken_previous_response_links, 1);
        assert_eq!(
            right.turns[1]["request"]["previous_response_id"],
            "present_unmatched"
        );
    }

    #[test]
    fn event_loop_recognizes_full_history_replay_and_replayed_tool_results() {
        let mut requests = event_loop_fixture("session", "cache", "response");
        requests[0].response_events.insert(
            1,
            serde_json::json!({
                "type": "response.output_item.done",
                "item": {
                    "type": "custom_tool_call",
                    "name": "exec",
                    "call_id": "replayed-call",
                    "input": "text(await tools.exec_command({cmd: \"true\"}));"
                }
            }),
        );
        requests[1].response_events.truncate(1);
        requests.push(ApiRequestPayload {
            request_index: 3,
            phase: Some("generation".to_owned()),
            payload: serde_json::json!({
                "type": "response.create",
                "prompt_cache_key": "cache",
                "input": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "I ran a command."}]
                    },
                    {
                        "type": "reasoning",
                        "encrypted_content": "opaque"
                    },
                    {
                        "type": "custom_tool_call",
                        "name": "exec",
                        "call_id": "replayed-call",
                        "input": "text(await tools.exec_command({cmd: \"true\"}));"
                    },
                    {
                        "type": "custom_tool_call_output",
                        "call_id": "replayed-call",
                        "output": "ok"
                    }
                ]
            }),
            sha256: String::new(),
            response_events: vec![serde_json::json!({
                "type": "response.completed",
                "response": {"id": "response-third", "status": "completed"}
            })],
        });

        let trace = build_event_loop_trace(&requests);

        assert_eq!(trace.summary.previous_response_links, 1);
        assert_eq!(trace.summary.full_history_replays, 1);
        assert_eq!(trace.summary.full_history_replays_after_nonterminal_turn, 1);
        assert_eq!(trace.summary.broken_previous_response_links, 0);
        assert_eq!(trace.summary.tool_result_links, 1);
        assert_eq!(trace.summary.replayed_tool_result_links, 1);
        assert_eq!(trace.summary.broken_tool_result_links, 0);
        assert!(
            serde_json::to_string(&trace.turns[2])
                .unwrap()
                .contains("matches_replayed_output")
        );
        assert!(
            !serde_json::to_string(&trace.turns[2])
                .unwrap()
                .contains("present_unmatched")
        );
    }

    #[test]
    fn trajectory_summary_identifies_only_empty_stdin_poll_roundtrips() {
        let steps: Vec<AtifStep> = serde_json::from_value(serde_json::json!([
            {
                "step_id": 1,
                "source": "agent",
                "model_name": "gpt-test",
                "reasoning_effort": "medium",
                "message": "",
                "tool_calls": [
                    {
                        "tool_call_id": "wrapper",
                        "function_name": "exec",
                        "arguments": {"raw": "await tools.write_stdin(...)"},
                        "extra": {"model_call_index": 1}
                    },
                    {
                        "tool_call_id": "poll",
                        "function_name": "write_stdin",
                        "arguments": {
                            "session_id": 2,
                            "chars": "",
                            "yield_time_ms": 1000
                        },
                        "extra": {"model_call_index": 1}
                    }
                ],
                "observation": {
                    "results": [{
                        "source_call_id": "poll",
                        "content": "still running",
                        "extra": {"status": "completed", "duration_ns": 5000000000_u64}
                    }]
                },
                "metrics": {
                    "prompt_tokens": 100,
                    "completion_tokens": 4,
                    "cached_tokens": 80,
                    "extra": {
                        "model_call_index": 1,
                        "attempt": 1,
                        "connection_generation": 1,
                        "duration_ns": 3000000000_u64,
                        "time_to_first_event_ns": 1,
                        "time_to_first_output_ns": 2,
                        "tool_calls": 1,
                        "cache_write_input_tokens": 0,
                        "reasoning_output_tokens": 0
                    }
                },
                "llm_call_count": 1
            },
            {
                "step_id": 2,
                "source": "agent",
                "message": "",
                "tool_calls": [{
                    "tool_call_id": "input",
                    "function_name": "write_stdin",
                    "arguments": {"session_id": 2, "chars": "q"},
                    "extra": {"model_call_index": 2}
                }],
                "llm_call_count": 1
            },
            {
                "step_id": 3,
                "source": "agent",
                "message": "",
                "tool_calls": [
                    {
                        "tool_call_id": "poll-and-work",
                        "function_name": "write_stdin",
                        "arguments": {"session_id": 2},
                        "extra": {"model_call_index": 3}
                    },
                    {
                        "tool_call_id": "work",
                        "function_name": "apply_patch",
                        "arguments": {"patch": "*** Begin Patch"},
                        "extra": {"model_call_index": 3}
                    }
                ],
                "llm_call_count": 1
            }
        ]))
        .unwrap();

        let summary = ShellPollingSummary::new(&steps);

        assert_eq!(summary.poll_only_steps, 1);
        assert!(summary.model_call_attribution_complete);
        assert_eq!(summary.confirmed_model_calls, Some(1));
        assert_eq!(summary.empty_stdin_tool_calls, 1);
        assert_eq!(summary.sessions, 1);
        assert_eq!(summary.explicit_requested_yield_ms, 1_000);
        assert_eq!(summary.tool_wait_duration_ns, 5_000_000_000);
        assert_eq!(summary.model_duration_ns, 3_000_000_000);
        assert_eq!(summary.prompt_tokens, 100);
        assert_eq!(summary.cached_tokens, 80);
        assert_eq!(summary.completion_tokens, 4);
    }

    #[test]
    fn raw_api_summary_detects_poll_only_model_responses_and_usage() {
        let events = vec![
            serde_json::json!({
                "type": "response.output_item.done",
                "item": {
                    "type": "custom_tool_call",
                    "name": "exec",
                    "call_id": "call-1",
                    "input": "const r = await tools.write_stdin({ session_id: 2, chars: \"\", yield_time_ms: 1000 }); text(r.output);"
                }
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {
                    "status": "completed",
                    "usage": {
                        "input_tokens": 100,
                        "input_tokens_details": {"cached_tokens": 80},
                        "output_tokens": 4
                    }
                }
            }),
        ];

        let polling = detected_polling_turn(&events).unwrap();

        assert_eq!(polling.empty_stdin_calls, 1);
        assert_eq!(polling.input_tokens, 100);
        assert_eq!(polling.cached_tokens, 80);
        assert_eq!(polling.output_tokens, 4);
        let direct = detected_polling_turn(&[serde_json::json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "name": "write_stdin",
                "call_id": "call-2",
                "arguments": "{\"session_id\":2}"
            }
        })])
        .unwrap();
        assert_eq!(direct.empty_stdin_calls, 1);
        assert_eq!(
            detected_code_mode_empty_stdin_calls(
                "await tools.write_stdin({session_id: 2, chars: \"q\"});"
            ),
            None
        );
        assert_eq!(
            detected_code_mode_empty_stdin_calls(
                "await tools.write_stdin({session_id: 2}); await tools.exec_command({cmd: \"pwd\"});"
            ),
            None
        );
    }

    #[test]
    fn event_loop_unpaired_tail_aggregates_cache_and_poll_cost() {
        let completed = |input, cached, output, reasoning, total| {
            serde_json::json!({
                "type": "response.completed",
                "response": {
                    "id": format!("response-{total}"),
                    "status": "completed",
                    "usage": {
                        "input_tokens": input,
                        "input_tokens_details": {"cached_tokens": cached},
                        "output_tokens": output,
                        "output_tokens_details": {"reasoning_tokens": reasoning},
                        "total_tokens": total
                    }
                }
            })
        };
        let request = |request_index, response_events| ApiRequestPayload {
            request_index,
            phase: Some("generation".to_owned()),
            payload: serde_json::json!({
                "type": "response.create",
                "model": "gpt-test",
                "generate": true,
                "input": []
            }),
            sha256: format!("request-{request_index}"),
            response_events,
        };
        let requests = vec![
            request(1, vec![completed(10, 0, 3, 1, 13)]),
            request(
                2,
                vec![
                    serde_json::json!({
                        "type": "response.output_item.done",
                        "item": {
                            "type": "custom_tool_call",
                            "name": "exec",
                            "call_id": "poll",
                            "input": "await tools.write_stdin({session_id: 2});"
                        }
                    }),
                    completed(100, 80, 10, 4, 110),
                ],
            ),
            request(3, vec![completed(120, 100, 5, 2, 125)]),
        ];

        let tail = build_event_loop_trace(&requests).unpaired_tail(1);

        assert_eq!(
            tail,
            ApiEventLoopTailSummary {
                turns: 2,
                generation_turns: 2,
                tool_call_turns: 1,
                detected_poll_only_turns: 1,
                turns_with_usage: 2,
                turns_without_usage: 0,
                usage: ApiTokenUsageSummary {
                    input_tokens: 220,
                    cached_input_tokens: 180,
                    uncached_input_tokens: 40,
                    output_tokens: 15,
                    reasoning_output_tokens: 6,
                    total_tokens: 235,
                },
            }
        );
    }

    #[tokio::test]
    async fn codex_arm_uses_the_native_workspace_verifier_and_event_lifecycle() {
        let temporary = tempdir().unwrap();
        let binary = write_fake_codex(temporary.path());
        let codex = CodexExec::new(&binary, MODEL, "medium")
            .unwrap()
            .api_key("test");
        let task =
            Task::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks/write-greeting"))
                .unwrap();
        let agent = Nanocodex::builder(OpenAi::new("unused").unwrap());
        let configured = codex.clone();
        let report = run_arm(
            task,
            Evaluator::builder(agent)
                .output_directory(temporary.path().join("evaluations"))
                .attempt_agent(move |_attempt, _builder| {
                    Ok::<_, Infallible>(AttemptAgent::codex(configured.clone()))
                }),
            TrajectoryProjection::Codex {
                version: CodexVersion::Fixed("codex-cli-test".to_owned()),
            },
            false,
            DiffProgress::default(),
        )
        .await;

        assert!(report.operational_error.is_none());
        assert!(report.event_error.is_none());
        assert!(report.trajectory_error.is_none());
        assert!(matches!(report.summary.status, ArmStatus::Passed));
        assert_eq!(report.summary.tool_calls, Some(1));
        assert_eq!(
            report
                .summary
                .usage
                .as_ref()
                .map(|usage| usage.total_tokens),
            Some(17)
        );
        assert!(
            report
                .codex_events
                .as_ref()
                .is_some_and(|path| path.is_file())
        );
        assert!(
            report
                .codex_stderr
                .as_ref()
                .is_some_and(|path| path.is_file())
        );
        assert!(
            report
                .codex_summary
                .as_ref()
                .is_some_and(|path| path.is_file())
        );
        assert!(
            report
                .trajectory
                .as_ref()
                .is_some_and(|path| path.is_file())
        );
        let trajectory: AtifTrajectory =
            serde_json::from_slice(&fs::read(report.trajectory.as_ref().unwrap()).unwrap())
                .unwrap();
        assert_eq!(trajectory.agent.name, "codex");
        assert_eq!(trajectory.agent.version, "codex-cli-test");
        assert_eq!(trajectory.tool_call_count(), 1);
        assert_eq!(trajectory.observation_count(), 1);
        assert_eq!(
            report
                .trajectory_summary
                .as_ref()
                .and_then(|summary| summary.model_calls),
            None
        );
        assert!(report.event_log.as_ref().is_some_and(|path| path.is_file()));
        let event_log = fs::read_to_string(report.event_log.as_ref().unwrap()).unwrap();
        assert!(event_log.contains("\"type\":\"attempt_started\""));
        assert!(event_log.contains("\"type\":\"verifier_completed\""));
        assert!(event_log.contains("\"type\":\"completed\""));

        let EvalAttemptOutcome::Scored(outcome) = report.outcome.unwrap() else {
            panic!("fake Codex attempt should be scored");
        };
        assert_eq!(outcome.status, EvalStatus::Passed);
        let agent = outcome.agent.unwrap();
        assert_eq!(agent.metadata.status, AgentStatus::Completed);
        assert_eq!(agent.metadata.transport, "codex_exec_jsonl");
        assert_eq!(agent.metadata.orchestration, "stock_codex_cli");
        assert_eq!(
            fs::read_to_string(outcome.artifacts.workspace.join("greeting.txt")).unwrap(),
            "hello from nanoeval\n"
        );
    }

    fn write_fake_codex(directory: &Path) -> PathBuf {
        let binary = directory.join("codex");
        fs::write(
            &binary,
            r#"#!/bin/sh
set -eu
if [ "${1:-}" = "--version" ]; then
  printf '%s\n' 'codex-cli-test'
  exit 0
fi
printf '%s\n' '{"type":"thread.started","thread_id":"00000000-0000-0000-0000-000000000001"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"id":"item-1","type":"command_execution","command":"printf greeting","aggregated_output":"","exit_code":0,"status":"completed"}}'
printf 'hello from nanoeval\n' > greeting.txt
printf '%s\n' '{"type":"item.completed","item":{"id":"item-2","type":"agent_message","text":"done"}}'
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":4,"output_tokens":7}}'
printf '%s\n' 'fake diagnostic' >&2
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&binary).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&binary, permissions).unwrap();
        binary
    }

    fn event_loop_fixture(
        session_id: &str,
        prompt_cache_key: &str,
        first_response_id: &str,
    ) -> Vec<ApiRequestPayload> {
        let second_response_id = format!("{first_response_id}-second");
        vec![
            ApiRequestPayload {
                request_index: 1,
                phase: Some("warmup".to_owned()),
                payload: serde_json::json!({
                    "type": "response.create",
                    "generate": false,
                    "prompt_cache_key": prompt_cache_key,
                    "client_metadata": {
                        "session_id": session_id,
                        "thread_id": format!("{session_id}-thread"),
                        "ws_request_header_x_openai_internal_codex_responses_lite": "true"
                    },
                    "reasoning": {
                        "context": "all_turns",
                        "effort": "medium",
                        "summary": "auto"
                    },
                    "input": [{
                        "type": "additional_tools",
                        "role": "developer",
                        "tools": [{
                            "type": "custom",
                            "name": "exec",
                            "description": "execute code\n\n### `exec_command`\nRun a command.\n\n### `write_stdin`\nWrite input."
                        }]
                    }]
                }),
                sha256: String::new(),
                response_events: vec![
                    serde_json::json!({
                        "type": "response.created",
                        "response": {"id": first_response_id, "status": "in_progress"}
                    }),
                    serde_json::json!({
                        "type": "response.completed",
                        "response": {"id": first_response_id, "status": "completed"}
                    }),
                ],
            },
            ApiRequestPayload {
                request_index: 2,
                phase: Some("generation".to_owned()),
                payload: serde_json::json!({
                    "type": "response.create",
                    "prompt_cache_key": prompt_cache_key,
                    "previous_response_id": first_response_id,
                    "client_metadata": {
                        "session_id": session_id,
                        "thread_id": format!("{session_id}-thread"),
                        "turn_id": format!("{session_id}-turn"),
                        "ws_request_header_x_openai_internal_codex_responses_lite": "true"
                    },
                    "reasoning": {
                        "context": "all_turns",
                        "effort": "medium",
                        "summary": "auto"
                    },
                    "input": [{
                        "id": format!("{session_id}-message"),
                        "type": "message",
                        "role": "user",
                        "internal_chat_message_metadata_passthrough": {
                            "turn_id": format!("{session_id}-turn")
                        },
                        "content": [{"type": "input_text", "text": "same prompt"}]
                    }]
                }),
                sha256: String::new(),
                response_events: vec![
                    serde_json::json!({
                        "type": "response.created",
                        "response": {"id": second_response_id, "status": "in_progress"}
                    }),
                    serde_json::json!({
                        "type": "response.completed",
                        "response": {"id": second_response_id, "status": "completed"}
                    }),
                ],
            },
        ]
    }
}
