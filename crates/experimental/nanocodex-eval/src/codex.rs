use std::{
    fmt,
    fs::File as SyncFile,
    future::Future,
    io::{self, BufRead, BufReader as SyncBufReader},
    path::{Path, PathBuf},
    pin::Pin,
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(unix)]
use nix::{
    errno::Errno,
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
use tokio::{
    fs::{self, File},
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    task::JoinHandle,
    time::timeout,
};

use crate::{
    AgentMetadata, AgentResult, AgentStatus, AtifAgent, AtifAgentExtra, AtifObservation,
    AtifObservationExtra, AtifObservationResult, AtifSource, AtifStep, AtifToolCall,
    AtifToolCallExtra, AtifTrajectory, BillingCompleteness, CleanupPhase, MeasurementCompleteness,
    UsageTotals, atif::finish_projected_trajectory,
};

const EVENTS_FILE: &str = "agent/codex-events.jsonl";
const STDERR_FILE: &str = "agent/codex-stderr.log";
const SUMMARY_FILE: &str = "agent/codex-summary.json";
const STDERR_TAIL_BYTES: usize = 32 * 1024;
const SUMMARY_ITEM_LIMIT: usize = 10_000;
const SUMMARY_LABEL_BYTES: usize = 4 * 1024;
const PROCESS_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

/// A pinned stock-Codex executable used by the native evaluator.
///
/// This is a concrete evaluation adapter, not an SDK provider abstraction.
/// The executable runs in the evaluator-owned disposable workspace and its
/// complete JSONL/stdout and stderr streams are retained in the attempt.
#[doc(hidden)]
#[derive(Clone)]
pub struct CodexExec {
    binary: PathBuf,
    model: String,
    effort: String,
    web_search: bool,
    code_mode_only: bool,
    api_base_url: Option<String>,
    auth: CodexAuth,
    command_runner: Option<Arc<dyn CodexCommandRunner>>,
}

#[derive(Clone)]
enum CodexAuth {
    Inherit,
    ApiKey(Arc<str>),
    File(PathBuf),
}

impl fmt::Debug for CodexExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexExec")
            .field("binary", &self.binary)
            .field("model", &self.model)
            .field("effort", &self.effort)
            .field("web_search", &self.web_search)
            .field("code_mode_only", &self.code_mode_only)
            .field("api_base_url", &self.api_base_url)
            .field("auth", &"[redacted]")
            .field(
                "command_runner",
                &self.command_runner.as_ref().map(|_| "custom"),
            )
            .finish()
    }
}

impl CodexExec {
    /// Pins one executable and the model policy used for every configured
    /// attempt.
    ///
    /// # Errors
    ///
    /// Returns an error when `binary` is missing or is not a regular file.
    pub fn new(
        binary: impl Into<PathBuf>,
        model: impl Into<String>,
        effort: impl Into<String>,
    ) -> Result<Self, CodexExecError> {
        let requested = binary.into();
        let binary = requested
            .canonicalize()
            .map_err(|source| CodexExecError::Binary {
                path: requested.clone(),
                source,
            })?;
        if !binary.is_file() {
            return Err(CodexExecError::NotAFile(binary));
        }
        Ok(Self {
            binary,
            model: model.into(),
            effort: effort.into(),
            web_search: false,
            code_mode_only: false,
            api_base_url: None,
            auth: CodexAuth::Inherit,
            command_runner: None,
        })
    }

    /// Applies the same explicit web-search policy as the paired Nanocodex
    /// attempt. Search is disabled by default.
    #[must_use]
    pub const fn web_search(mut self, enabled: bool) -> Self {
        self.web_search = enabled;
        self
    }

    /// Restricts stock Codex to the same model-visible Code Mode entrypoints
    /// used by Nanocodex's evaluator profile.
    ///
    /// The evaluator-owned capture proxy must also pin a remote `/models`
    /// selector because Codex intentionally gives that selector precedence
    /// over feature flags.
    #[doc(hidden)]
    #[must_use]
    pub const fn code_mode_only(mut self) -> Self {
        self.code_mode_only = true;
        self
    }

    /// Returns the model whose remote catalog selector must be pinned.
    #[doc(hidden)]
    #[must_use]
    pub fn code_mode_only_model(&self) -> Option<&str> {
        self.code_mode_only.then_some(self.model.as_str())
    }

    /// Routes stock Codex through one evaluator-owned OpenAI-compatible base
    /// URL.
    #[doc(hidden)]
    #[must_use]
    pub fn api_base_url(mut self, url: impl Into<String>) -> Self {
        self.api_base_url = Some(url.into());
        self
    }

    /// Supplies an API key to the child without writing it to retained
    /// artifacts.
    #[must_use]
    pub fn api_key(mut self, api_key: impl Into<Arc<str>>) -> Self {
        self.auth = CodexAuth::ApiKey(api_key.into());
        self
    }

    /// Supplies a Codex-compatible `auth.json` through an attempt-private
    /// temporary `CODEX_HOME`.
    ///
    /// # Errors
    ///
    /// Returns an error when the credential file is missing or not a regular
    /// file.
    pub fn auth_file(mut self, path: impl Into<PathBuf>) -> Result<Self, CodexExecError> {
        let requested = path.into();
        let path = requested
            .canonicalize()
            .map_err(|source| CodexExecError::AuthFile {
                path: requested.clone(),
                source,
            })?;
        if !path.is_file() {
            return Err(CodexExecError::AuthNotAFile(path));
        }
        self.auth = CodexAuth::File(path);
        Ok(self)
    }

    /// Runs the exact Codex argument vector through an evaluator-owned
    /// execution environment.
    #[doc(hidden)]
    #[must_use]
    pub fn command_runner(mut self, runner: Arc<dyn CodexCommandRunner>) -> Self {
        self.command_runner = Some(runner);
        self
    }

    pub(crate) async fn run(
        &self,
        workspace: &Path,
        attempt_directory: &Path,
        prompt: &str,
        attempt_timeout: Duration,
    ) -> CodexExecution {
        if let Some(runner) = &self.command_runner {
            return self
                .run_with_command_runner(
                    runner.as_ref(),
                    attempt_directory,
                    prompt,
                    attempt_timeout,
                )
                .await;
        }
        let started = Instant::now();
        let mut process =
            match CodexProcess::spawn(self, workspace, attempt_directory, prompt).await {
                Ok(process) => process,
                Err(error) => return CodexExecution::setup_failed(error),
            };

        match timeout(attempt_timeout, process.wait_status()).await {
            Ok(waited) => {
                let cleanup_started = chrono::Utc::now();
                let output = match waited {
                    Ok(status) => process.collect(status).await,
                    Err(error) => Err(error),
                };
                match output {
                    Ok(output) => CodexExecution::from_output(
                        self,
                        output,
                        started.elapsed(),
                        CleanupPhase::completed(cleanup_started),
                    ),
                    Err(error) => {
                        let cleanup = match process.finish_cleanup().await {
                            Ok(()) => CleanupPhase::completed(cleanup_started),
                            Err(cleanup_error) => {
                                CleanupPhase::failed(cleanup_started, &cleanup_error)
                            }
                        };
                        CodexExecution {
                            result: None,
                            error: Some(CodexRunError::Execution(error)),
                            cleanup,
                        }
                    }
                }
            }
            Err(_) => {
                let cleanup_started = chrono::Utc::now();
                let recovered = process.terminate().await;
                let cleanup = match &recovered {
                    Ok(_) => CleanupPhase::completed(cleanup_started),
                    Err(error) => CleanupPhase::failed(cleanup_started, error),
                };
                let result = recovered.ok().and_then(|output| {
                    output.transcript.agent_result(
                        self,
                        started.elapsed(),
                        AgentStatus::Cancelled,
                        BillingCompleteness::Unknown,
                    )
                });
                CodexExecution {
                    result,
                    error: Some(CodexRunError::Timeout(attempt_timeout)),
                    cleanup,
                }
            }
        }
    }

    async fn run_with_command_runner(
        &self,
        runner: &dyn CodexCommandRunner,
        attempt_directory: &Path,
        prompt: &str,
        attempt_timeout: Duration,
    ) -> CodexExecution {
        let started = Instant::now();
        let output = match runner
            .run(self.command_arguments(prompt), attempt_timeout)
            .await
        {
            Ok(output) => output,
            Err(error) => {
                return CodexExecution::setup_failed(CodexExecError::CommandRunner(error));
            }
        };
        let cleanup_started = chrono::Utc::now();
        let agent_directory = attempt_directory.join("agent");
        if let Err(error) = fs::create_dir_all(&agent_directory).await {
            return CodexExecution::setup_failed(error.into());
        }
        let events_path = attempt_directory.join(EVENTS_FILE);
        let stderr_path = attempt_directory.join(STDERR_FILE);
        let (transcript, stderr_tail) = tokio::join!(
            capture_stdout(&output.stdout[..], events_path),
            capture_stderr(&output.stderr[..], stderr_path),
        );
        let transcript = match transcript {
            Ok(transcript) => transcript,
            Err(error) => {
                return CodexExecution {
                    result: None,
                    error: Some(CodexRunError::Execution(error)),
                    cleanup: CleanupPhase::completed(cleanup_started),
                };
            }
        };
        let stderr_tail = match stderr_tail {
            Ok(stderr_tail) => stderr_tail,
            Err(error) => {
                return CodexExecution {
                    result: None,
                    error: Some(CodexRunError::Execution(error)),
                    cleanup: CleanupPhase::completed(cleanup_started),
                };
            }
        };
        let duration = started.elapsed();
        let cleanup = CleanupPhase::completed(cleanup_started);
        match output.status {
            CodexCommandStatus::TimedOut => {
                let result = transcript.agent_result(
                    self,
                    duration,
                    AgentStatus::Cancelled,
                    BillingCompleteness::Unknown,
                );
                CodexExecution {
                    result,
                    error: Some(CodexRunError::Timeout(attempt_timeout)),
                    cleanup,
                }
            }
            CodexCommandStatus::Exited(exit_code) => CodexExecution::from_portable_output(
                self,
                exit_code,
                transcript,
                stderr_tail,
                duration,
                cleanup,
            ),
        }
    }

    fn command_arguments(&self, prompt: &str) -> Vec<String> {
        let mut arguments = vec![
            "exec".to_owned(),
            "--json".to_owned(),
            "--ephemeral".to_owned(),
            "--ignore-user-config".to_owned(),
            "--ignore-rules".to_owned(),
            "--dangerously-bypass-approvals-and-sandbox".to_owned(),
            "--skip-git-repo-check".to_owned(),
            "--model".to_owned(),
            self.model.clone(),
            "--config".to_owned(),
            format!("model_reasoning_effort=\"{}\"", self.effort),
            "--config".to_owned(),
            format!(
                "web_search=\"{}\"",
                if self.web_search { "live" } else { "disabled" }
            ),
        ];
        if let Some(api_base_url) = &self.api_base_url {
            arguments.extend([
                "--config".to_owned(),
                format!("openai_base_url={}", toml_string(api_base_url)),
            ]);
        }
        if self.code_mode_only {
            arguments.extend([
                "--config".to_owned(),
                "features.code_mode=true".to_owned(),
                "--config".to_owned(),
                "features.code_mode_only=true".to_owned(),
                "--config".to_owned(),
                "features.multi_agent=false".to_owned(),
                "--config".to_owned(),
                "features.multi_agent_v2=false".to_owned(),
                "--config".to_owned(),
                "agents.enabled=false".to_owned(),
                "--config".to_owned(),
                "suppress_unstable_features_warning=true".to_owned(),
                "--config".to_owned(),
                "tools.experimental_request_user_input.enabled=false".to_owned(),
                "--config".to_owned(),
                "model_reasoning_summary=\"auto\"".to_owned(),
            ]);
        }
        arguments.extend(["--".to_owned(), prompt.to_owned()]);
        arguments
    }
}

fn toml_string(value: &str) -> String {
    toml::Value::String(value.to_owned()).to_string()
}

/// One evaluator-owned way to execute the stock Codex CLI argument vector.
#[doc(hidden)]
pub trait CodexCommandRunner: Send + Sync {
    /// Runs one complete `codex exec --json` process, including timeout
    /// cleanup, and returns its bounded exact output streams.
    fn run<'a>(
        &'a self,
        arguments: Vec<String>,
        timeout: Duration,
    ) -> Pin<
        Box<dyn Future<Output = Result<CodexCommandOutput, CodexCommandRunnerError>> + Send + 'a>,
    >;
}

/// Complete output from an evaluator-owned stock Codex process.
#[doc(hidden)]
pub struct CodexCommandOutput {
    /// Terminal process status.
    pub status: CodexCommandStatus,
    /// Complete bounded standard output.
    pub stdout: Vec<u8>,
    /// Complete bounded standard error.
    pub stderr: Vec<u8>,
}

/// Portable terminal status for an evaluator-owned stock Codex process.
#[doc(hidden)]
pub enum CodexCommandStatus {
    /// The process exited with this numeric code.
    Exited(i32),
    /// The runner terminated the process after its deadline.
    TimedOut,
}

/// Failure in the evaluator-owned command transport.
#[doc(hidden)]
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct CodexCommandRunnerError {
    message: String,
}

impl CodexCommandRunnerError {
    /// Wraps a runner-specific diagnostic without exposing its concrete
    /// transport type through the evaluator crate.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Failure while validating or executing the pinned Codex CLI.
#[doc(hidden)]
#[derive(Debug, thiserror::Error)]
pub enum CodexExecError {
    /// The configured executable could not be resolved.
    #[error("failed to resolve Codex executable {path}: {source}")]
    Binary {
        /// Requested executable path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: io::Error,
    },

    /// The configured executable path was not a regular file.
    #[error("Codex executable is not a regular file: {0}")]
    NotAFile(PathBuf),

    /// The configured credential file could not be resolved.
    #[error("failed to resolve Codex auth file {path}: {source}")]
    AuthFile {
        /// Requested credential path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: io::Error,
    },

    /// The configured credential path was not a regular file.
    #[error("Codex auth path is not a regular file: {0}")]
    AuthNotAFile(PathBuf),

    /// A process or artifact I/O operation failed.
    #[error("Codex process I/O failed: {0}")]
    Io(#[from] io::Error),

    /// A JSONL event was malformed.
    #[error("invalid Codex JSONL event on line {line}: {source}")]
    EventJson {
        /// One-based stdout line number.
        line: u64,
        /// JSON decoding error.
        #[source]
        source: serde_json::Error,
    },

    /// A spawned output task failed.
    #[error("Codex output capture stopped: {0}")]
    Capture(String),

    /// Codex reported a failed turn.
    #[error("Codex turn failed: {0}")]
    TurnFailed(String),

    /// Codex exited without a terminal turn event.
    #[error("Codex exited without a turn.completed event")]
    MissingTerminal,

    /// Codex returned a non-zero process status.
    #[error("Codex exited with {status}: {stderr}")]
    Exit {
        /// Process exit status.
        status: ExitStatus,
        /// Bounded stderr tail. Complete stderr is retained on disk.
        stderr: String,
    },

    /// An evaluator-owned command transport failed.
    #[error("Codex command runner failed: {0}")]
    CommandRunner(#[source] CodexCommandRunnerError),

    /// A portable evaluator-owned Codex process returned a non-zero exit code.
    #[error("Codex exited with code {code}: {stderr}")]
    ExitCode {
        /// Numeric exit code returned by the guest process.
        code: i32,
        /// Bounded stderr tail. Complete stderr is retained on disk.
        stderr: String,
    },
}

pub(crate) struct CodexExecution {
    pub(crate) result: Option<AgentResult>,
    pub(crate) error: Option<CodexRunError>,
    pub(crate) cleanup: CleanupPhase,
}

impl CodexExecution {
    const fn setup_failed(error: CodexExecError) -> Self {
        Self {
            result: None,
            error: Some(CodexRunError::Execution(error)),
            cleanup: CleanupPhase::not_required(),
        }
    }

    fn from_output(
        config: &CodexExec,
        output: CodexProcessOutput,
        duration: Duration,
        cleanup: CleanupPhase,
    ) -> Self {
        let error = if !output.status.success() {
            Some(CodexRunError::Execution(CodexExecError::Exit {
                status: output.status,
                stderr: output.stderr_tail,
            }))
        } else if let Some(error) = output.transcript.terminal_error.clone() {
            Some(CodexRunError::Execution(CodexExecError::TurnFailed(error)))
        } else if !output.transcript.completed {
            Some(CodexRunError::Execution(CodexExecError::MissingTerminal))
        } else {
            None
        };
        let status = if error.is_none() {
            AgentStatus::Completed
        } else {
            AgentStatus::Failed
        };
        let billing = if output.transcript.usage.is_some() {
            BillingCompleteness::Complete
        } else {
            BillingCompleteness::Unknown
        };
        let result = output
            .transcript
            .agent_result(config, duration, status, billing);
        Self {
            result,
            error,
            cleanup,
        }
    }

    fn from_portable_output(
        config: &CodexExec,
        exit_code: i32,
        transcript: CodexTranscript,
        stderr_tail: String,
        duration: Duration,
        cleanup: CleanupPhase,
    ) -> Self {
        let error = if exit_code != 0 {
            Some(CodexRunError::Execution(CodexExecError::ExitCode {
                code: exit_code,
                stderr: stderr_tail,
            }))
        } else if let Some(error) = transcript.terminal_error.clone() {
            Some(CodexRunError::Execution(CodexExecError::TurnFailed(error)))
        } else if !transcript.completed {
            Some(CodexRunError::Execution(CodexExecError::MissingTerminal))
        } else {
            None
        };
        let status = if error.is_none() {
            AgentStatus::Completed
        } else {
            AgentStatus::Failed
        };
        let billing = if transcript.usage.is_some() {
            BillingCompleteness::Complete
        } else {
            BillingCompleteness::Unknown
        };
        let result = transcript.agent_result(config, duration, status, billing);
        Self {
            result,
            error,
            cleanup,
        }
    }
}

pub(crate) enum CodexRunError {
    Timeout(Duration),
    Execution(CodexExecError),
}

struct CodexProcess {
    child: Child,
    stdout: Option<JoinHandle<Result<CodexTranscript, CodexExecError>>>,
    stderr: Option<JoinHandle<Result<String, CodexExecError>>>,
    #[cfg(unix)]
    process_group: Pid,
    #[cfg(unix)]
    process_group_killed: bool,
    _auth_home: Option<tempfile::TempDir>,
}

impl CodexProcess {
    async fn spawn(
        config: &CodexExec,
        workspace: &Path,
        attempt_directory: &Path,
        prompt: &str,
    ) -> Result<Self, CodexExecError> {
        let agent_directory = attempt_directory.join("agent");
        fs::create_dir_all(&agent_directory).await?;
        let auth_home = prepare_auth_home(&config.auth)?;
        let mut command = Command::new(&config.binary);
        command
            .args(config.command_arguments(prompt))
            .current_dir(workspace)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(home) = &auth_home {
            command.env("CODEX_HOME", home.path());
        }
        match &config.auth {
            CodexAuth::ApiKey(api_key) => {
                command.env("OPENAI_API_KEY", api_key.as_ref());
            }
            CodexAuth::File(_) => {
                command.env_remove("OPENAI_API_KEY");
            }
            CodexAuth::Inherit => {}
        }
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn()?;
        #[cfg(unix)]
        let process_group = child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .map(Pid::from_raw)
            .ok_or_else(|| io::Error::other("spawned Codex process has no process group"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("spawned Codex process has no stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("spawned Codex process has no stderr"))?;
        let events_path = attempt_directory.join(EVENTS_FILE);
        let stderr_path = attempt_directory.join(STDERR_FILE);
        Ok(Self {
            child,
            stdout: Some(tokio::spawn(capture_stdout(stdout, events_path))),
            stderr: Some(tokio::spawn(capture_stderr(stderr, stderr_path))),
            #[cfg(unix)]
            process_group,
            #[cfg(unix)]
            process_group_killed: false,
            _auth_home: auth_home,
        })
    }

    async fn wait_status(&mut self) -> Result<ExitStatus, CodexExecError> {
        Ok(self.child.wait().await?)
    }

    async fn terminate(&mut self) -> Result<CodexProcessOutput, CodexExecError> {
        #[cfg(unix)]
        self.signal_process_group(Signal::SIGTERM)?;
        #[cfg(not(unix))]
        self.child.start_kill()?;

        match timeout(PROCESS_SHUTDOWN_GRACE, self.child.wait()).await {
            Ok(status) => self.collect(status?).await,
            Err(_) => {
                #[cfg(unix)]
                self.signal_process_group(Signal::SIGKILL)?;
                #[cfg(not(unix))]
                self.child.start_kill()?;
                let status = self.child.wait().await?;
                self.collect(status).await
            }
        }
    }

    async fn collect(&mut self, status: ExitStatus) -> Result<CodexProcessOutput, CodexExecError> {
        #[cfg(unix)]
        self.signal_process_group(Signal::SIGKILL)?;
        let stdout = self
            .stdout
            .take()
            .ok_or_else(|| CodexExecError::Capture("stdout was already collected".to_owned()))?;
        let stderr = self
            .stderr
            .take()
            .ok_or_else(|| CodexExecError::Capture("stderr was already collected".to_owned()))?;
        let transcript = stdout
            .await
            .map_err(|error| CodexExecError::Capture(error.to_string()))??;
        let stderr_tail = stderr
            .await
            .map_err(|error| CodexExecError::Capture(error.to_string()))??;
        Ok(CodexProcessOutput {
            status,
            transcript,
            stderr_tail,
        })
    }

    async fn finish_cleanup(&mut self) -> Result<(), CodexExecError> {
        if !self.child.try_wait()?.is_some() {
            let _ = self.terminate().await?;
        }
        #[cfg(unix)]
        self.signal_process_group(Signal::SIGKILL)?;
        Ok(())
    }

    #[cfg(unix)]
    fn signal_process_group(&mut self, signal: Signal) -> Result<(), CodexExecError> {
        match killpg(self.process_group, signal) {
            Ok(()) | Err(Errno::ESRCH) => {
                if signal == Signal::SIGKILL {
                    self.process_group_killed = true;
                }
                Ok(())
            }
            Err(error) => Err(io::Error::other(format!(
                "failed to signal Codex process group with {signal:?}: {error}"
            ))
            .into()),
        }
    }
}

impl Drop for CodexProcess {
    fn drop(&mut self) {
        #[cfg(unix)]
        if !self.process_group_killed {
            let _ = killpg(self.process_group, Signal::SIGKILL);
        }
        let _ = self.child.start_kill();
        if let Some(stdout) = self.stdout.take() {
            stdout.abort();
        }
        if let Some(stderr) = self.stderr.take() {
            stderr.abort();
        }
    }
}

struct CodexProcessOutput {
    status: ExitStatus,
    transcript: CodexTranscript,
    stderr_tail: String,
}

#[derive(Debug, Default, Serialize)]
struct CodexTranscript {
    schema_version: u32,
    thread_id: Option<String>,
    completed: bool,
    terminal_error: Option<String>,
    usage: Option<CodexUsage>,
    final_message: String,
    items: Vec<CodexItemSummary>,
    omitted_items: usize,
    tool_calls: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CodexUsage {
    input_tokens: i64,
    cached_input_tokens: i64,
    #[serde(default)]
    cache_write_input_tokens: i64,
    output_tokens: i64,
    #[serde(default)]
    reasoning_output_tokens: i64,
}

#[derive(Debug, Serialize)]
struct CodexItemSummary {
    line: u64,
    kind: String,
    label: String,
    label_truncated: bool,
    status: Option<String>,
}

impl CodexTranscript {
    fn new() -> Self {
        Self {
            schema_version: 1,
            ..Self::default()
        }
    }

    fn observe(&mut self, line: u64, event: &Value) -> Result<(), CodexExecError> {
        let Some(kind) = event.get("type").and_then(Value::as_str) else {
            return Ok(());
        };
        match kind {
            "thread.started" => {
                self.thread_id = event
                    .get("thread_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
            "turn.completed" => {
                self.usage = event
                    .get("usage")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|source| CodexExecError::EventJson { line, source })?;
                self.completed = true;
            }
            "turn.failed" => {
                self.terminal_error = event
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .map_or_else(
                        || Some("Codex reported a failed turn".to_owned()),
                        |message| Some(message.to_owned()),
                    );
            }
            "error" => {
                self.terminal_error = event.get("message").and_then(Value::as_str).map_or_else(
                    || Some("Codex reported an unrecoverable stream error".to_owned()),
                    |message| Some(message.to_owned()),
                );
            }
            "item.completed" => {
                if let Some(item) = event.get("item") {
                    self.observe_item(line, item);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn observe_item(&mut self, line: u64, item: &Value) {
        let kind = item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if kind == "agent_message"
            && let Some(text) = item.get("text").and_then(Value::as_str)
        {
            self.final_message = text.to_owned();
        }
        let (label, status, tool_call) = match kind {
            "command_execution" => (
                item.get("command")
                    .and_then(Value::as_str)
                    .unwrap_or("command")
                    .to_owned(),
                item.get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                true,
            ),
            "file_change" => (
                item.get("changes")
                    .and_then(Value::as_array)
                    .map(|changes| {
                        changes
                            .iter()
                            .filter_map(|change| change.get("path").and_then(Value::as_str))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_else(|| "file change".to_owned()),
                item.get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                true,
            ),
            "mcp_tool_call" => (
                format!(
                    "{}/{}",
                    item.get("server").and_then(Value::as_str).unwrap_or("mcp"),
                    item.get("tool")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                ),
                item.get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                true,
            ),
            "web_search" => ("web search".to_owned(), None, true),
            "collab_tool_call" => (
                item.get("tool")
                    .and_then(Value::as_str)
                    .unwrap_or("collab")
                    .to_owned(),
                item.get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                true,
            ),
            "todo_list" => ("plan update".to_owned(), None, true),
            "reasoning" => (
                item.get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("reasoning")
                    .to_owned(),
                None,
                false,
            ),
            "agent_message" => ("assistant message".to_owned(), None, false),
            other => (other.to_owned(), None, false),
        };
        if tool_call {
            self.tool_calls = self.tool_calls.saturating_add(1);
        }
        if self.items.len() >= SUMMARY_ITEM_LIMIT {
            self.omitted_items = self.omitted_items.saturating_add(1);
            return;
        }
        let (label, label_truncated) = bounded_label(label);
        self.items.push(CodexItemSummary {
            line,
            kind: kind.to_owned(),
            label,
            label_truncated,
            status,
        });
    }

    fn agent_result(
        &self,
        config: &CodexExec,
        duration: Duration,
        status: AgentStatus,
        billing_completeness: BillingCompleteness,
    ) -> Option<AgentResult> {
        let usage = self.usage.as_ref().and_then(CodexUsage::totals);
        if self.final_message.is_empty() && usage.is_none() && self.items.is_empty() {
            return None;
        }
        let usage = usage.unwrap_or_default();
        let duration_ns = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let metadata = AgentMetadata {
            status,
            model: config.model.clone(),
            effort: config.effort.clone(),
            reasoning_mode: None,
            transport: "codex_exec_jsonl".to_owned(),
            orchestration: "stock_codex_cli".to_owned(),
            runtime_completeness: MeasurementCompleteness::ObservedLowerBound,
            duration_ms: u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            duration_ns,
            model_calls: 0,
            steers: 0,
            compactions: 0,
            tool_calls: self.tool_calls,
            connection_attempts: 0,
            websocket_reconnects: 0,
            response_attempts: 0,
            response_retries: 0,
            billing_uncertain_response_attempts: u32::from(!self.completed),
            connection_duration_ns: 0,
            retry_backoff_duration_ns: 0,
            model_duration_ns: 0,
            warmup_duration_ns: 0,
            tool_work_duration_ns: 0,
            tool_wall_duration_ns: 0,
            usage: usage.clone(),
            warmup_usage: UsageTotals::default(),
            _last_response_id: None,
            cost_usd: None,
            cost_status: if self.usage.is_some() {
                "usage_reported_unpriced"
            } else {
                "usage_not_reported"
            }
            .to_owned(),
            pricing_revision: None,
            estimated_cost: None,
        };
        Some(AgentResult {
            final_message: self.final_message.clone(),
            model: config.model.clone(),
            effort: config.effort.clone(),
            model_calls: 0,
            tool_calls: self.tool_calls,
            usage,
            cost_usd: None,
            billing_completeness,
            metadata,
        })
    }
}

impl CodexUsage {
    fn totals(&self) -> Option<UsageTotals> {
        let input_tokens = u64::try_from(self.input_tokens).ok()?;
        let cached_input_tokens = u64::try_from(self.cached_input_tokens).ok()?;
        let cache_write_input_tokens = u64::try_from(self.cache_write_input_tokens).ok()?;
        let output_tokens = u64::try_from(self.output_tokens).ok()?;
        let reasoning_output_tokens = u64::try_from(self.reasoning_output_tokens).ok()?;
        Some(UsageTotals {
            input_tokens,
            cached_input_tokens,
            cache_write_input_tokens,
            output_tokens,
            reasoning_output_tokens,
            total_tokens: input_tokens.saturating_add(output_tokens),
        })
    }
}

/// Projects one retained stock-Codex `exec --json` stream into ATIF v1.7.
///
/// The raw JSONL remains authoritative. Codex's exec stream does not expose
/// logical model-call boundaries or per-call latency, so the resulting
/// trajectory preserves completed items in stream order and retains the
/// attempt's observed-lower-bound runtime completeness.
///
/// # Errors
///
/// Returns an error when the stream cannot be read, contains malformed JSON,
/// or a completed item cannot be represented as an ATIF step.
#[doc(hidden)]
pub fn project_codex_atif(
    events_path: &Path,
    prompt: &str,
    result: &AgentResult,
    codex_version: &str,
) -> Result<AtifTrajectory, CodexExecError> {
    let input = SyncFile::open(events_path)?;
    let mut input = SyncBufReader::new(input);
    let mut projection = CodexAtifProjection::new(&result.model, &result.effort);
    let mut line_number = 0_u64;
    let mut line = Vec::new();
    loop {
        line.clear();
        if input.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        line_number = line_number.saturating_add(1);
        let event =
            serde_json::from_slice::<Value>(&line).map_err(|source| CodexExecError::EventJson {
                line: line_number,
                source,
            })?;
        projection.observe(line_number, &event)?;
    }
    Ok(projection.finish(prompt, result, codex_version))
}

struct CodexAtifProjection {
    session_id: String,
    model: String,
    effort: String,
    steps: Vec<AtifStep>,
}

impl CodexAtifProjection {
    fn new(model: &str, effort: &str) -> Self {
        Self {
            session_id: String::new(),
            model: model.to_owned(),
            effort: effort.to_owned(),
            steps: Vec::new(),
        }
    }

    fn observe(&mut self, line: u64, event: &Value) -> Result<(), CodexExecError> {
        match event.get("type").and_then(Value::as_str) {
            Some("thread.started") => {
                if let Some(thread_id) = event.get("thread_id").and_then(Value::as_str) {
                    self.session_id = thread_id.to_owned();
                }
            }
            Some("item.completed") => {
                if let Some(item) = event.get("item") {
                    self.observe_item(line, item)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn observe_item(&mut self, line: u64, item: &Value) -> Result<(), CodexExecError> {
        let kind = item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let item_id = item
            .get("id")
            .and_then(Value::as_str)
            .map_or_else(|| format!("codex-item-{line}"), str::to_owned);
        let step = match kind {
            "agent_message" => self.agent_step(
                item.get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                None,
            ),
            "reasoning" => self.agent_step(
                String::new(),
                item.get("text").and_then(Value::as_str).map(str::to_owned),
            ),
            "command_execution" => self.tool_step(
                line,
                item_id,
                "command_execution".to_owned(),
                json!({
                    "command": item.get("command").cloned().unwrap_or(Value::Null),
                }),
                json!({
                    "aggregated_output": item
                        .get("aggregated_output")
                        .cloned()
                        .unwrap_or(Value::Null),
                    "exit_code": item.get("exit_code").cloned().unwrap_or(Value::Null),
                    "status": item.get("status").cloned().unwrap_or(Value::Null),
                }),
                item_status(item),
            )?,
            "file_change" => self.tool_step(
                line,
                item_id,
                "file_change".to_owned(),
                json!({
                    "changes": item.get("changes").cloned().unwrap_or(Value::Null),
                }),
                json!({
                    "status": item.get("status").cloned().unwrap_or(Value::Null),
                }),
                item_status(item),
            )?,
            "mcp_tool_call" => {
                let server = item.get("server").and_then(Value::as_str).unwrap_or("mcp");
                let tool = item
                    .get("tool")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                self.tool_step(
                    line,
                    item_id,
                    format!("mcp/{server}/{tool}"),
                    item.get("arguments").cloned().unwrap_or(Value::Null),
                    json!({
                        "result": item.get("result").cloned().unwrap_or(Value::Null),
                        "error": item.get("error").cloned().unwrap_or(Value::Null),
                        "status": item.get("status").cloned().unwrap_or(Value::Null),
                    }),
                    item_status(item),
                )?
            }
            "web_search" => self.tool_step(
                line,
                item_id,
                "web_search".to_owned(),
                json!({
                    "query": item.get("query").cloned().unwrap_or(Value::Null),
                    "action": item.get("action").cloned().unwrap_or(Value::Null),
                }),
                json!({"status": "completed"}),
                "completed".to_owned(),
            )?,
            "collab_tool_call" => {
                let tool = item
                    .get("tool")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                self.tool_step(
                    line,
                    item_id,
                    format!("collab/{tool}"),
                    json!({
                        "sender_thread_id": item
                            .get("sender_thread_id")
                            .cloned()
                            .unwrap_or(Value::Null),
                        "receiver_thread_ids": item
                            .get("receiver_thread_ids")
                            .cloned()
                            .unwrap_or(Value::Null),
                        "prompt": item.get("prompt").cloned().unwrap_or(Value::Null),
                    }),
                    json!({
                        "agents_states": item
                            .get("agents_states")
                            .cloned()
                            .unwrap_or(Value::Null),
                        "status": item.get("status").cloned().unwrap_or(Value::Null),
                    }),
                    item_status(item),
                )?
            }
            "todo_list" => self.agent_step(
                String::new(),
                Some(
                    serde_json::to_string(item.get("items").unwrap_or(&Value::Null))
                        .map_err(|source| CodexExecError::EventJson { line, source })?,
                ),
            ),
            "error" => self.agent_step(
                item.get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("Codex item error")
                    .to_owned(),
                None,
            ),
            _ => self.agent_step(
                serde_json::to_string(item)
                    .map_err(|source| CodexExecError::EventJson { line, source })?,
                None,
            ),
        };
        self.steps.push(step);
        Ok(())
    }

    fn agent_step(&self, message: String, reasoning_content: Option<String>) -> AtifStep {
        AtifStep {
            step_id: 0,
            source: AtifSource::Agent,
            model_name: Some(self.model.clone()),
            reasoning_effort: Some(self.effort.clone()),
            message,
            reasoning_content,
            tool_calls: None,
            observation: None,
            metrics: None,
            llm_call_count: None,
            extra: None,
        }
    }

    fn tool_step(
        &self,
        line: u64,
        item_id: String,
        function_name: String,
        arguments: Value,
        observation: Value,
        status: String,
    ) -> Result<AtifStep, CodexExecError> {
        let arguments = object_raw_value(arguments, line)?;
        let content = serde_json::to_string(&observation)
            .map_err(|source| CodexExecError::EventJson { line, source })?;
        Ok(AtifStep {
            step_id: 0,
            source: AtifSource::Agent,
            model_name: Some(self.model.clone()),
            reasoning_effort: Some(self.effort.clone()),
            message: String::new(),
            reasoning_content: None,
            tool_calls: Some(vec![AtifToolCall {
                tool_call_id: item_id.clone(),
                function_name,
                arguments,
                extra: AtifToolCallExtra {
                    model_call_index: 0,
                },
            }]),
            observation: Some(AtifObservation {
                results: vec![AtifObservationResult {
                    source_call_id: item_id,
                    content,
                    extra: AtifObservationExtra {
                        status,
                        duration_ns: 0,
                    },
                }],
            }),
            metrics: None,
            llm_call_count: None,
            extra: None,
        })
    }

    fn finish(self, prompt: &str, result: &AgentResult, codex_version: &str) -> AtifTrajectory {
        finish_projected_trajectory(
            prompt,
            self.session_id,
            AtifAgent {
                name: "codex".to_owned(),
                version: codex_version.to_owned(),
                model_name: result.model.clone(),
                extra: AtifAgentExtra {
                    transport: result.metadata.transport.clone(),
                    orchestration: result.metadata.orchestration.clone(),
                },
            },
            self.steps,
            result,
        )
    }
}

fn object_raw_value(value: Value, line: u64) -> Result<Box<RawValue>, CodexExecError> {
    let value = if value.is_object() {
        value
    } else {
        json!({ "raw": value })
    };
    RawValue::from_string(
        serde_json::to_string(&value)
            .map_err(|source| CodexExecError::EventJson { line, source })?,
    )
    .map_err(|source| CodexExecError::EventJson { line, source })
}

fn item_status(item: &Value) -> String {
    item.get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed")
        .to_owned()
}

async fn capture_stdout(
    stdout: impl AsyncRead + Unpin,
    path: PathBuf,
) -> Result<CodexTranscript, CodexExecError> {
    let mut output = File::create(&path).await?;
    let mut stdout = BufReader::new(stdout);
    let mut transcript = CodexTranscript::new();
    let mut line_number = 0_u64;
    let mut first_error = None;
    let mut line = Vec::new();
    loop {
        line.clear();
        if stdout.read_until(b'\n', &mut line).await? == 0 {
            break;
        }
        line_number = line_number.saturating_add(1);
        output.write_all(&line).await?;
        tracing::info!(
            target: "nanocodex_eval",
            content_kind = "codex.exec.event",
            content = String::from_utf8_lossy(&line).as_ref(),
            "trace content"
        );
        match serde_json::from_slice(&line) {
            Ok(event) => {
                if let Err(error) = transcript.observe(line_number, &event)
                    && first_error.is_none()
                {
                    first_error = Some(error);
                }
            }
            Err(source) if first_error.is_none() => {
                first_error = Some(CodexExecError::EventJson {
                    line: line_number,
                    source,
                });
            }
            Err(_) => {}
        }
    }
    output.flush().await?;
    output.sync_all().await?;
    write_summary(&path, &transcript).await?;
    first_error.map_or(Ok(transcript), Err)
}

async fn capture_stderr(
    stderr: impl AsyncRead + Unpin,
    path: PathBuf,
) -> Result<String, CodexExecError> {
    let mut stderr = BufReader::new(stderr);
    let mut output = File::create(path).await?;
    let mut tail = Vec::with_capacity(STDERR_TAIL_BYTES);
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        let read = stderr.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let chunk = &buffer[..read];
        output.write_all(chunk).await?;
        tracing::info!(
            target: "nanocodex_eval",
            content_kind = "codex.exec.stderr",
            content = String::from_utf8_lossy(chunk).as_ref(),
            "trace content"
        );
        tail.extend_from_slice(chunk);
        if tail.len() > STDERR_TAIL_BYTES {
            let excess = tail.len() - STDERR_TAIL_BYTES;
            tail.drain(..excess);
        }
    }
    output.flush().await?;
    output.sync_all().await?;
    Ok(String::from_utf8_lossy(&tail).into_owned())
}

async fn write_summary(events_path: &Path, transcript: &CodexTranscript) -> io::Result<()> {
    let summary = events_path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| io::Error::other("Codex events path has no attempt root"))?
        .join(SUMMARY_FILE);
    let mut encoded = serde_json::to_vec_pretty(transcript).map_err(io::Error::other)?;
    encoded.push(b'\n');
    fs::write(summary, encoded).await
}

fn prepare_auth_home(auth: &CodexAuth) -> Result<Option<tempfile::TempDir>, CodexExecError> {
    match auth {
        CodexAuth::Inherit => Ok(None),
        CodexAuth::ApiKey(_) => {
            let home = tempfile::Builder::new()
                .prefix("nanocodex-eval-codex-home-")
                .tempdir()?;
            Ok(Some(home))
        }
        CodexAuth::File(auth_file) => {
            let home = tempfile::Builder::new()
                .prefix("nanocodex-eval-codex-home-")
                .tempdir()?;
            let destination = home.path().join("auth.json");
            std::fs::copy(auth_file, &destination)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o600))?;
            }
            Ok(Some(home))
        }
    }
}

fn bounded_label(mut label: String) -> (String, bool) {
    if label.len() <= SUMMARY_LABEL_BYTES {
        return (label, false);
    }
    let mut boundary = SUMMARY_LABEL_BYTES;
    while !label.is_char_boundary(boundary) {
        boundary = boundary.saturating_sub(1);
    }
    label.truncate(boundary);
    (label, true)
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        fs,
        future::Future,
        os::unix::fs::PermissionsExt as _,
        path::{Path, PathBuf},
        pin::Pin,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use serde_json::json;
    use tempfile::tempdir;
    use tokio::time::sleep;

    use crate::{AgentStatus, AtifSource, BillingCompleteness, MeasurementCompleteness};

    use super::{
        CodexCommandOutput, CodexCommandRunner, CodexCommandRunnerError, CodexCommandStatus,
        CodexExec, CodexExecError, CodexRunError, CodexTranscript, EVENTS_FILE, STDERR_FILE,
        SUMMARY_FILE, capture_stdout, project_codex_atif,
    };

    #[derive(Default)]
    struct StaticCommandRunner {
        arguments: Mutex<Vec<String>>,
    }

    impl CodexCommandRunner for StaticCommandRunner {
        fn run<'a>(
            &'a self,
            arguments: Vec<String>,
            _timeout: Duration,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<CodexCommandOutput, CodexCommandRunnerError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                *self.arguments.lock().unwrap() = arguments;
                Ok(CodexCommandOutput {
                    status: CodexCommandStatus::Exited(0),
                    stdout: concat!(
                        "{\"type\":\"thread.started\",\"thread_id\":\"thread-runner\"}\n",
                        "{\"type\":\"item.completed\",\"item\":{\"id\":\"message-1\",\"type\":\"agent_message\",\"text\":\"done in guest\"}}\n",
                        "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":2,\"cached_input_tokens\":1,\"output_tokens\":3}}\n",
                    )
                    .as_bytes()
                    .to_vec(),
                    stderr: b"guest diagnostic\n".to_vec(),
                })
            })
        }
    }

    #[test]
    fn transcript_extracts_terminal_usage_message_and_tool_shape() {
        let mut transcript = CodexTranscript::new();
        transcript
            .observe(
                1,
                &json!({
                    "type": "thread.started",
                    "thread_id": "00000000-0000-0000-0000-000000000001"
                }),
            )
            .unwrap();
        transcript
            .observe(
                2,
                &json!({
                    "type": "item.completed",
                    "item": {
                        "id": "one",
                        "type": "command_execution",
                        "command": "cargo test",
                        "status": "completed"
                    }
                }),
            )
            .unwrap();
        transcript
            .observe(
                3,
                &json!({
                    "type": "item.completed",
                    "item": {
                        "id": "two",
                        "type": "agent_message",
                        "text": "finished"
                    }
                }),
            )
            .unwrap();
        transcript
            .observe(
                4,
                &json!({
                    "type": "turn.completed",
                    "usage": {
                        "input_tokens": 12,
                        "cached_input_tokens": 3,
                        "output_tokens": 8
                    }
                }),
            )
            .unwrap();

        assert!(transcript.completed);
        assert_eq!(
            transcript.thread_id.as_deref(),
            Some("00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(transcript.final_message, "finished");
        assert_eq!(transcript.tool_calls, 1);
        assert_eq!(transcript.items.len(), 2);
        let usage = transcript.usage.unwrap().totals().unwrap();
        assert_eq!(usage.input_tokens, 12);
        assert_eq!(usage.cached_input_tokens, 3);
        assert_eq!(usage.output_tokens, 8);
        assert_eq!(usage.total_tokens, 20);
    }

    #[test]
    fn codex_jsonl_projects_complete_ordered_items_into_atif() {
        let temporary = tempdir().unwrap();
        let events = temporary.path().join("codex-events.jsonl");
        let input = concat!(
            "{\"type\":\"thread.started\",\"thread_id\":\"thread-1\"}\n",
            "{\"type\":\"turn.started\"}\n",
            "{\"type\":\"item.completed\",\"item\":{\"id\":\"reason-1\",\"type\":\"reasoning\",\"text\":\"inspect first\"}}\n",
            "{\"type\":\"item.completed\",\"item\":{\"id\":\"message-1\",\"type\":\"agent_message\",\"text\":\"I will inspect.\"}}\n",
            "{\"type\":\"item.completed\",\"item\":{\"id\":\"command-1\",\"type\":\"command_execution\",\"command\":\"printf hi\",\"aggregated_output\":\"hi\",\"exit_code\":0,\"status\":\"completed\"}}\n",
            "{\"type\":\"item.completed\",\"item\":{\"id\":\"patch-1\",\"type\":\"file_change\",\"changes\":[{\"path\":\"greeting.txt\",\"kind\":\"add\"}],\"status\":\"completed\"}}\n",
            "{\"type\":\"item.completed\",\"item\":{\"id\":\"mcp-1\",\"type\":\"mcp_tool_call\",\"server\":\"files\",\"tool\":\"read\",\"arguments\":{\"path\":\"greeting.txt\"},\"result\":null,\"error\":{\"message\":\"missing\"},\"status\":\"failed\"}}\n",
            "{\"type\":\"item.completed\",\"item\":{\"id\":\"todo-1\",\"type\":\"todo_list\",\"items\":[{\"text\":\"finish\",\"completed\":true}]}}\n",
            "{\"type\":\"item.completed\",\"item\":{\"id\":\"message-2\",\"type\":\"agent_message\",\"text\":\"done\"}}\n",
            "{\"type\":\"item.completed\",\"item\":{\"id\":\"command-2\",\"type\":\"command_execution\",\"command\":\"true\",\"aggregated_output\":\"\",\"exit_code\":0,\"status\":\"completed\"}}\n",
            "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":12,\"cached_input_tokens\":3,\"cache_write_input_tokens\":1,\"output_tokens\":8,\"reasoning_output_tokens\":2}}\n",
        );
        fs::write(&events, input).unwrap();
        let mut transcript = CodexTranscript::new();
        for (index, line) in input.lines().enumerate() {
            transcript
                .observe(
                    u64::try_from(index + 1).unwrap(),
                    &serde_json::from_str(line).unwrap(),
                )
                .unwrap();
        }
        let config =
            CodexExec::new(std::env::current_exe().unwrap(), "gpt-5.6-sol", "medium").unwrap();
        let result = transcript
            .agent_result(
                &config,
                Duration::from_millis(10),
                AgentStatus::Completed,
                BillingCompleteness::Complete,
            )
            .unwrap();

        let trajectory =
            project_codex_atif(&events, "complete the task", &result, "codex-cli-test").unwrap();

        assert_eq!(trajectory.session_id, "thread-1");
        assert_eq!(trajectory.agent.name, "codex");
        assert_eq!(trajectory.agent.version, "codex-cli-test");
        assert_eq!(trajectory.steps.len(), 9);
        assert!(matches!(trajectory.steps[0].source, AtifSource::User));
        assert_eq!(
            trajectory.steps[1].reasoning_content.as_deref(),
            Some("inspect first")
        );
        assert_eq!(trajectory.steps[2].message, "I will inspect.");
        let tool_names = trajectory
            .steps
            .iter()
            .filter_map(|step| step.tool_calls.as_ref())
            .flatten()
            .map(|tool| tool.function_name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            tool_names,
            [
                "command_execution",
                "file_change",
                "mcp/files/read",
                "command_execution"
            ]
        );
        let command = trajectory.steps[3].tool_calls.as_ref().unwrap()[0]
            .arguments
            .get();
        assert_eq!(command, r#"{"command":"printf hi"}"#);
        assert!(
            trajectory.steps[3].observation.as_ref().unwrap().results[0]
                .content
                .contains(r#""aggregated_output":"hi""#)
        );
        assert_eq!(
            trajectory.steps[6].reasoning_content.as_deref(),
            Some(r#"[{"completed":true,"text":"finish"}]"#)
        );
        assert_eq!(trajectory.steps[7].message, "done");
        assert!(trajectory.steps[8].message.is_empty());
        assert!(trajectory.steps[8].extra.is_some());
        assert_eq!(
            trajectory
                .steps
                .iter()
                .filter(|step| step.message == "done")
                .count(),
            1
        );
        assert_eq!(trajectory.tool_call_count(), 4);
        assert_eq!(trajectory.observation_count(), 4);
        assert_eq!(trajectory.final_metrics.total_prompt_tokens, 12);
        assert_eq!(trajectory.final_metrics.total_completion_tokens, 8);
        assert_eq!(trajectory.final_metrics.total_cached_tokens, 3);
        assert_eq!(
            trajectory.final_metrics.extra.runtime_completeness,
            MeasurementCompleteness::ObservedLowerBound
        );
        assert_eq!(
            trajectory.final_metrics.extra.usage_completeness,
            Some(MeasurementCompleteness::Complete)
        );
    }

    #[tokio::test]
    async fn evaluator_owned_runner_uses_the_exact_exec_arguments_and_retains_streams() {
        let temporary = tempdir().unwrap();
        let workspace = temporary.path().join("workspace");
        let attempt = temporary.path().join("attempt");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&attempt).unwrap();
        let runner = Arc::new(StaticCommandRunner::default());
        let codex = CodexExec::new(std::env::current_exe().unwrap(), "gpt-5.6-sol", "medium")
            .unwrap()
            .web_search(true)
            .code_mode_only()
            .command_runner(runner.clone());

        let execution = codex
            .run(
                &workspace,
                &attempt,
                "finish the benchmark",
                Duration::from_secs(2),
            )
            .await;

        assert!(execution.error.is_none());
        let result = execution.result.unwrap();
        assert_eq!(result.final_message, "done in guest");
        assert_eq!(result.usage.total_tokens, 5);
        let arguments = runner.arguments.lock().unwrap();
        assert_eq!(arguments.first().map(String::as_str), Some("exec"));
        assert!(arguments.iter().any(|argument| argument == "--ephemeral"));
        assert!(
            arguments
                .iter()
                .any(|argument| argument == "web_search=\"live\"")
        );
        assert!(
            arguments
                .iter()
                .any(|argument| argument == "features.code_mode_only=true")
        );
        assert!(
            arguments
                .iter()
                .any(|argument| argument == "agents.enabled=false")
        );
        assert!(
            arguments
                .iter()
                .any(|argument| argument == "tools.experimental_request_user_input.enabled=false")
        );
        assert!(
            arguments
                .iter()
                .any(|argument| argument == "model_reasoning_summary=\"auto\"")
        );
        assert_eq!(codex.code_mode_only_model(), Some("gpt-5.6-sol"));
        assert_eq!(
            arguments.last().map(String::as_str),
            Some("finish the benchmark")
        );
        assert!(
            fs::read_to_string(attempt.join(EVENTS_FILE))
                .unwrap()
                .contains("thread-runner")
        );
        assert_eq!(
            fs::read_to_string(attempt.join(STDERR_FILE)).unwrap(),
            "guest diagnostic\n"
        );
        assert!(attempt.join(SUMMARY_FILE).is_file());
    }

    #[tokio::test]
    async fn timeout_terminates_the_codex_process_group_and_descendants() {
        let temporary = tempdir().unwrap();
        let workspace = temporary.path().join("workspace");
        let attempt = temporary.path().join("attempt");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&attempt).unwrap();
        let marker = temporary.path().join("descendant-survived");
        let binary = write_timeout_codex(temporary.path(), &marker);
        let codex = CodexExec::new(binary, "gpt-5.6-sol", "medium")
            .unwrap()
            .api_key("test");

        let execution = codex
            .run(
                &workspace,
                &attempt,
                "do the task",
                Duration::from_millis(50),
            )
            .await;

        assert!(matches!(
            execution.error,
            Some(CodexRunError::Timeout(timeout)) if timeout == Duration::from_millis(50)
        ));
        sleep(Duration::from_millis(700)).await;
        assert!(
            !marker.exists(),
            "a process descended from timed-out Codex survived cleanup"
        );
        assert!(attempt.join("agent/codex-events.jsonl").is_file());
        assert!(attempt.join("agent/codex-stderr.log").is_file());
    }

    #[tokio::test]
    async fn successful_parent_exit_still_terminates_leftover_descendants() {
        let temporary = tempdir().unwrap();
        let workspace = temporary.path().join("workspace");
        let attempt = temporary.path().join("attempt");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&attempt).unwrap();
        let marker = temporary.path().join("descendant-survived-success");
        let binary = write_success_with_descendant_codex(temporary.path(), &marker);
        let codex = CodexExec::new(binary, "gpt-5.6-sol", "medium")
            .unwrap()
            .api_key("test");

        let execution = codex
            .run(&workspace, &attempt, "do the task", Duration::from_secs(2))
            .await;

        assert!(execution.error.is_none());
        sleep(Duration::from_millis(700)).await;
        assert!(
            !marker.exists(),
            "a process left behind by successful Codex survived cleanup"
        );
    }

    #[tokio::test]
    async fn malformed_event_does_not_truncate_the_retained_stdout_stream() {
        let temporary = tempdir().unwrap();
        let agent = temporary.path().join("attempt/agent");
        fs::create_dir_all(&agent).unwrap();
        let events = agent.join("codex-events.jsonl");
        let input = b"not-json\r\n{\"type\":\"thread.started\",\"thread_id\":\"later\"}";

        let error = capture_stdout(&input[..], events.clone())
            .await
            .unwrap_err();

        assert!(matches!(error, CodexExecError::EventJson { line: 1, .. }));
        assert_eq!(fs::read(&events).unwrap(), input);
        let summary = fs::read_to_string(agent.join("codex-summary.json")).unwrap();
        assert!(summary.contains("\"thread_id\": \"later\""));
    }

    fn write_timeout_codex(directory: &Path, marker: &Path) -> PathBuf {
        let binary = directory.join("codex-timeout");
        let marker = shell_single_quote(marker);
        fs::write(
            &binary,
            format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' '{{"type":"thread.started","thread_id":"00000000-0000-0000-0000-000000000001"}}'
( sleep 0.4; printf '%s\n' survived > {marker} ) &
sleep 5
"#
            ),
        )
        .unwrap();
        let mut permissions = fs::metadata(&binary).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&binary, permissions).unwrap();
        binary
    }

    fn write_success_with_descendant_codex(directory: &Path, marker: &Path) -> PathBuf {
        let binary = directory.join("codex-success-descendant");
        let marker = shell_single_quote(marker);
        fs::write(
            &binary,
            format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' '{{"type":"thread.started","thread_id":"00000000-0000-0000-0000-000000000001"}}'
( sleep 0.4; printf '%s\n' survived > {marker} ) &
printf '%s\n' '{{"type":"item.completed","item":{{"id":"item-1","type":"agent_message","text":"done"}}}}'
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":1,"cached_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0}}}}'
"#
            ),
        )
        .unwrap();
        let mut permissions = fs::metadata(&binary).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&binary, permissions).unwrap();
        binary
    }

    fn shell_single_quote(path: &Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', "'\"'\"'"))
    }
}
