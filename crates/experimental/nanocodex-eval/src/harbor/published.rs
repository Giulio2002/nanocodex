use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    path::{Path, PathBuf},
    process::ExitStatus,
    time::Duration,
};

use futures_util::{StreamExt, stream};
use reqwest::{Client, StatusCode, Url, header::RETRY_AFTER};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use tokio::{fs, process::Command};
use uuid::Uuid;

use crate::{EvalCleanup, EvalOutcome, infer_retained_scored};

const DEFAULT_REPOSITORY: &str =
    "https://huggingface.co/datasets/harborframework/terminal-bench-2-leaderboard";
const DEFAULT_DOWNLOAD_BASE: &str =
    "https://huggingface.co/datasets/harborframework/terminal-bench-2-leaderboard/resolve/";
const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const DOWNLOAD_CONCURRENCY: usize = 4;
const DOWNLOAD_ATTEMPTS: u32 = 8;

#[derive(Debug, thiserror::Error)]
/// An error produced while reading Harbor's published result archive.
pub enum PublishedError {
    /// A filesystem or Git process I/O operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A published JSON artifact could not be decoded.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// An HTTP request for a published artifact failed.
    #[error(transparent)]
    Http(#[from] reqwest::Error),

    /// A configured artifact base was not a valid URL.
    #[error(transparent)]
    Url(#[from] url::ParseError),

    /// The configured cache path had no parent directory.
    #[error("published-results cache path has no parent: {0}")]
    MissingCacheParent(PathBuf),

    /// A Git operation on the archive index exited unsuccessfully.
    #[error("git {operation} failed with {status}: {stderr}")]
    Git {
        /// The operation being performed.
        operation: &'static str,
        /// The Git process exit status.
        status: ExitStatus,
        /// Git's standard-error output.
        stderr: String,
    },

    /// Git emitted non-UTF-8 output for a textual result.
    #[error("git returned non-UTF-8 output while {0}")]
    GitUtf8(&'static str),

    /// A remote artifact exceeded the bounded download size.
    #[error("published artifact is larger than {limit} bytes: {url}")]
    ArtifactTooLarge {
        /// The maximum accepted artifact size in bytes.
        limit: usize,
        /// The rejected artifact URL.
        url: Url,
    },

    /// A referenced artifact does not exist in the archive.
    #[error("published artifact does not exist: {0}")]
    MissingArtifact(Url),

    /// An archive path did not match Harbor's result layout.
    #[error("invalid published result path: {0}")]
    InvalidResultPath(String),
}

#[derive(Clone, Debug)]
/// Builder for a cached [`PublishedResults`] reader.
pub struct PublishedResultsBuilder {
    cache_directory: PathBuf,
    repository: String,
    download_base: String,
    refresh: bool,
}

impl PublishedResultsBuilder {
    /// Sets the local directory used for the Git index and downloaded artifacts.
    #[must_use]
    pub fn cache_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.cache_directory = directory.into();
        self
    }

    /// Refreshes the archive's small Git tree index before querying.
    #[must_use]
    pub const fn refresh(mut self, refresh: bool) -> Self {
        self.refresh = refresh;
        self
    }

    /// Overrides the metadata repository and artifact base, primarily for mirrors.
    #[must_use]
    pub fn source(mut self, repository: impl Into<String>, download_base: &str) -> Self {
        self.repository = repository.into();
        download_base.clone_into(&mut self.download_base);
        self
    }

    /// Builds the reader after validating its artifact URL.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured artifact base is not a valid URL.
    pub fn build(self) -> Result<PublishedResults, PublishedError> {
        Ok(PublishedResults {
            cache_directory: self.cache_directory,
            repository: self.repository,
            download_base: Url::parse(&self.download_base)?,
            refresh: self.refresh,
            client: Client::new(),
        })
    }
}

#[derive(Clone, Debug)]
/// Cached, typed access to Harbor's published Terminal-Bench results.
pub struct PublishedResults {
    cache_directory: PathBuf,
    repository: String,
    download_base: Url,
    refresh: bool,
    client: Client,
}

impl PublishedResults {
    /// Builds a cached reader for Harbor's public Terminal-Bench archive.
    #[must_use]
    pub fn builder() -> PublishedResultsBuilder {
        PublishedResultsBuilder {
            cache_directory: PathBuf::from(".cache/nanocodex/eval/published"),
            repository: DEFAULT_REPOSITORY.to_owned(),
            download_base: DEFAULT_DOWNLOAD_BASE.to_owned(),
            refresh: false,
        }
    }

    /// Finds successful published attempts for one task and downloads their
    /// typed result and ATIF artifacts into the content-addressed local cache.
    ///
    /// # Errors
    ///
    /// Returns an error when the archive index cannot be prepared, an artifact
    /// cannot be downloaded, or a published JSON document is malformed.
    pub async fn query(&self, query: &PublishedQuery) -> Result<PublishedTask, PublishedError> {
        let (revision, records) = self.load_query_records(query).await?;
        let matching_results = records.len();

        let mut passing = Vec::new();
        for record in records {
            let PublishedRecord { candidate, result } = record;
            let reward = result.reward();
            if reward <= 0.0 || !result.is_scored() {
                continue;
            }
            if !query.agent_matches(&candidate.submission, &result) {
                continue;
            }
            passing.push((candidate, result, reward));
        }
        passing.sort_by(|left, right| {
            let left_exact = query.matches_checksum(&left.1.task_checksum);
            let right_exact = query.matches_checksum(&right.1.task_checksum);
            right_exact
                .cmp(&left_exact)
                .then_with(|| {
                    right
                        .0
                        .trajectory
                        .is_some()
                        .cmp(&left.0.trajectory.is_some())
                })
                .then_with(|| left.0.submission.cmp(&right.0.submission))
        });

        let passing_results = passing.len();
        let exact_passing_results = passing
            .iter()
            .filter(|(_, result, _)| query.matches_checksum(&result.task_checksum))
            .count();
        let mut submissions = BTreeSet::new();
        let selected = passing
            .into_iter()
            .filter(|(candidate, _, _)| submissions.insert(candidate.submission.clone()))
            .take(query.limit)
            .collect::<Vec<_>>();
        let published = self;
        let archive_revision = &revision;
        let trials = stream::iter(selected.into_iter().map(
            move |(candidate, result, reward)| async move {
                let (trajectory, trajectory_error) = match candidate.trajectory.as_deref() {
                    Some(trajectory_path) => {
                        match published.download(archive_revision, trajectory_path).await {
                            Ok(bytes) => match decode_published_trajectory(&bytes) {
                                Ok(trajectory) => (Some(trajectory), None),
                                Err(error) => (None, Some(error.to_string())),
                            },
                            Err(error) => (None, Some(error.to_string())),
                        }
                    }
                    None => (None, None),
                };
                let thinking = result
                    .config
                    .as_ref()
                    .and_then(PublishedTrialConfig::thinking);
                let agent_import_path = result
                    .config
                    .as_ref()
                    .and_then(PublishedTrialConfig::agent_import_path);
                let agent = result.resolved_agent_info();
                Ok::<_, PublishedError>(PublishedTrial {
                    submission: candidate.submission,
                    run: candidate.run,
                    trial_name: result.trial_name,
                    task_name: result.task_name,
                    task_checksum: result.task_checksum,
                    reward,
                    agent,
                    thinking,
                    agent_import_path,
                    result_path: candidate.result,
                    trajectory_path: candidate.trajectory,
                    trajectory,
                    trajectory_error,
                })
            },
        ))
        .buffered(DOWNLOAD_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        Ok(PublishedTask {
            task: query.task.clone(),
            requested_checksum: query.checksum.clone(),
            archive_revision: revision,
            matching_results,
            passing_results,
            exact_passing_results,
            trials,
        })
    }

    /// Loads typed per-attempt metadata without downloading trajectories.
    ///
    /// This is the fast path for exact-revision aggregate comparisons across
    /// multiple tasks or retained jobs.
    ///
    /// # Errors
    ///
    /// Returns an error when the archive index cannot be prepared, a result
    /// cannot be downloaded, or a published JSON document is malformed.
    pub async fn attempts(
        &self,
        query: &PublishedQuery,
    ) -> Result<PublishedAttempts, PublishedError> {
        let (revision, records) = self.load_query_records(query).await?;
        let attempts = records
            .into_iter()
            .filter(|record| query.agent_matches(&record.candidate.submission, &record.result))
            .map(|record| {
                let PublishedRecord { candidate, result } = record;
                let scored = result.is_scored();
                let passed = scored && result.reward() > 0.0;
                let errored = result.is_errored();
                let refused = result.is_refused();
                let cleanup_failed = result.is_cleanup_failed();
                let thinking = result
                    .config
                    .as_ref()
                    .and_then(PublishedTrialConfig::thinking);
                let agent_import_path = result
                    .config
                    .as_ref()
                    .and_then(PublishedTrialConfig::agent_import_path);
                let agent = result.resolved_agent_info();
                PublishedAttempt {
                    submission: candidate.submission,
                    run: candidate.run,
                    task_name: result.task_name,
                    task_checksum: result.task_checksum,
                    trial_name: result.trial_name,
                    scored,
                    passed,
                    errored,
                    refused,
                    cleanup_failed,
                    agent,
                    thinking,
                    agent_import_path,
                    result_path: candidate.result,
                    trajectory_path: candidate.trajectory,
                }
            })
            .collect();
        Ok(PublishedAttempts {
            task: query.task.clone(),
            requested_checksum: query.checksum.clone(),
            archive_revision: revision,
            attempts,
        })
    }

    async fn load_query_records(
        &self,
        query: &PublishedQuery,
    ) -> Result<(String, Vec<PublishedRecord>), PublishedError> {
        let index = self.prepare_index().await?;
        let revision = self
            .git_output(&index, "read archive revision", &["rev-parse", "HEAD"])
            .await?
            .trim()
            .to_owned();
        let candidates = self.load_task_paths(&index, &revision, &query.task).await?;
        let records = self
            .load_task_results(&revision, &query.task, candidates)
            .await?;
        Ok((revision, records))
    }

    async fn prepare_index(&self) -> Result<PathBuf, PublishedError> {
        let index = self.cache_directory.join("terminal-bench-2-leaderboard");
        if !index.join(".git").is_dir() {
            let parent = index
                .parent()
                .ok_or_else(|| PublishedError::MissingCacheParent(index.clone()))?;
            fs::create_dir_all(parent).await?;
            let output = Command::new("git")
                .args([
                    "clone",
                    "--filter=blob:none",
                    "--no-checkout",
                    "--depth",
                    "1",
                    &self.repository,
                ])
                .arg(&index)
                .output()
                .await?;
            ensure_git("clone archive index", output.status, &output.stderr)?;
        } else if self.refresh {
            let output = Command::new("git")
                .current_dir(&index)
                .args(["fetch", "--depth", "1", "origin", "main"])
                .output()
                .await?;
            ensure_git("refresh archive index", output.status, &output.stderr)?;
            let output = Command::new("git")
                .current_dir(&index)
                .args(["update-ref", "HEAD", "FETCH_HEAD"])
                .output()
                .await?;
            ensure_git(
                "select refreshed archive index",
                output.status,
                &output.stderr,
            )?;
        }
        Ok(index)
    }

    async fn load_task_results(
        &self,
        revision: &str,
        task: &str,
        candidates: Vec<PublishedPath>,
    ) -> Result<Vec<PublishedRecord>, PublishedError> {
        let task_key = hex::encode(Sha256::digest(task.as_bytes()));
        let manifest = self
            .cache_directory
            .join("task-results-v3")
            .join(revision)
            .join(format!("{task_key}.json"));
        if manifest.is_file() {
            return Ok(serde_json::from_slice(&fs::read(manifest).await?)?);
        }

        let downloads = stream::iter(candidates.into_iter().map(|candidate| async {
            let bytes = self.download(revision, &candidate.result).await?;
            let result = serde_json::from_slice::<PublishedResult>(&bytes)?;
            Ok::<_, PublishedError>(PublishedRecord { candidate, result })
        }))
        .buffer_unordered(DOWNLOAD_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        if let Some(parent) = manifest.parent() {
            fs::create_dir_all(parent).await?;
        }
        let temporary = manifest.with_extension(format!("{}.tmp", Uuid::now_v7()));
        fs::write(&temporary, serde_json::to_vec(&downloads)?).await?;
        fs::rename(temporary, manifest).await?;
        Ok(downloads)
    }

    async fn load_task_paths(
        &self,
        index: &Path,
        revision: &str,
        task: &str,
    ) -> Result<Vec<PublishedPath>, PublishedError> {
        let task_key = hex::encode(Sha256::digest(task.as_bytes()));
        let manifest = self
            .cache_directory
            .join("task-paths")
            .join(revision)
            .join(format!("{task_key}.json"));
        if manifest.is_file() {
            return Ok(serde_json::from_slice(&fs::read(manifest).await?)?);
        }

        let tree = self
            .git_output(
                index,
                "list archive tree",
                &["ls-tree", "-r", "--name-only", "HEAD"],
            )
            .await?;
        let entries = tree.lines().collect::<HashSet<_>>();
        let mut candidates = entries
            .iter()
            .filter(|entry| is_task_result(entry, task))
            .map(|entry| PublishedPath::parse(entry, &entries))
            .collect::<Result<Vec<_>, _>>()?;
        candidates.sort_by(|left, right| left.result.cmp(&right.result));
        if let Some(parent) = manifest.parent() {
            fs::create_dir_all(parent).await?;
        }
        let temporary = manifest.with_extension(format!("{}.tmp", Uuid::now_v7()));
        fs::write(&temporary, serde_json::to_vec(&candidates)?).await?;
        fs::rename(temporary, manifest).await?;
        Ok(candidates)
    }

    async fn git_output(
        &self,
        index: &Path,
        operation: &'static str,
        arguments: &[&str],
    ) -> Result<String, PublishedError> {
        let output = Command::new("git")
            .current_dir(index)
            .args(arguments)
            .output()
            .await?;
        ensure_git(operation, output.status, &output.stderr)?;
        String::from_utf8(output.stdout).map_err(|_| PublishedError::GitUtf8(operation))
    }

    async fn download(&self, revision: &str, artifact: &str) -> Result<Vec<u8>, PublishedError> {
        let cached = self
            .cache_directory
            .join("artifacts")
            .join(revision)
            .join(artifact);
        if cached.is_file() {
            return Ok(fs::read(cached).await?);
        }

        let mut url = self.download_base.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| PublishedError::MissingCacheParent(cached.clone()))?;
            segments.pop_if_empty();
            segments.push(revision);
            for segment in artifact.split('/') {
                segments.push(segment);
            }
        }
        let mut attempt = 0;
        let response = loop {
            let response = self.client.get(url.clone()).send().await?;
            if response.status() != StatusCode::TOO_MANY_REQUESTS
                && !response.status().is_server_error()
            {
                break response;
            }
            attempt += 1;
            if attempt >= DOWNLOAD_ATTEMPTS {
                break response;
            }
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs);
            let exponential = Duration::from_millis(250 * 2_u64.pow(attempt - 1));
            tokio::time::sleep(retry_after.unwrap_or(exponential)).await;
        };
        if response.status() == StatusCode::NOT_FOUND {
            return Err(PublishedError::MissingArtifact(url));
        }
        let response = response.error_for_status()?;
        if response.content_length().is_some_and(|size| {
            usize::try_from(size).map_or(true, |size| size > MAX_ARTIFACT_BYTES)
        }) {
            return Err(PublishedError::ArtifactTooLarge {
                limit: MAX_ARTIFACT_BYTES,
                url,
            });
        }
        let bytes = response.bytes().await?;
        if bytes.len() > MAX_ARTIFACT_BYTES {
            return Err(PublishedError::ArtifactTooLarge {
                limit: MAX_ARTIFACT_BYTES,
                url,
            });
        }
        if let Some(parent) = cached.parent() {
            fs::create_dir_all(parent).await?;
        }
        let temporary = cached.with_extension(format!("{}.tmp", Uuid::now_v7()));
        fs::write(&temporary, &bytes).await?;
        fs::rename(temporary, &cached).await?;
        Ok(bytes.to_vec())
    }
}

#[derive(Clone, Debug)]
/// Filters one task's records in Harbor's published result archive.
pub struct PublishedQuery {
    task: String,
    checksum: Option<String>,
    limit: usize,
    agents: Vec<String>,
}

impl PublishedQuery {
    /// Creates a query for a Terminal-Bench task name.
    ///
    /// An optional `terminal-bench/` prefix is removed automatically.
    #[must_use]
    pub fn new(task: impl Into<String>) -> Self {
        let task = task.into();
        Self {
            task: task
                .strip_prefix("terminal-bench/")
                .unwrap_or(&task)
                .to_owned(),
            checksum: None,
            limit: 10,
            agents: Vec::new(),
        }
    }

    /// Prefers and counts attempts matching an exact task checksum.
    #[must_use]
    pub fn checksum(mut self, checksum: impl Into<String>) -> Self {
        self.checksum = Some(checksum.into());
        self
    }

    /// Limits the number of distinct passing submissions whose trajectories are downloaded.
    #[must_use]
    pub const fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Restricts results to submissions, agents, or models containing `agent`.
    ///
    /// Calling this method more than once matches any configured value.
    #[must_use]
    pub fn agent(mut self, agent: impl Into<String>) -> Self {
        self.agents.push(agent.into());
        self
    }

    fn matches_checksum(&self, checksum: &str) -> bool {
        self.checksum
            .as_deref()
            .is_some_and(|expected| checksum == expected)
    }

    fn agent_matches(&self, submission: &str, result: &PublishedResult) -> bool {
        self.agents.is_empty()
            || self.agents.iter().any(|needle| {
                let needle = needle.to_lowercase();
                submission.to_lowercase().contains(&needle)
                    || result.agent_info.name.to_lowercase().contains(&needle)
                    || result
                        .agent_info
                        .model_info
                        .as_ref()
                        .is_some_and(|model| model.name.to_lowercase().contains(&needle))
                    || result
                        .config
                        .as_ref()
                        .is_some_and(|config| config.agent_matches(&needle))
            })
    }
}

#[derive(Clone, Debug, Serialize)]
/// Passing published trials and aggregate counts for one task query.
pub struct PublishedTask {
    /// The normalized Terminal-Bench task name.
    pub task: String,
    /// The exact checksum requested by the caller, if any.
    pub requested_checksum: Option<String>,
    /// The immutable Git revision used for the query.
    pub archive_revision: String,
    /// The number of result documents found before pass and agent filtering.
    pub matching_results: usize,
    /// The number of passing results matching the agent filters.
    pub passing_results: usize,
    /// The number of passing results with the requested checksum.
    pub exact_passing_results: usize,
    /// Selected passing trials, including downloaded trajectories when available.
    pub trials: Vec<PublishedTrial>,
}

#[derive(Clone, Debug, Serialize)]
/// Typed attempt metadata for one task query without downloaded trajectories.
pub struct PublishedAttempts {
    /// The normalized Terminal-Bench task name.
    pub task: String,
    /// The exact checksum requested by the caller, if any.
    pub requested_checksum: Option<String>,
    /// The immutable Git revision used for the query.
    pub archive_revision: String,
    /// Every attempt matching the query's agent filters.
    pub attempts: Vec<PublishedAttempt>,
}

#[derive(Clone, Debug, Serialize)]
/// Metadata for one attempt in Harbor's published result archive.
pub struct PublishedAttempt {
    /// The archive submission name.
    pub submission: String,
    /// The run directory within the submission.
    pub run: String,
    /// The task name reported by Harbor.
    pub task_name: String,
    /// The packaged task checksum reported by Harbor.
    pub task_checksum: String,
    /// The trial name reported by Harbor.
    pub trial_name: String,
    /// Whether the attempt retained verifier evidence.
    pub scored: bool,
    /// Whether the verifier awarded a positive reward.
    pub passed: bool,
    /// Whether the trial retained a non-cleanup lifecycle exception.
    pub errored: bool,
    /// Whether the agent refused the task for a provider safety policy.
    pub refused: bool,
    /// Whether a retained agent or verifier cleanup boundary failed.
    pub cleanup_failed: bool,
    /// The published agent and model identity.
    pub agent: PublishedAgentInfo,
    /// The configured reasoning effort, when recorded.
    pub thinking: Option<String>,
    /// The configured Harbor agent import path, when recorded.
    pub agent_import_path: Option<String>,
    /// The result artifact's path within the archive.
    pub result_path: String,
    /// The trajectory artifact's path within the archive, when present.
    pub trajectory_path: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
/// One selected passing trial, optionally including its decoded trajectory.
pub struct PublishedTrial {
    /// The archive submission name.
    pub submission: String,
    /// The run directory within the submission.
    pub run: String,
    /// The trial name reported by Harbor.
    pub trial_name: String,
    /// The task name reported by Harbor.
    pub task_name: String,
    /// The packaged task checksum reported by Harbor.
    pub task_checksum: String,
    /// The verifier's scalar reward.
    pub reward: f64,
    /// The published agent and model identity.
    pub agent: PublishedAgentInfo,
    /// The configured reasoning effort, when recorded.
    pub thinking: Option<String>,
    /// The configured Harbor agent import path, when recorded.
    pub agent_import_path: Option<String>,
    /// The result artifact's path within the archive.
    pub result_path: String,
    /// The trajectory artifact's path within the archive, when present.
    pub trajectory_path: Option<String>,
    /// The decoded ATIF trajectory, when available and valid.
    pub trajectory: Option<PublishedTrajectory>,
    /// A trajectory download or decode error that did not invalidate the result.
    pub trajectory_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
/// Agent identity recorded in a published Harbor result.
pub struct PublishedAgentInfo {
    /// The agent implementation name.
    pub name: String,
    /// The agent implementation version, when recorded.
    pub version: Option<String>,
    /// Model identity recorded for the attempt, when available.
    pub model_info: Option<PublishedModelInfo>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
/// Model identity recorded in a published Harbor result.
pub struct PublishedModelInfo {
    /// The model name.
    pub name: String,
    /// The model provider, when recorded.
    pub provider: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PublishedResult {
    task_name: String,
    task_checksum: String,
    trial_name: String,
    agent_info: PublishedAgentInfo,
    #[serde(default)]
    config: Option<PublishedTrialConfig>,
    #[serde(default)]
    outcome: Option<EvalOutcome>,
    #[serde(default)]
    scored: Option<bool>,
    verifier_result: Option<PublishedVerifierResult>,
    exception_info: Option<Box<RawValue>>,
    #[serde(default)]
    cleanup: EvalCleanup,
}

#[derive(Deserialize)]
struct PublishedException {
    exception_type: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PublishedTrialConfig {
    agent: PublishedAgentConfig,
}

impl PublishedTrialConfig {
    fn thinking(&self) -> Option<String> {
        self.agent
            .kwargs
            .effort
            .clone()
            .or_else(|| self.agent.kwargs.reasoning_effort.clone())
    }

    fn agent_import_path(&self) -> Option<String> {
        self.agent.import_path.clone()
    }

    fn agent_matches(&self, needle: &str) -> bool {
        self.agent
            .model_name
            .as_ref()
            .is_some_and(|model| model.to_lowercase().contains(needle))
            || self
                .agent
                .import_path
                .as_ref()
                .is_some_and(|path| path.to_lowercase().contains(needle))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PublishedAgentConfig {
    #[serde(default)]
    model_name: Option<String>,
    #[serde(default)]
    import_path: Option<String>,
    #[serde(default)]
    kwargs: PublishedAgentKwargs,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct PublishedAgentKwargs {
    effort: Option<String>,
    reasoning_effort: Option<String>,
}

impl PublishedResult {
    const fn is_scored(&self) -> bool {
        infer_retained_scored(
            self.scored,
            self.outcome,
            self.verifier_result.is_some(),
            self.exception_info.is_some(),
        )
    }

    fn is_errored(&self) -> bool {
        match self.exception_info.as_ref() {
            Some(exception) => serde_json::from_str::<PublishedException>(exception.get())
                .map_or(true, |exception| exception.exception_type != "CleanupError"),
            None => self.outcome.is_some_and(|outcome| {
                matches!(
                    outcome,
                    EvalOutcome::SafetyRefusal
                        | EvalOutcome::AgentTimeout
                        | EvalOutcome::InfrastructureError
                )
            }),
        }
    }

    fn is_refused(&self) -> bool {
        match self.exception_info.as_ref() {
            Some(exception) => serde_json::from_str::<PublishedException>(exception.get())
                .is_ok_and(|exception| exception.exception_type == "AgentSafetyRefusalError"),
            None => matches!(self.outcome, Some(EvalOutcome::SafetyRefusal)),
        }
    }

    fn is_cleanup_failed(&self) -> bool {
        self.cleanup.is_failed()
            || self.exception_info.as_ref().is_some_and(|exception| {
                serde_json::from_str::<PublishedException>(exception.get())
                    .is_ok_and(|exception| exception.exception_type == "CleanupError")
            })
    }

    fn reward(&self) -> f64 {
        self.verifier_result
            .as_ref()
            .and_then(|result| result.rewards.get("reward"))
            .copied()
            .unwrap_or(0.0)
    }

    fn resolved_agent_info(&self) -> PublishedAgentInfo {
        let mut agent = self.agent_info.clone();
        if agent.model_info.is_none()
            && let Some(model) = self
                .config
                .as_ref()
                .and_then(|config| config.agent.model_name.as_ref())
        {
            agent.model_info = Some(PublishedModelInfo {
                name: model.strip_prefix("openai/").unwrap_or(model).to_owned(),
                provider: None,
            });
        }
        agent
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PublishedVerifierResult {
    rewards: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
/// A decoded ATIF trajectory from Harbor's published archive.
pub struct PublishedTrajectory {
    /// The ATIF schema version.
    pub schema_version: String,
    /// The source agent session identifier, when recorded.
    pub session_id: Option<String>,
    /// The trajectory's agent identity.
    pub agent: PublishedAgent,
    /// Ordered agent, tool, and observation steps.
    pub steps: Vec<PublishedStep>,
}

#[derive(Debug, Deserialize)]
struct WrappedPublishedTrajectory {
    atif_trajectory: PublishedTrajectory,
}

fn decode_published_trajectory(bytes: &[u8]) -> Result<PublishedTrajectory, serde_json::Error> {
    match serde_json::from_slice(bytes) {
        Ok(trajectory) => Ok(trajectory),
        Err(_) => serde_json::from_slice::<WrappedPublishedTrajectory>(bytes)
            .map(|wrapped| wrapped.atif_trajectory),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
/// Agent identity as either structured metadata or a legacy name.
pub enum PublishedAgent {
    /// Structured agent metadata.
    Details(PublishedAgentDetails),
    /// A legacy agent name.
    Name(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
/// Structured agent metadata in a published trajectory.
pub struct PublishedAgentDetails {
    /// The agent implementation name.
    pub name: String,
    /// The agent implementation version, when recorded.
    pub version: Option<String>,
    /// The model name, when recorded.
    pub model_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
/// One ordered step in a published ATIF trajectory.
pub struct PublishedStep {
    /// The source trajectory's numeric or textual step identifier.
    pub step_id: PublishedStepId,
    /// The source timestamp, when recorded.
    pub timestamp: Option<String>,
    /// The step producer, such as `agent`, `tool`, or `user`.
    pub source: String,
    /// The model that produced the step, when recorded.
    pub model_name: Option<String>,
    #[serde(alias = "content")]
    /// User-visible message content, including legacy `content` fields.
    pub message: Option<String>,
    /// Model reasoning content exposed by the source artifact.
    pub reasoning_content: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_vec")]
    /// Tool calls requested by this step.
    pub tool_calls: Vec<PublishedToolCall>,
    /// Tool results observed by this step.
    pub observation: Option<PublishedObservation>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
/// A published trajectory step identifier.
pub enum PublishedStepId {
    /// A numeric ATIF step identifier.
    Number(u64),
    /// A textual identifier used by some legacy publishers.
    Text(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
/// A tool call recorded in a published trajectory.
pub struct PublishedToolCall {
    #[serde(default)]
    /// The source tool-call identifier, when recorded.
    pub tool_call_id: Option<String>,
    #[serde(alias = "tool_name")]
    /// The invoked function name.
    pub function_name: String,
    #[serde(alias = "parameters")]
    /// The retained raw JSON arguments.
    pub arguments: Box<RawValue>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
/// Tool results recorded in a published trajectory step.
pub struct PublishedObservation {
    #[serde(default)]
    /// Ordered tool results.
    pub results: Vec<PublishedObservationResult>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
/// One tool result recorded in a published trajectory.
pub struct PublishedObservationResult {
    #[serde(default)]
    /// The tool call that produced this result.
    pub source_call_id: String,
    /// The tool's textual result content.
    pub content: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PublishedPath {
    submission: String,
    run: String,
    result: String,
    trajectory: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PublishedRecord {
    candidate: PublishedPath,
    result: PublishedResult,
}

impl PublishedPath {
    fn parse(result: &str, entries: &HashSet<&str>) -> Result<Self, PublishedError> {
        let components = result.split('/').collect::<Vec<_>>();
        if components.len() < 7 {
            return Err(PublishedError::InvalidResultPath(result.to_owned()));
        }
        let submission = components
            .get(3)
            .ok_or_else(|| PublishedError::InvalidResultPath(result.to_owned()))?
            .to_string();
        let run = components
            .get(4)
            .ok_or_else(|| PublishedError::InvalidResultPath(result.to_owned()))?
            .to_string();
        let trajectory = format!(
            "{}/agent/trajectory.json",
            result
                .strip_suffix("/result.json")
                .ok_or_else(|| PublishedError::InvalidResultPath(result.to_owned()))?
        );
        Ok(Self {
            submission,
            run,
            result: result.to_owned(),
            trajectory: entries.contains(trajectory.as_str()).then_some(trajectory),
        })
    }
}

fn is_task_result(entry: &str, task: &str) -> bool {
    entry.ends_with("/result.json")
        && entry
            .rsplit('/')
            .nth(1)
            .is_some_and(|trial| trial.starts_with(&format!("{task}__")))
}

fn ensure_git(
    operation: &'static str,
    status: ExitStatus,
    stderr: &[u8],
) -> Result<(), PublishedError> {
    if status.success() {
        Ok(())
    } else {
        Err(PublishedError::Git {
            operation,
            status,
            stderr: String::from_utf8_lossy(stderr).trim().to_owned(),
        })
    }
}

fn deserialize_optional_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<Vec<T>>::deserialize(deserializer).map(Option::unwrap_or_default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_result_matching_is_segment_aware() {
        assert!(is_task_result(
            "submissions/terminal-bench/2.0/a/run/configure-git-webserver__abc/result.json",
            "configure-git-webserver"
        ));
        assert!(!is_task_result(
            "submissions/terminal-bench/2.0/a/run/not-configure-git-webserver__abc/result.json",
            "configure-git-webserver"
        ));
    }

    #[test]
    fn published_path_finds_optional_trajectory() {
        let result = "submissions/terminal-bench/2.0/agent/run/task__abc/result.json".to_owned();
        let trajectory =
            "submissions/terminal-bench/2.0/agent/run/task__abc/agent/trajectory.json".to_owned();
        let entries = HashSet::from([result.as_str(), trajectory.as_str()]);
        let parsed = PublishedPath::parse(&result, &entries).unwrap();
        assert_eq!(parsed.submission, "agent");
        assert_eq!(parsed.run, "run");
        assert_eq!(parsed.trajectory.as_deref(), Some(trajectory.as_str()));
    }

    #[test]
    fn query_normalizes_package_prefix() {
        let query = PublishedQuery::new("terminal-bench/task");
        assert_eq!(query.task, "task");
    }

    #[test]
    fn legacy_scores_and_lifecycle_errors_remain_independent() {
        let mut result = PublishedResult {
            task_name: "task".to_owned(),
            task_checksum: "checksum".to_owned(),
            trial_name: "task__trial".to_owned(),
            agent_info: PublishedAgentInfo {
                name: "agent".to_owned(),
                version: None,
                model_info: None,
            },
            config: None,
            outcome: None,
            scored: None,
            verifier_result: Some(PublishedVerifierResult {
                rewards: BTreeMap::from([("reward".to_owned(), 1.0)]),
            }),
            exception_info: None,
            cleanup: EvalCleanup::default(),
        };

        assert!(result.is_scored());
        assert!(!result.is_errored());
        assert!(!result.is_refused());
        assert!(!result.is_cleanup_failed());

        result.outcome = Some(EvalOutcome::AgentTimeout);
        assert!(!result.is_scored());

        result.scored = Some(true);
        result.exception_info = Some(
            RawValue::from_string(r#"{"exception_type":"AgentTimeoutError"}"#.to_owned()).unwrap(),
        );
        assert!(result.is_scored());

        result.scored = None;
        result.outcome = None;
        assert!(!result.is_scored());

        result.scored = Some(false);
        assert!(!result.is_scored());

        result.verifier_result = None;
        assert!(!result.is_scored());
        assert!(result.is_errored());
        assert!(!result.is_refused());
        assert!(!result.is_cleanup_failed());

        result.exception_info =
            Some(RawValue::from_string(r#"{"exception_type":"CleanupError"}"#.to_owned()).unwrap());
        assert!(!result.is_scored());
        assert!(!result.is_errored());
        assert!(!result.is_refused());
        assert!(result.is_cleanup_failed());

        result.outcome = Some(EvalOutcome::SafetyRefusal);
        assert!(!result.is_errored());
        assert!(!result.is_refused());

        result.exception_info = None;
        assert!(result.is_errored());
        assert!(result.is_refused());

        result.exception_info = Some(
            RawValue::from_string(r#"{"exception_type":"AgentSafetyRefusalError"}"#.to_owned())
                .unwrap(),
        );
        result.outcome = None;
        assert!(result.is_errored());
        assert!(result.is_refused());

        result.exception_info = None;
        result.cleanup.agent.status = crate::CleanupStatus::Failed;
        assert!(!result.is_errored());
        assert!(!result.is_refused());
        assert!(result.is_cleanup_failed());
    }
}
