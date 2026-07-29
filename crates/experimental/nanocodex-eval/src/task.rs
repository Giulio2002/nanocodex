use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::digest::TaskPackage;

const TASK_CONFIG: &str = "task.toml";
const TASK_INSTRUCTION: &str = "instruction.md";
const TASK_ENVIRONMENT: &str = "environment";
const VERIFIER_SCRIPT: &str = "tests/test.sh";

/// One immutable benchmark task loaded from a Terminal-Bench task directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Task {
    root: PathBuf,
    content_digest: Box<str>,
    package: Arc<TaskPackage>,
    name: Box<str>,
    description: Box<str>,
    prompt: Box<str>,
    image: OciImage,
    agent_timeout: Duration,
    verifier: Verifier,
    artifacts: Vec<PathBuf>,
    resources: Resources,
    network: NetworkPolicy,
    environment: BTreeMap<String, String>,
    requires_compose: bool,
}

/// OCI image declared by a benchmark task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OciImage {
    reference: Box<str>,
}

/// Verifier recipe loaded from a task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Verifier {
    script: PathBuf,
    timeout: Duration,
    environment: BTreeMap<String, String>,
    environment_mode: VerifierEnvironmentMode,
    collect: Vec<VerifierCollect>,
}

/// Whether verification reuses the agent environment or a separate image.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VerifierEnvironmentMode {
    /// Run verification in the mutated agent environment.
    #[default]
    Same,
    /// Run verification in the task's dedicated `tests/Dockerfile` image.
    Separate,
}

/// One post-verifier artifact collection command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerifierCollect {
    command: Box<str>,
}

/// Task-declared resource requirements used by admission and VM sizing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Resources {
    /// Virtual CPU count.
    pub cpus: u32,
    /// Required memory in mebibytes.
    pub memory_mb: u64,
    /// Required storage in mebibytes.
    pub storage_mb: u64,
    /// Required GPU count.
    pub gpus: u32,
}

/// Task network policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkPolicy {
    /// The task may reach public network destinations.
    Public,
    /// The task must run without a network device.
    Disabled,
}

/// Failure to load or validate an immutable task directory.
#[derive(Debug, thiserror::Error)]
pub enum TaskLoadError {
    /// The task root could not be canonicalized.
    #[error("failed to resolve task directory {path}: {source}")]
    ResolveDirectory {
        /// Requested task path.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: std::io::Error,
    },

    /// A task file could not be read.
    #[error("failed to read task file {path}: {source}")]
    Read {
        /// File that could not be read.
        path: PathBuf,
        /// Filesystem failure.
        #[source]
        source: std::io::Error,
    },

    /// `task.toml` was not valid TOML.
    #[error("failed to parse task configuration {path}: {source}")]
    Parse {
        /// Configuration path.
        path: PathBuf,
        /// TOML parser failure.
        #[source]
        source: toml::de::Error,
    },

    /// The manifest declares an unsupported schema revision.
    #[error("unsupported task schema version {found:?}; expected \"1.1\"")]
    UnsupportedSchema {
        /// Unsupported revision read from the manifest.
        found: String,
    },

    /// A known task field or directory shape is invalid.
    #[error("task configuration {path} is invalid: {message}")]
    Invalid {
        /// File or directory containing the invalid value.
        path: PathBuf,
        /// Validation failure.
        message: String,
    },

    /// A required task file or directory is absent.
    #[error("task is missing required file {path}")]
    MissingFile {
        /// Missing path.
        path: PathBuf,
    },

    /// The immutable task package could not be fingerprinted.
    #[error("failed to fingerprint task package {path}: {source}")]
    Fingerprint {
        /// Task root that could not be fingerprinted.
        path: PathBuf,
        /// Filesystem or package-entry failure.
        #[source]
        source: std::io::Error,
    },

    /// The task package changed after it was loaded.
    #[error(
        "task package changed after load at {path}: expected digest {expected}, found {actual}"
    )]
    ContentChanged {
        /// Canonical task root.
        path: PathBuf,
        /// Digest captured when the task was loaded.
        expected: String,
        /// Digest observed immediately before use.
        actual: String,
    },
}

impl Task {
    /// Loads the Terminal-Bench 2.1 task rooted at `directory`.
    ///
    /// # Errors
    ///
    /// Returns [`TaskLoadError`] when the directory cannot be resolved, a
    /// required task file is absent or unreadable, the TOML is malformed, or
    /// the declared Terminal-Bench 2.1 fields are invalid.
    pub fn load(directory: impl AsRef<Path>) -> Result<Self, TaskLoadError> {
        let requested = directory.as_ref();
        let root =
            fs::canonicalize(requested).map_err(|source| TaskLoadError::ResolveDirectory {
                path: requested.to_path_buf(),
                source,
            })?;
        if !root.is_dir() {
            return Err(TaskLoadError::Invalid {
                path: root,
                message: "task root is not a directory".to_owned(),
            });
        }

        let package = TaskPackage::load(&root).map_err(|source| TaskLoadError::Fingerprint {
            path: root.clone(),
            source,
        })?;
        let config_path = root.join(TASK_CONFIG);
        let config_text = read_package_file(&package, &root, TASK_CONFIG)?;
        let raw: RawTask = toml::from_str(&config_text).map_err(|source| TaskLoadError::Parse {
            path: config_path.clone(),
            source,
        })?;
        if raw.schema_version != "1.1" {
            return Err(TaskLoadError::UnsupportedSchema {
                found: raw.schema_version,
            });
        }

        let instruction_path = root.join(TASK_INSTRUCTION);
        let prompt = strip_leading_canary(&read_package_file(&package, &root, TASK_INSTRUCTION)?);
        if prompt.trim().is_empty() {
            return Err(TaskLoadError::Invalid {
                path: instruction_path,
                message: "instruction is empty".to_owned(),
            });
        }

        let verifier_script = root.join(VERIFIER_SCRIPT);
        require_file(&verifier_script)?;
        let environment_directory = root.join(TASK_ENVIRONMENT);
        if !package.contains_directory(Path::new(TASK_ENVIRONMENT)) {
            return Err(TaskLoadError::MissingFile {
                path: environment_directory,
            });
        }

        let name = required_string(&config_path, "task.name", raw.task.name)?;
        let image = raw
            .environment
            .docker_image
            .unwrap_or_else(|| "local-dockerfile".to_owned());
        let content_digest = package.digest().to_owned().into_boxed_str();
        let task = Self {
            root,
            content_digest,
            package: Arc::new(package),
            name: name.into_boxed_str(),
            description: raw.task.description.into_boxed_str(),
            prompt: prompt.into_boxed_str(),
            image: OciImage {
                reference: image.into_boxed_str(),
            },
            agent_timeout: duration(&config_path, "agent.timeout_sec", raw.agent.timeout_sec)?,
            verifier: Verifier {
                script: verifier_script,
                timeout: duration(
                    &config_path,
                    "verifier.timeout_sec",
                    raw.verifier.timeout_sec,
                )?,
                environment: raw.verifier.env,
                environment_mode: raw.verifier.environment_mode,
                collect: raw.verifier.collect,
            },
            artifacts: raw.artifacts,
            resources: Resources {
                cpus: positive(&config_path, "environment.cpus", raw.environment.cpus)?,
                memory_mb: positive(
                    &config_path,
                    "environment.memory_mb",
                    raw.environment.memory_mb,
                )?,
                storage_mb: positive(
                    &config_path,
                    "environment.storage_mb",
                    raw.environment.storage_mb,
                )?,
                gpus: raw.environment.gpus,
            },
            network: if raw.environment.allow_internet {
                NetworkPolicy::Public
            } else {
                NetworkPolicy::Disabled
            },
            environment: raw.environment.env,
            requires_compose: raw.metadata.custom_docker_compose
                || raw.environment.custom_docker_compose,
        };
        task.validate_package()?;
        Ok(task)
    }

    /// Returns the canonical task root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn content_digest(&self) -> &str {
        &self.content_digest
    }

    /// Re-fingerprints every packaged execution input and rejects mutation
    /// since this task was loaded.
    ///
    /// # Errors
    ///
    /// Returns [`TaskLoadError::Fingerprint`] when the package cannot be read,
    /// or [`TaskLoadError::ContentChanged`] when its canonical digest changed.
    pub fn validate_package(&self) -> Result<(), TaskLoadError> {
        let started = Instant::now();
        let package = self.current_package();
        let duration_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        match &package {
            Ok(package) => tracing::info!(
                target: "nanocodex_eval",
                task_name = self.name(),
                task_package_entries = package.entry_count(),
                task_package_file_bytes = package.file_bytes(),
                duration_ns,
                status = "unchanged",
                "validated task package identity"
            ),
            Err(error) => tracing::info!(
                target: "nanocodex_eval",
                task_name = self.name(),
                duration_ns,
                status = "changed",
                error = %error,
                "task package validation failed"
            ),
        }
        package.map(drop)
    }

    /// Materializes the environment tree captured when this task was loaded.
    ///
    /// # Errors
    ///
    /// Returns [`TaskLoadError`] when a captured file changed, materialization
    /// failed, or the package mutated during the copy.
    #[doc(hidden)]
    pub fn materialize_environment(&self, destination: &Path) -> Result<(), TaskLoadError> {
        self.materialize_package_directory(Path::new(TASK_ENVIRONMENT), destination)
    }

    /// Materializes the verifier tree captured when this task was loaded.
    ///
    /// Files, directories, and Unix modes are reproduced from the same
    /// manifest used for task identity. The source package is revalidated
    /// after materialization.
    ///
    /// # Errors
    ///
    /// Returns [`TaskLoadError`] when a captured file changed, materialization
    /// failed, or the package mutated during the copy.
    #[doc(hidden)]
    pub fn materialize_verifier_files(&self, destination: &Path) -> Result<(), TaskLoadError> {
        self.materialize_package_directory(Path::new("tests"), destination)
    }

    /// Reads the verifier script captured when this task was loaded.
    ///
    /// # Errors
    ///
    /// Returns [`TaskLoadError`] when the captured script changed, became
    /// unreadable, or the package mutated during the read.
    #[doc(hidden)]
    pub fn verifier_script_bytes(&self) -> Result<Vec<u8>, TaskLoadError> {
        let script = self
            .package
            .read_file(Path::new(VERIFIER_SCRIPT))
            .map_err(|source| TaskLoadError::Read {
                path: self.verifier.script.clone(),
                source,
            })?
            .ok_or_else(|| TaskLoadError::MissingFile {
                path: self.verifier.script.clone(),
            })?;
        self.validate_package()?;
        Ok(script)
    }

    /// Returns the stable task name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the human-readable task description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the complete instruction presented to the agent.
    #[must_use]
    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    /// Files copied into the disposable native workspace before an attempt.
    #[must_use]
    pub fn environment_directory(&self) -> PathBuf {
        self.root.join(TASK_ENVIRONMENT)
    }

    /// Returns the task's declared OCI image.
    #[must_use]
    pub const fn image(&self) -> &OciImage {
        &self.image
    }

    /// Returns the maximum agent execution duration.
    #[must_use]
    pub const fn agent_timeout(&self) -> Duration {
        self.agent_timeout
    }

    /// Returns the verifier recipe.
    #[must_use]
    pub const fn verifier(&self) -> &Verifier {
        &self.verifier
    }

    /// Returns task-relative artifact paths requested after verification.
    #[must_use]
    pub fn artifacts(&self) -> &[PathBuf] {
        &self.artifacts
    }

    /// Returns declared resource requirements.
    #[must_use]
    pub const fn resources(&self) -> &Resources {
        &self.resources
    }

    /// Returns the task's network policy.
    #[must_use]
    pub const fn network(&self) -> NetworkPolicy {
        self.network
    }

    /// Returns environment variables supplied to the task process.
    #[must_use]
    pub const fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    /// Returns whether the task requires a custom Docker Compose topology.
    #[must_use]
    pub const fn requires_compose(&self) -> bool {
        self.requires_compose
    }

    fn current_package(&self) -> Result<TaskPackage, TaskLoadError> {
        let package =
            TaskPackage::load(&self.root).map_err(|source| TaskLoadError::Fingerprint {
                path: self.root.clone(),
                source,
            })?;
        if package.digest() != self.content_digest() {
            return Err(TaskLoadError::ContentChanged {
                path: self.root.clone(),
                expected: self.content_digest().to_owned(),
                actual: package.digest().to_owned(),
            });
        }
        Ok(package)
    }

    fn materialize_package_directory(
        &self,
        package_directory: &Path,
        destination: &Path,
    ) -> Result<(), TaskLoadError> {
        self.package
            .materialize_directory(package_directory, destination)
            .map_err(|source| TaskLoadError::Fingerprint {
                path: self.root.clone(),
                source,
            })?;
        self.validate_package()
    }
}

impl OciImage {
    /// Returns the manifest's image reference.
    #[must_use]
    pub fn reference(&self) -> &str {
        &self.reference
    }
}

impl NetworkPolicy {
    /// Returns the stable artifact and telemetry spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Disabled => "no-network",
        }
    }
}

impl Verifier {
    /// Returns the canonical verifier script path.
    #[must_use]
    pub fn script(&self) -> &Path {
        &self.script
    }

    /// Returns the verifier execution deadline.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns environment variables supplied to the verifier.
    #[must_use]
    pub const fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    /// Returns where verification executes.
    #[must_use]
    pub const fn environment_mode(&self) -> VerifierEnvironmentMode {
        self.environment_mode
    }

    /// Returns post-verifier collection commands.
    #[must_use]
    pub fn collect(&self) -> &[VerifierCollect] {
        &self.collect
    }
}

impl VerifierCollect {
    /// Returns the complete shell command.
    #[must_use]
    pub fn command(&self) -> &str {
        &self.command
    }
}

impl VerifierEnvironmentMode {
    /// Returns the stable manifest and artifact spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Same => "same",
            Self::Separate => "separate",
        }
    }
}

#[derive(Deserialize)]
struct RawTask {
    schema_version: String,
    #[serde(default)]
    artifacts: Vec<PathBuf>,
    task: RawTaskInfo,
    #[serde(default)]
    metadata: RawMetadata,
    agent: RawPhase,
    verifier: RawVerifier,
    environment: RawEnvironment,
}

#[derive(Default, Deserialize)]
struct RawMetadata {
    #[serde(default)]
    custom_docker_compose: bool,
}

#[derive(Deserialize)]
struct RawTaskInfo {
    name: String,
    #[serde(default)]
    description: String,
}

#[derive(Deserialize)]
struct RawPhase {
    timeout_sec: f64,
}

#[derive(Deserialize)]
struct RawVerifier {
    timeout_sec: f64,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    environment_mode: VerifierEnvironmentMode,
    #[serde(default)]
    collect: Vec<VerifierCollect>,
}

#[derive(Deserialize)]
struct RawEnvironment {
    #[serde(default)]
    docker_image: Option<String>,
    cpus: u32,
    memory_mb: u64,
    storage_mb: u64,
    #[serde(default)]
    gpus: u32,
    #[serde(default = "enabled")]
    allow_internet: bool,
    #[serde(default)]
    custom_docker_compose: bool,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

impl<'de> Deserialize<'de> for VerifierEnvironmentMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match String::deserialize(deserializer)?.as_str() {
            "same" => Ok(Self::Same),
            "separate" => Ok(Self::Separate),
            mode => Err(serde::de::Error::unknown_variant(
                mode,
                &["same", "separate"],
            )),
        }
    }
}

impl<'de> Deserialize<'de> for VerifierCollect {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawCollect {
            command: String,
        }

        let raw = RawCollect::deserialize(deserializer)?;
        if raw.command.trim().is_empty() {
            return Err(serde::de::Error::custom(
                "verifier collect command must not be empty",
            ));
        }
        Ok(Self {
            command: raw.command.into_boxed_str(),
        })
    }
}

const fn enabled() -> bool {
    true
}

fn read_package_file(
    package: &TaskPackage,
    root: &Path,
    relative: &str,
) -> Result<String, TaskLoadError> {
    let path = root.join(relative);
    let bytes = package
        .read_file(Path::new(relative))
        .map_err(|source| TaskLoadError::Read {
            path: path.clone(),
            source,
        })?
        .ok_or_else(|| TaskLoadError::MissingFile { path: path.clone() })?;
    String::from_utf8(bytes).map_err(|source| TaskLoadError::Read {
        path,
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })
}

fn require_file(path: &Path) -> Result<(), TaskLoadError> {
    if path.is_file() {
        Ok(())
    } else {
        Err(TaskLoadError::MissingFile {
            path: path.to_path_buf(),
        })
    }
}

fn required_string(path: &Path, field: &str, value: String) -> Result<String, TaskLoadError> {
    if value.trim().is_empty() {
        Err(TaskLoadError::Invalid {
            path: path.to_path_buf(),
            message: format!("{field} must not be empty"),
        })
    } else {
        Ok(value)
    }
}

fn duration(path: &Path, field: &str, seconds: f64) -> Result<Duration, TaskLoadError> {
    if seconds <= 0.0 {
        return Err(TaskLoadError::Invalid {
            path: path.to_path_buf(),
            message: format!("{field} must be greater than zero"),
        });
    }
    Duration::try_from_secs_f64(seconds).map_err(|error| TaskLoadError::Invalid {
        path: path.to_path_buf(),
        message: format!("{field} is invalid: {error}"),
    })
}

fn positive<T>(path: &Path, field: &str, value: T) -> Result<T, TaskLoadError>
where
    T: Copy + Default + PartialEq,
{
    if value == T::default() {
        Err(TaskLoadError::Invalid {
            path: path.to_path_buf(),
            message: format!("{field} must be greater than zero"),
        })
    } else {
        Ok(value)
    }
}

fn strip_leading_canary(text: &str) -> String {
    let mut lines = text.lines().peekable();
    while lines.peek().is_some_and(|line| is_canary(line)) {
        lines.next();
    }
    while lines.peek().is_some_and(|line| line.trim().is_empty()) {
        lines.next();
    }
    lines.collect::<Vec<_>>().join("\n")
}

fn is_canary(line: &str) -> bool {
    let line = line.trim();
    let comment = line.starts_with('#') || (line.starts_with("<!--") && line.ends_with("-->"));
    comment && line.to_ascii_lowercase().contains("canary")
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use tempfile::tempdir;

    use super::{NetworkPolicy, Task, VerifierEnvironmentMode};

    #[test]
    fn loads_terminal_bench_2_1_task_directory() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("tests")).unwrap();
        fs::create_dir(directory.path().join("environment")).unwrap();
        fs::write(
            directory.path().join("task.toml"),
            r#"
schema_version = "1.1"

[task]
name = "terminal-bench/example"
description = "Example task"

[metadata]
custom_docker_compose = true

[agent]
timeout_sec = 900.0

[verifier]
timeout_sec = 600.0

[verifier.env]
ANSWER = "42"

[environment]
docker_image = "example/task:20251031"
cpus = 2
memory_mb = 4096
storage_mb = 10240
gpus = 0
allow_internet = false

[environment.env]
MODE = "test"
"#,
        )
        .unwrap();
        fs::write(
            directory.path().join("instruction.md"),
            "# terminal-bench-canary secret\n\nFix the task.\n",
        )
        .unwrap();
        fs::write(directory.path().join("tests/test.sh"), "#!/bin/sh\n").unwrap();

        let task = Task::load(directory.path()).unwrap();

        assert_eq!(task.name(), "terminal-bench/example");
        assert_eq!(task.prompt(), "Fix the task.");
        assert_eq!(task.image().reference(), "example/task:20251031");
        assert_eq!(task.resources().cpus, 2);
        assert_eq!(task.network(), NetworkPolicy::Disabled);
        assert_eq!(task.environment()["MODE"], "test");
        assert_eq!(task.verifier().environment()["ANSWER"], "42");
        assert!(task.requires_compose());
    }

    #[test]
    fn rejects_missing_verifier_script() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("environment")).unwrap();
        fs::write(
            directory.path().join("task.toml"),
            r#"
schema_version = "1.1"
[task]
name = "terminal-bench/example"
[agent]
timeout_sec = 1.0
[verifier]
timeout_sec = 1.0
[environment]
docker_image = "example/task:latest"
cpus = 1
memory_mb = 1
storage_mb = 1
"#,
        )
        .unwrap();
        fs::write(directory.path().join("instruction.md"), "Do it.").unwrap();

        let error = Task::load(directory.path()).unwrap_err();
        assert!(error.to_string().contains("tests/test.sh"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinks_in_execution_inputs() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("tests")).unwrap();
        fs::create_dir(directory.path().join("environment")).unwrap();
        fs::write(
            directory.path().join("task.toml"),
            r#"
schema_version = "1.1"
[task]
name = "terminal-bench/symlink"
[agent]
timeout_sec = 1.0
[verifier]
timeout_sec = 1.0
[environment]
docker_image = "example/task:latest"
cpus = 1
memory_mb = 1
storage_mb = 1
"#,
        )
        .unwrap();
        fs::write(directory.path().join("instruction.md"), "Do it.").unwrap();
        fs::write(directory.path().join("tests/test.sh"), "exit 0\n").unwrap();
        std::os::unix::fs::symlink("/etc/passwd", directory.path().join("environment/escape"))
            .unwrap();

        let error = Task::load(directory.path()).unwrap_err();

        assert!(matches!(error, super::TaskLoadError::Fingerprint { .. }));
        assert!(error.to_string().contains("symlinks are unsupported"));
        assert!(error.to_string().contains("/etc/passwd"));
    }

    #[test]
    fn loads_frontier_bench_task_with_a_separate_verifier() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("tests")).unwrap();
        fs::create_dir(directory.path().join("environment")).unwrap();
        fs::write(
            directory.path().join("task.toml"),
            r#"
schema_version = "1.1"
artifacts = ["/app/output.txt"]

[task]
name = "terminal-bench/frontier-example"

[agent]
timeout_sec = 900.0

[verifier]
timeout_sec = 600.0
environment_mode = "separate"

[[verifier.collect]]
command = "cp /app/output.txt /tmp/output.txt"

[environment]
cpus = 2
memory_mb = 4096
storage_mb = 10240
"#,
        )
        .unwrap();
        fs::write(directory.path().join("instruction.md"), "Fix the task.").unwrap();
        fs::write(
            directory.path().join("environment/Dockerfile"),
            "FROM scratch\n",
        )
        .unwrap();
        fs::write(directory.path().join("tests/Dockerfile"), "FROM scratch\n").unwrap();
        fs::write(directory.path().join("tests/test.sh"), "#!/bin/sh\n").unwrap();

        let task = Task::load(directory.path()).unwrap();

        assert_eq!(task.image().reference(), "local-dockerfile");
        assert_eq!(
            task.verifier().environment_mode(),
            VerifierEnvironmentMode::Separate
        );
        assert_eq!(task.artifacts(), [PathBuf::from("/app/output.txt")]);
        assert_eq!(
            task.verifier().collect()[0].command(),
            "cp /app/output.txt /tmp/output.txt"
        );
    }

    #[test]
    fn loads_the_native_suite_fixtures() {
        let tasks = ["write-greeting", "uppercase-message", "extract-todos"];
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks");

        for name in tasks {
            let task = Task::load(root.join(name)).unwrap();
            assert_eq!(task.name(), format!("nanoeval/{name}"));
            assert!(!task.prompt().is_empty());
            assert!(!task.requires_compose());
        }
    }
}
