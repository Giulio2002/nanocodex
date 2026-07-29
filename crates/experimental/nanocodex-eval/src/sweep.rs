use std::{
    fmt,
    num::NonZeroU16,
    path::{Path, PathBuf},
};

use nanocodex_agent::NanocodexBuilder;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use uuid::Uuid;

use crate::{Task, digest::PACKAGE_DIGEST_SCHEMA};

/// Stable caller-defined identity for one agent configuration in a sweep.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct AgentId(Box<str>);

/// A finite task-by-agent-by-trial evaluation sweep.
#[derive(Clone, Debug)]
pub struct Sweep {
    tasks: Vec<Task>,
    agents: Vec<SweepAgent>,
    trials: NonZeroU16,
    attempt_count: usize,
}

/// Builder for an advanced multi-agent evaluation sweep.
pub struct SweepBuilder {
    tasks: Vec<Task>,
    agents: Vec<SweepAgent>,
    trials: u16,
}

#[derive(Clone)]
struct SweepAgent {
    id: AgentId,
    nanocodex: NanocodexBuilder,
}

#[derive(Clone, Copy)]
pub(crate) struct SweepAttempt<'a> {
    task: &'a Task,
    agent: &'a SweepAgent,
    trial: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct RunManifest {
    task_digest_schema: String,
    tasks: Vec<RunTask>,
    agents: Vec<AgentId>,
    trials: NonZeroU16,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
struct RunTask {
    root: PathBuf,
    name: String,
    content_digest: String,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RunCoordinate {
    task_root: PathBuf,
    agent: AgentId,
    repetition: u16,
}

/// Failure to construct a filesystem-safe agent recipe identity.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AgentIdError {
    /// The identity was empty.
    #[error("agent identifier must not be empty")]
    Empty,

    /// The first character was not alphanumeric.
    #[error("agent identifier `{value}` must begin with an ASCII letter or digit")]
    InvalidStart {
        /// Rejected identity.
        value: String,
    },

    /// The identity contained a character unsafe for retained trial paths.
    #[error("agent identifier `{value}` contains invalid character `{character}`")]
    InvalidCharacter {
        /// Rejected identity.
        value: String,
        /// First invalid character.
        character: char,
    },
}

/// Failure to validate a finite evaluation sweep.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SweepError {
    /// No tasks were configured.
    #[error("an evaluation sweep requires at least one task")]
    NoTasks,

    /// No agent recipes were configured.
    #[error("an evaluation sweep requires at least one agent")]
    NoAgents,

    /// Trial count was zero.
    #[error("sweep trial count must be greater than zero")]
    ZeroTrials,

    /// The same canonical task root was configured twice.
    #[error("task `{0}` appears more than once in the evaluation sweep")]
    DuplicateTask(String),

    /// The same agent identity was configured twice.
    #[error("agent `{0}` appears more than once in the evaluation sweep")]
    DuplicateAgent(AgentId),

    /// The task × agent × trial product overflowed [`usize`].
    #[error("evaluation sweep contains too many attempts")]
    TooManyAttempts,
}

impl AgentId {
    /// Creates a filesystem-safe stable agent identity.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is empty or contains characters other
    /// than ASCII letters, digits, `.`, `_`, or `-`.
    pub fn new(value: impl Into<String>) -> Result<Self, AgentIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(AgentIdError::Empty);
        }
        if !value.starts_with(|character: char| character.is_ascii_alphanumeric()) {
            return Err(AgentIdError::InvalidStart { value });
        }
        if let Some(character) = value.chars().find(|character| {
            !character.is_ascii_alphanumeric() && !matches!(character, '.' | '_' | '-')
        }) {
            return Err(AgentIdError::InvalidCharacter { value, character });
        }
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the validated identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AgentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl Sweep {
    /// Starts an empty sweep builder with one trial.
    #[must_use]
    pub const fn builder() -> SweepBuilder {
        SweepBuilder {
            tasks: Vec::new(),
            agents: Vec::new(),
            trials: 1,
        }
    }

    /// Returns tasks in execution order.
    #[must_use]
    pub fn tasks(&self) -> &[Task] {
        &self.tasks
    }

    /// Returns agent recipe identities in execution order.
    #[must_use]
    pub fn agents(&self) -> impl ExactSizeIterator<Item = &AgentId> {
        self.agents.iter().map(|agent| &agent.id)
    }

    /// Returns the number of independent trials per task and agent.
    #[must_use]
    pub const fn trials(&self) -> u16 {
        self.trials.get()
    }

    /// Returns the complete task × agent × trial attempt count.
    #[must_use]
    pub const fn attempt_count(&self) -> usize {
        self.attempt_count
    }

    pub(crate) fn attempts(&self) -> impl Iterator<Item = SweepAttempt<'_>> {
        let tasks = &self.tasks;
        let agents = &self.agents;
        let trials = self.trials.get();
        (1..=trials).flat_map(move |trial| {
            tasks.iter().flat_map(move |task| {
                agents
                    .iter()
                    .map(move |agent| SweepAttempt { task, agent, trial })
            })
        })
    }

    pub(crate) fn manifest(&self) -> RunManifest {
        RunManifest {
            task_digest_schema: PACKAGE_DIGEST_SCHEMA.to_owned(),
            tasks: self
                .tasks
                .iter()
                .map(|task| RunTask {
                    root: task.root().to_path_buf(),
                    name: task.name().to_owned(),
                    content_digest: task.content_digest().to_owned(),
                })
                .collect(),
            agents: self.agents.iter().map(|agent| agent.id.clone()).collect(),
            trials: self.trials,
        }
    }
}

impl RunManifest {
    pub(crate) fn attempt_count(&self) -> usize {
        self.tasks.len() * self.agents.len() * usize::from(self.trials.get())
    }

    pub(crate) fn task_roots(&self) -> impl Iterator<Item = &Path> {
        self.tasks.iter().map(|task| task.root.as_path())
    }

    pub(crate) fn contains_task_root(&self, task_root: &Path) -> bool {
        self.tasks.iter().any(|task| task.root == task_root)
    }

    pub(crate) fn task_content_digest(&self, task_root: &Path) -> Option<&str> {
        Some(
            self.tasks
                .iter()
                .find(|task| task.root == task_root)?
                .content_digest
                .as_str(),
        )
    }

    pub(crate) fn task_digest_schema(&self) -> &str {
        &self.task_digest_schema
    }

    pub(crate) fn coordinate_for_trial(
        &self,
        task_root: &Path,
        task_name: &str,
        trial_name: &str,
        attempt_id: Uuid,
    ) -> Option<RunCoordinate> {
        let task = self.tasks.iter().find(|task| task.root == task_root)?;
        if task.name != task_name {
            return None;
        }
        let short_name = task.name.rsplit('/').next().unwrap_or(&task.name);
        let compact_id = attempt_id.simple().to_string();
        for agent in &self.agents {
            for repetition in 1..=self.trials.get() {
                let prefix = format!("{short_name}__{agent}__{repetition:03}__");
                let retained_id = trial_name.strip_prefix(&prefix);
                if retained_id
                    .is_some_and(|retained| retained == compact_id || retained == &compact_id[..8])
                {
                    return Some(RunCoordinate {
                        task_root: task.root.clone(),
                        agent: agent.clone(),
                        repetition,
                    });
                }
            }
        }
        None
    }
}

impl RunCoordinate {
    pub(crate) fn task_root(&self) -> &Path {
        &self.task_root
    }

    pub(crate) const fn agent(&self) -> &AgentId {
        &self.agent
    }

    pub(crate) const fn repetition(&self) -> u16 {
        self.repetition
    }
}

impl PartialEq for RunManifest {
    fn eq(&self, other: &Self) -> bool {
        if self.task_digest_schema != other.task_digest_schema
            || self.trials != other.trials
            || self.agents != other.agents
            || self.tasks.len() != other.tasks.len()
        {
            return false;
        }

        let mut tasks = self.tasks.iter().collect::<Vec<_>>();
        let mut other_tasks = other.tasks.iter().collect::<Vec<_>>();
        tasks.sort_unstable();
        other_tasks.sort_unstable();
        tasks == other_tasks
    }
}

impl Eq for RunManifest {}

impl fmt::Debug for SweepAgent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SweepAgent")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl SweepBuilder {
    /// Replaces the ordered task collection.
    #[must_use]
    pub fn tasks(mut self, tasks: Vec<Task>) -> Self {
        self.tasks = tasks;
        self
    }

    /// Appends one task.
    #[must_use]
    pub fn task(mut self, task: Task) -> Self {
        self.tasks.push(task);
        self
    }

    /// Sets the independent trial count for every task and agent.
    #[must_use]
    pub const fn trials(mut self, trials: u16) -> Self {
        self.trials = trials;
        self
    }

    /// Adds one independently configured Nanocodex recipe.
    ///
    /// # Errors
    ///
    /// Returns an error when `id` is not a filesystem-safe stable identity.
    pub fn agent(
        mut self,
        id: impl Into<String>,
        nanocodex: NanocodexBuilder,
    ) -> Result<Self, AgentIdError> {
        self.agents.push(SweepAgent {
            id: AgentId::new(id)?,
            nanocodex: nanocodex.shared_prompt_cache(),
        });
        Ok(self)
    }

    /// Validates uniqueness and fixes deterministic task-agent-trial order.
    ///
    /// # Errors
    ///
    /// Returns an error for empty or duplicate inputs, zero trials, or an
    /// attempt count that does not fit in [`usize`].
    pub fn build(self) -> Result<Sweep, SweepError> {
        if self.tasks.is_empty() {
            return Err(SweepError::NoTasks);
        }
        if self.agents.is_empty() {
            return Err(SweepError::NoAgents);
        }
        let trials = NonZeroU16::new(self.trials).ok_or(SweepError::ZeroTrials)?;
        for (index, task) in self.tasks.iter().enumerate() {
            if self.tasks[..index]
                .iter()
                .any(|other| other.root() == task.root())
            {
                return Err(SweepError::DuplicateTask(task.root().display().to_string()));
            }
        }
        for (index, agent) in self.agents.iter().enumerate() {
            if self.agents[..index]
                .iter()
                .any(|other| other.id == agent.id)
            {
                return Err(SweepError::DuplicateAgent(agent.id.clone()));
            }
        }
        let attempt_count = self
            .tasks
            .len()
            .checked_mul(self.agents.len())
            .and_then(|count| count.checked_mul(usize::from(trials.get())))
            .ok_or(SweepError::TooManyAttempts)?;
        Ok(Sweep {
            tasks: self.tasks,
            agents: self.agents,
            trials,
            attempt_count,
        })
    }
}

impl SweepAttempt<'_> {
    pub(crate) const fn task(&self) -> &Task {
        self.task
    }

    pub(crate) const fn agent_id(&self) -> &AgentId {
        &self.agent.id
    }

    pub(crate) const fn nanocodex(&self) -> &NanocodexBuilder {
        &self.agent.nanocodex
    }

    pub(crate) const fn trial(&self) -> u16 {
        self.trial
    }

    pub(crate) fn coordinate(&self) -> RunCoordinate {
        RunCoordinate {
            task_root: self.task.root().to_path_buf(),
            agent: self.agent.id.clone(),
            repetition: self.trial,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use nanocodex_agent::{Nanocodex, OpenAi};

    use super::*;

    #[test]
    fn expands_task_agent_trial_product_in_stable_order() {
        let sweep = Sweep::builder()
            .tasks(vec![
                load_task("write-greeting"),
                load_task("uppercase-message"),
            ])
            .trials(2)
            .agent("low", Nanocodex::builder(OpenAi::new("test-key").unwrap()))
            .unwrap()
            .agent("high", Nanocodex::builder(OpenAi::new("test-key").unwrap()))
            .unwrap()
            .build()
            .unwrap();

        let expanded = sweep
            .attempts()
            .map(|attempt| {
                (
                    attempt.task().name().to_owned(),
                    attempt.agent_id().as_str().to_owned(),
                    attempt.trial(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(sweep.attempt_count(), 8);
        assert_eq!(expanded[0].1, "low");
        assert_eq!(expanded[0].2, 1);
        assert_eq!(expanded[1].1, "high");
        assert_eq!(expanded[2].0, "nanoeval/uppercase-message");
        assert_eq!(expanded[4].2, 2);
    }

    #[test]
    fn manifest_identity_ignores_task_priority_order() {
        let tasks = vec![load_task("write-greeting"), load_task("uppercase-message")];
        let first = Sweep::builder()
            .tasks(tasks.clone())
            .agent(
                "default",
                Nanocodex::builder(OpenAi::new("test-key").unwrap()),
            )
            .unwrap()
            .build()
            .unwrap();
        let second = Sweep::builder()
            .tasks(tasks.into_iter().rev().collect())
            .agent(
                "default",
                Nanocodex::builder(OpenAi::new("test-key").unwrap()),
            )
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(first.manifest(), second.manifest());
        assert_ne!(
            first
                .attempts()
                .map(|attempt| attempt.task().name().to_owned())
                .collect::<Vec<_>>(),
            second
                .attempts()
                .map(|attempt| attempt.task().name().to_owned())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn manifest_requires_the_current_task_identity_schema() {
        let current = Sweep::builder()
            .task(load_task("write-greeting"))
            .agent(
                "default",
                Nanocodex::builder(OpenAi::new("test-key").unwrap()),
            )
            .unwrap()
            .build()
            .unwrap()
            .manifest();
        let mut retained = serde_json::to_value(&current).unwrap();
        retained
            .as_object_mut()
            .unwrap()
            .remove("task_digest_schema");

        assert!(serde_json::from_value::<RunManifest>(retained).is_err());
    }

    #[test]
    fn manifest_identity_rejects_a_renamed_task_at_the_same_root() {
        let sweep = Sweep::builder()
            .task(load_task("write-greeting"))
            .agent(
                "default",
                Nanocodex::builder(OpenAi::new("test-key").unwrap()),
            )
            .unwrap()
            .build()
            .unwrap();
        let current = sweep.manifest();
        let mut retained = current.clone();
        retained.tasks[0].name = "nanoeval/renamed-task".to_owned();

        assert_ne!(retained, current);
    }

    #[test]
    fn rejects_unsafe_and_duplicate_agent_ids() {
        assert!(matches!(
            AgentId::new("mcp/on"),
            Err(AgentIdError::InvalidCharacter { character: '/', .. })
        ));
        let error = Sweep::builder()
            .task(load_task("write-greeting"))
            .agent("same", Nanocodex::builder(OpenAi::new("test-key").unwrap()))
            .unwrap()
            .agent("same", Nanocodex::builder(OpenAi::new("test-key").unwrap()))
            .unwrap()
            .build()
            .unwrap_err();
        assert_eq!(
            error,
            SweepError::DuplicateAgent(AgentId::new("same").unwrap())
        );
    }

    fn load_task(name: &str) -> Task {
        Task::load(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../tasks")
                .join(name),
        )
        .unwrap()
    }
}
