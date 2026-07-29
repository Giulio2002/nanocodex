use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use uuid::Uuid;

use crate::sweep::{RunCoordinate, RunManifest};

#[derive(Debug)]
pub(crate) struct DurableTrial {
    directory: PathBuf,
    result_path: PathBuf,
    coordinate: RunCoordinate,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DurableTrialError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("failed to decode durable trial artifact {}: {source}", path.display())]
    Decode {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "durable trial directory {} does not match retained trial name `{trial_name}`",
        directory.display()
    )]
    DirectoryName {
        directory: PathBuf,
        trial_name: String,
    },
    #[error("durable trial `{trial_name}` config names a different trial `{config_trial_name}`")]
    ConfigTrialName {
        trial_name: String,
        config_trial_name: String,
    },
    #[error(
        "durable trial `{trial_name}` task roots disagree: result={} config={}",
        result_root.display(),
        config_root.display()
    )]
    TaskRootMismatch {
        trial_name: String,
        result_root: PathBuf,
        config_root: PathBuf,
    },
    #[error("durable trial `{trial_name}` has no retained lock at {}", path.display())]
    MissingLock { trial_name: String, path: PathBuf },
    #[error(
        "durable trial `{trial_name}` lock references task root {}; result references {}",
        lock_root.display(),
        result_root.display()
    )]
    LockTaskRootMismatch {
        trial_name: String,
        result_root: PathBuf,
        lock_root: PathBuf,
    },
    #[error("durable trial `{trial_name}` task content digest is `{found}`; expected `{expected}`")]
    TaskContentDigestMismatch {
        trial_name: String,
        task_root: PathBuf,
        expected: String,
        found: String,
    },
    #[error("durable trial `{trial_name}` task digest schema is `{found}`; expected `{expected}`")]
    TaskContentDigestSchemaMismatch {
        trial_name: String,
        expected: String,
        found: String,
    },
    #[error(
        "durable trial `{trial_name}` references foreign task root {}",
        task_root.display()
    )]
    ForeignTaskRoot {
        trial_name: String,
        task_root: PathBuf,
    },
    #[error("durable trial `{trial_name}` references foreign job {found}; expected {expected}")]
    ForeignJob {
        trial_name: String,
        expected: Uuid,
        found: Uuid,
    },
    #[error(
        "durable trial `{trial_name}` references foreign job root {}; expected {}",
        found.display(),
        expected.display()
    )]
    ForeignJobRoot {
        trial_name: String,
        expected: PathBuf,
        found: PathBuf,
    },
    #[error("durable trial `{trial_name}` does not match its run manifest coordinate")]
    InvalidCoordinate { trial_name: String },
    #[error("durable trials reuse attempt UUID {0}")]
    DuplicateAttempt(Uuid),
    #[error(
        "durable trials reuse coordinate ({}, {agent}, {repetition})",
        task_root.display()
    )]
    DuplicateCoordinate {
        task_root: PathBuf,
        agent: String,
        repetition: u16,
    },
}

#[derive(Deserialize)]
struct RetainedTrialIdentity {
    id: Uuid,
    task_name: String,
    trial_name: String,
    task_id: RetainedTaskPath,
    config: RetainedTrialConfig,
}

#[derive(Deserialize)]
struct RetainedTrialConfig {
    task: RetainedTaskPath,
    trial_name: String,
    trials_dir: PathBuf,
    job_id: Uuid,
}

#[derive(Deserialize)]
struct RetainedTaskPath {
    path: PathBuf,
}

#[derive(Deserialize)]
struct RetainedTrialLock {
    task: RetainedTaskLock,
    nanocodex: RetainedNanocodexTrialLock,
}

#[derive(Deserialize)]
struct RetainedTaskLock {
    path: PathBuf,
}

#[derive(Deserialize)]
struct RetainedNanocodexTrialLock {
    materialization_digest_schema: String,
    materialization_digest: String,
}

impl DurableTrial {
    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }

    pub(crate) fn result_path(&self) -> &Path {
        &self.result_path
    }

    pub(crate) const fn coordinate(&self) -> &RunCoordinate {
        &self.coordinate
    }
}

pub(crate) fn scan_manifest_trials(
    job_root: &Path,
    job_id: Uuid,
    manifest: &RunManifest,
) -> Result<Vec<DurableTrial>, DurableTrialError> {
    let mut result_paths = Vec::new();
    for entry in fs::read_dir(job_root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let result_path = entry.path().join("result.json");
            if result_path.is_file() {
                result_paths.push(result_path);
            }
        }
    }
    result_paths.sort_unstable();

    let mut attempts = BTreeSet::new();
    let mut coordinates = BTreeSet::new();
    let mut trials = Vec::with_capacity(result_paths.len());
    for result_path in result_paths {
        let bytes = fs::read(&result_path)?;
        let result: RetainedTrialIdentity =
            serde_json::from_slice(&bytes).map_err(|source| DurableTrialError::Decode {
                path: result_path.clone(),
                source,
            })?;
        let directory = result_path.parent().map(Path::to_path_buf).ok_or_else(|| {
            DurableTrialError::DirectoryName {
                directory: result_path.clone(),
                trial_name: result.trial_name.clone(),
            }
        })?;
        if directory.file_name() != Some(result.trial_name.as_ref()) {
            return Err(DurableTrialError::DirectoryName {
                directory,
                trial_name: result.trial_name,
            });
        }
        if result.config.trial_name != result.trial_name {
            return Err(DurableTrialError::ConfigTrialName {
                trial_name: result.trial_name,
                config_trial_name: result.config.trial_name,
            });
        }
        if result.task_id.path != result.config.task.path {
            return Err(DurableTrialError::TaskRootMismatch {
                trial_name: result.trial_name,
                result_root: result.task_id.path,
                config_root: result.config.task.path,
            });
        }
        if !manifest.contains_task_root(&result.task_id.path) {
            return Err(DurableTrialError::ForeignTaskRoot {
                trial_name: result.trial_name,
                task_root: result.task_id.path,
            });
        }
        let lock_path = directory.join("lock.json");
        let lock_bytes = match fs::read(&lock_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(DurableTrialError::MissingLock {
                    trial_name: result.trial_name,
                    path: lock_path,
                });
            }
            Err(error) => return Err(error.into()),
        };
        let lock: RetainedTrialLock =
            serde_json::from_slice(&lock_bytes).map_err(|source| DurableTrialError::Decode {
                path: lock_path,
                source,
            })?;
        if lock.task.path != result.task_id.path {
            return Err(DurableTrialError::LockTaskRootMismatch {
                trial_name: result.trial_name,
                result_root: result.task_id.path,
                lock_root: lock.task.path,
            });
        }
        if let Some(expected) = manifest.task_content_digest(&result.task_id.path) {
            let expected_schema = manifest.task_digest_schema();
            let found_schema = lock.nanocodex.materialization_digest_schema.as_str();
            if found_schema != expected_schema {
                return Err(DurableTrialError::TaskContentDigestSchemaMismatch {
                    trial_name: result.trial_name,
                    expected: expected_schema.to_owned(),
                    found: found_schema.to_owned(),
                });
            }
            let expected = format!("sha256:{expected}");
            let found = lock.nanocodex.materialization_digest.as_str();
            if found != expected {
                return Err(DurableTrialError::TaskContentDigestMismatch {
                    trial_name: result.trial_name,
                    task_root: result.task_id.path,
                    expected,
                    found: found.to_owned(),
                });
            }
        }
        if result.config.job_id != job_id {
            return Err(DurableTrialError::ForeignJob {
                trial_name: result.trial_name,
                expected: job_id,
                found: result.config.job_id,
            });
        }
        if result.config.trials_dir != job_root {
            return Err(DurableTrialError::ForeignJobRoot {
                trial_name: result.trial_name,
                expected: job_root.to_path_buf(),
                found: result.config.trials_dir,
            });
        }
        let coordinate = manifest
            .coordinate_for_trial(
                &result.task_id.path,
                &result.task_name,
                &result.trial_name,
                result.id,
            )
            .ok_or_else(|| DurableTrialError::InvalidCoordinate {
                trial_name: result.trial_name.clone(),
            })?;
        if !attempts.insert(result.id) {
            return Err(DurableTrialError::DuplicateAttempt(result.id));
        }
        if !coordinates.insert(coordinate.clone()) {
            return Err(DurableTrialError::DuplicateCoordinate {
                task_root: coordinate.task_root().to_path_buf(),
                agent: coordinate.agent().as_str().to_owned(),
                repetition: coordinate.repetition(),
            });
        }
        trials.push(DurableTrial {
            directory,
            result_path,
            coordinate,
        });
    }
    Ok(trials)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use nanocodex_agent::{Nanocodex, OpenAi};
    use serde_json::{Value, json};
    use tempfile::tempdir;

    use super::*;
    use crate::{Sweep, Task};

    #[test]
    fn rejects_duplicate_attempt_ids() {
        let fixture = Fixture::new(&[("task", "suite/task")], 2);
        let id = Uuid::now_v7();
        fixture.write_trial(0, "default", 1, id);
        fixture.write_trial(0, "default", 2, id);

        let error =
            scan_manifest_trials(&fixture.job, fixture.job_id, &fixture.manifest).unwrap_err();

        assert!(matches!(error, DurableTrialError::DuplicateAttempt(found) if found == id));
    }

    #[test]
    fn rejects_duplicate_manifest_coordinates() {
        let fixture = Fixture::new(&[("task", "suite/task")], 1);
        fixture.write_trial(
            0,
            "default",
            1,
            Uuid::from_u128(0x1111_1111_0000_0000_0000_0000_0000_0001),
        );
        fixture.write_trial(
            0,
            "default",
            1,
            Uuid::from_u128(0x2222_2222_0000_0000_0000_0000_0000_0002),
        );

        let error =
            scan_manifest_trials(&fixture.job, fixture.job_id, &fixture.manifest).unwrap_err();

        assert!(matches!(
            error,
            DurableTrialError::DuplicateCoordinate { repetition: 1, .. }
        ));
    }

    #[test]
    fn rejects_foreign_task_roots() {
        let fixture = Fixture::new(
            &[("expected", "suite/task"), ("foreign", "suite/foreign")],
            1,
        );
        let id = Uuid::now_v7();
        let (trial_name, mut result) = fixture.trial_result(0, "default", 1, id, fixture.job_id);
        let foreign = fixture.tasks[1].root();
        result["task_id"]["path"] = json!(foreign);
        result["config"]["task"]["path"] = json!(foreign);
        fixture.write_result(&trial_name, &result);

        let expected_manifest = Sweep::builder()
            .task(fixture.tasks[0].clone())
            .agent(
                "default",
                Nanocodex::builder(OpenAi::new("test-key").unwrap()),
            )
            .unwrap()
            .build()
            .unwrap()
            .manifest();
        let error =
            scan_manifest_trials(&fixture.job, fixture.job_id, &expected_manifest).unwrap_err();

        assert!(matches!(error, DurableTrialError::ForeignTaskRoot { .. }));
    }

    #[test]
    fn rejects_directory_and_result_name_mismatch() {
        let fixture = Fixture::new(&[("task", "suite/task")], 1);
        let (_, result) = fixture.trial_result(0, "default", 1, Uuid::now_v7(), fixture.job_id);
        fixture.write_result("copied-trial", &result);

        let error =
            scan_manifest_trials(&fixture.job, fixture.job_id, &fixture.manifest).unwrap_err();

        assert!(matches!(error, DurableTrialError::DirectoryName { .. }));
    }

    #[test]
    fn rejects_unconfigured_agent_and_repetition_coordinates() {
        let foreign_agent = Fixture::new(&[("task", "suite/task")], 1);
        foreign_agent.write_trial(0, "foreign", 1, Uuid::now_v7());
        let error = scan_manifest_trials(
            &foreign_agent.job,
            foreign_agent.job_id,
            &foreign_agent.manifest,
        )
        .unwrap_err();
        assert!(matches!(error, DurableTrialError::InvalidCoordinate { .. }));

        let foreign_repetition = Fixture::new(&[("task", "suite/task")], 1);
        foreign_repetition.write_trial(0, "default", 2, Uuid::now_v7());
        let error = scan_manifest_trials(
            &foreign_repetition.job,
            foreign_repetition.job_id,
            &foreign_repetition.manifest,
        )
        .unwrap_err();
        assert!(matches!(error, DurableTrialError::InvalidCoordinate { .. }));
    }

    #[test]
    fn rejects_results_copied_from_another_job() {
        let fixture = Fixture::new(&[("task", "suite/task")], 1);
        let foreign_job = Uuid::now_v7();
        let (trial_name, result) =
            fixture.trial_result(0, "default", 1, Uuid::now_v7(), foreign_job);
        fixture.write_result(&trial_name, &result);

        let error =
            scan_manifest_trials(&fixture.job, fixture.job_id, &fixture.manifest).unwrap_err();

        assert!(matches!(
            error,
            DurableTrialError::ForeignJob { found, .. } if found == foreign_job
        ));
    }

    #[test]
    fn rejects_a_trial_lock_from_another_task_revision() {
        let fixture = Fixture::new(&[("task", "suite/task")], 1);
        let directory = fixture.write_trial(0, "default", 1, Uuid::now_v7());
        let lock = directory.join("lock.json");
        let mut retained: Value = serde_json::from_slice(&fs::read(&lock).unwrap()).unwrap();
        retained["nanocodex"]["materialization_digest"] = json!("sha256:stale");
        fs::write(&lock, serde_json::to_vec_pretty(&retained).unwrap()).unwrap();

        let error =
            scan_manifest_trials(&fixture.job, fixture.job_id, &fixture.manifest).unwrap_err();

        assert!(matches!(
            error,
            DurableTrialError::TaskContentDigestMismatch { found, .. }
                if found == "sha256:stale"
        ));
    }

    #[test]
    fn internal_identity_is_read_separately_from_harbors_canonical_task_hash() {
        let fixture = Fixture::new(&[("task", "suite/task")], 1);
        let directory = fixture.write_trial(0, "default", 1, Uuid::now_v7());
        let lock = directory.join("lock.json");
        let mut retained: Value = serde_json::from_slice(&fs::read(&lock).unwrap()).unwrap();
        retained["task"]["digest"] = json!("sha256:harbor-canonical-fixture");
        retained["nanocodex"] = json!({
            "materialization_digest_schema": crate::digest::PACKAGE_DIGEST_SCHEMA,
            "materialization_digest": format!(
                "sha256:{}",
                fixture.tasks[0].content_digest()
            ),
        });
        fs::write(lock, serde_json::to_vec_pretty(&retained).unwrap()).unwrap();

        let trials = scan_manifest_trials(&fixture.job, fixture.job_id, &fixture.manifest).unwrap();

        assert_eq!(trials.len(), 1);
    }

    #[test]
    fn rejects_an_internal_task_identity_without_its_schema() {
        let fixture = Fixture::new(&[("task", "suite/task")], 1);
        let directory = fixture.write_trial(0, "default", 1, Uuid::now_v7());
        let lock = directory.join("lock.json");
        let mut retained: Value = serde_json::from_slice(&fs::read(&lock).unwrap()).unwrap();
        retained["nanocodex"]
            .as_object_mut()
            .unwrap()
            .remove("materialization_digest_schema");
        fs::write(&lock, serde_json::to_vec_pretty(&retained).unwrap()).unwrap();

        let error =
            scan_manifest_trials(&fixture.job, fixture.job_id, &fixture.manifest).unwrap_err();

        assert!(matches!(error, DurableTrialError::Decode { path, .. } if path == lock));
    }

    #[test]
    fn colliding_short_names_bind_only_to_the_full_task_root() {
        let fixture = Fixture::new(&[("first", "one/shared"), ("second", "two/shared")], 1);
        fixture.write_trial(0, "default", 1, Uuid::now_v7());

        let trials = scan_manifest_trials(&fixture.job, fixture.job_id, &fixture.manifest).unwrap();

        assert_eq!(trials.len(), 1);
        assert_eq!(trials[0].coordinate().task_root(), fixture.tasks[0].root());
        assert_ne!(trials[0].coordinate().task_root(), fixture.tasks[1].root());
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        job: PathBuf,
        job_id: Uuid,
        tasks: Vec<Task>,
        manifest: RunManifest,
    }

    impl Fixture {
        fn new(tasks: &[(&str, &str)], trials: u16) -> Self {
            let directory = tempdir().unwrap();
            let tasks = tasks
                .iter()
                .map(|(directory_name, task_name)| {
                    create_task(directory.path(), directory_name, task_name)
                })
                .collect::<Vec<_>>();
            let job = directory.path().join("job");
            fs::create_dir(&job).unwrap();
            let job = fs::canonicalize(job).unwrap();
            let sweep = Sweep::builder()
                .tasks(tasks.clone())
                .trials(trials)
                .agent(
                    "default",
                    Nanocodex::builder(OpenAi::new("test-key").unwrap()),
                )
                .unwrap()
                .build()
                .unwrap();
            Self {
                _directory: directory,
                job,
                job_id: Uuid::now_v7(),
                tasks,
                manifest: sweep.manifest(),
            }
        }

        fn write_trial(&self, task: usize, agent: &str, repetition: u16, id: Uuid) -> PathBuf {
            let (trial_name, result) = self.trial_result(task, agent, repetition, id, self.job_id);
            self.write_result(&trial_name, &result)
        }

        fn trial_result(
            &self,
            task: usize,
            agent: &str,
            repetition: u16,
            id: Uuid,
            job_id: Uuid,
        ) -> (String, Value) {
            let task = &self.tasks[task];
            let short_name = task.name().rsplit('/').next().unwrap_or(task.name());
            let compact_id = id.simple().to_string();
            let trial_name = format!("{short_name}__{agent}__{repetition:03}__{compact_id}");
            let result = json!({
                "id": id,
                "task_name": task.name(),
                "trial_name": trial_name,
                "task_id": {
                    "path": task.root(),
                },
                "config": {
                    "task": {
                        "path": task.root(),
                    },
                    "trial_name": trial_name,
                    "trials_dir": self.job,
                    "job_id": job_id,
                },
            });
            (trial_name, result)
        }

        fn write_result(&self, directory_name: &str, result: &Value) -> PathBuf {
            let directory = self.job.join(directory_name);
            fs::create_dir(&directory).unwrap();
            let task_root: PathBuf =
                serde_json::from_value(result["task_id"]["path"].clone()).unwrap();
            let digest = self
                .tasks
                .iter()
                .find(|task| task.root() == task_root)
                .map_or("foreign", Task::content_digest);
            fs::write(
                directory.join("lock.json"),
                serde_json::to_vec_pretty(&json!({
                    "task": {
                        "path": task_root,
                        "digest": "sha256:harbor-canonical-fixture",
                    },
                    "nanocodex": {
                        "materialization_digest_schema": crate::digest::PACKAGE_DIGEST_SCHEMA,
                        "materialization_digest": format!("sha256:{digest}"),
                    },
                }))
                .unwrap(),
            )
            .unwrap();
            fs::write(
                directory.join("result.json"),
                serde_json::to_vec_pretty(result).unwrap(),
            )
            .unwrap();
            directory
        }
    }

    fn create_task(parent: &Path, directory_name: &str, task_name: &str) -> Task {
        let root = parent.join(directory_name);
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
name = "{task_name}"
description = "durable coordinate fixture"
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
