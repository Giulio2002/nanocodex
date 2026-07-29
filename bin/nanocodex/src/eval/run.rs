use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    fs,
    future::Future,
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;

use arcbox_ext4::Reader;
use chrono::{DateTime, Utc};
use clap::{Args, ValueEnum};
use eyre::{Result, eyre};
use nanocodex::*;
use nanocodex_eval::{harbor::*, vm::*, *};
use regex::RegexSet;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use tokio::{process::Command, sync::watch};
use tracing::{info, warn};
use yansi::Painted;

use crate::{config::EvalAgentArgs, observability::ObservabilityArgs};

const DEFAULT_OUTPUT_DIRECTORY: &str = ".nanocodex/evals";
const INVOCATION_FILE: &str = "invocation.json";
const LAST_RUN_FILE: &str = ".nanocodex/eval/last-run.json";
const INVOCATION_VERSION: u32 = 3;
const SCHEDULING_POLICY: &str = "bounded_fifo_work_conserving-v1";
const DEFAULT_TRIALS: u16 = 5;
const DEFAULT_HOST_UTILIZATION_PERCENT: u8 = 80;
const BYTES_PER_MIB: u64 = 1024 * 1024;

#[derive(Args)]
#[group(id = "task_input", required = true, multiple = true)]
pub(crate) struct Run {
    #[command(flatten)]
    observability: ObservabilityArgs,

    /// Terminal-Bench task directory. Repeat for multiple evals in one job.
    #[arg(long = "task", value_name = "DIRECTORY", group = "task_input")]
    tasks: Vec<PathBuf>,

    /// Terminal-Bench suite directory whose immediate task children should run.
    #[arg(long = "suite", value_name = "DIRECTORY", group = "task_input")]
    suites: Vec<PathBuf>,

    #[command(flatten)]
    retry: RetryArgs,

    /// Parent directory for the retained Harbor-compatible job.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Number of fresh, independent attempts per task. Defaults to k=5.
    #[arg(
        long,
        default_value_t = DEFAULT_TRIALS,
        value_parser = clap::value_parser!(u16).range(1..)
    )]
    trials: u16,

    /// Maximum number of attempts executing at once.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    concurrency: Option<u16>,

    /// Maximum sum of task-declared memory across concurrent attempts.
    #[arg(long, value_name = "MIB", value_parser = clap::value_parser!(u64).range(1..))]
    max_memory_mb: Option<u64>,

    /// Percentage of detected host CPU and memory used for omitted scheduler limits.
    #[arg(
        long,
        default_value_t = DEFAULT_HOST_UTILIZATION_PERCENT,
        value_name = "PERCENT",
        value_parser = clap::value_parser!(u8).range(1..=100)
    )]
    host_utilization: u8,

    #[command(flatten)]
    lifecycle: RunLifecycleArgs,

    /// Print typed results as JSON instead of a human summary.
    #[arg(long)]
    json: bool,

    /// Override the prepared task rootfs directory or raw ext4 image.
    #[arg(long, value_name = "PATH")]
    vm_rootfs: Option<PathBuf>,

    /// Use this prebuilt guest-runtime ELF instead of building from workspace source.
    ///
    /// `NANOCODEX_VM_GUEST_RUNTIME` provides the same pin for unattended runs.
    #[arg(long, value_name = "ELF")]
    vm_guest_runtime: Option<PathBuf>,

    /// Content-addressed VM cache shared across evaluation jobs.
    #[arg(long, value_name = "DIRECTORY", default_value = ".cache/vm")]
    vm_cache: PathBuf,

    /// Resolve the task image at the registry instead of reusing its local resolution.
    #[arg(long, conflicts_with = "vm_rootfs")]
    vm_refresh: bool,

    /// Writable VM root-disk retention policy.
    #[arg(long, value_enum)]
    vm_retention: Option<VmRetention>,

    #[command(flatten)]
    agent: EvalAgentArgs,
}

#[derive(Args)]
struct RetryArgs {
    /// Rerun unresolved tasks from the latest completed Evaluator job.
    #[arg(long, group = "task_input", conflicts_with_all = ["tasks", "suites"])]
    rerun: bool,

    /// Literal task-name substring to rerun. Repeat positional values for OR matching.
    #[arg(value_name = "NAME", requires = "rerun")]
    names: Vec<String>,

    /// Resolve the retry queue from this job instead of the latest completed job.
    #[arg(long, value_name = "JOB", requires = "rerun")]
    rerun_from: Option<PathBuf>,

    /// Advanced regular expression over full task names. Repeat for OR matching.
    #[arg(long, value_name = "REGEX", requires = "rerun")]
    match_task: Vec<String>,

    /// Print the selected task names without starting a new evaluation job.
    #[arg(long, requires = "rerun")]
    list: bool,

    #[command(flatten)]
    statuses: RetryStatusArgs,
}

#[derive(Args)]
struct RetryStatusArgs {
    /// Include typed safety refusals in the rerun selection.
    #[arg(long, requires = "rerun")]
    include_refused: bool,

    /// Include harness-errored tasks in the rerun selection.
    #[arg(long, requires = "rerun")]
    include_errored: bool,
}

#[derive(Args)]
struct RunLifecycleArgs {
    /// Start a new job even when a matching incomplete job can be resumed.
    #[arg(long = "new")]
    new_job: bool,
}

#[derive(Clone, Debug)]
struct ResolvedRun {
    task_paths: Vec<PathBuf>,
    output: PathBuf,
    trials: u16,
    concurrency: u16,
    max_memory_mb: Option<u64>,
    vm_rootfs: Option<PathBuf>,
    vm_guest_runtime: Option<PathBuf>,
    vm_cache: PathBuf,
    vm_retention: VmRetention,
    thinking: Thinking,
    web_search: bool,
    rerun_from: Option<PathBuf>,
    automatic_scheduling: Option<AutomaticScheduling>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HostResources {
    logical_cpus: usize,
    physical_memory_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SchedulingDefaults {
    concurrency: u16,
    max_memory_mb: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedScheduling {
    concurrency: u16,
    max_memory_mb: Option<u64>,
    automatic: Option<AutomaticScheduling>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AutomaticScheduling {
    utilization_percent: u8,
    host: HostResources,
    concurrency: bool,
    memory: bool,
}

struct RerunSelection {
    job: PathBuf,
    tasks: Vec<PathBuf>,
}

struct RetainedRetryQueue {
    task_names: BTreeSet<String>,
    unresolved_tasks: usize,
    lineage: Vec<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RunInvocation {
    version: u32,
    nanocodex_build: RetainedBuild,
    model: String,
    tool_profile: String,
    seed: Option<u64>,
    scheduling: RetainedScheduling,
    trials: u16,
    concurrency: u16,
    max_memory_mb: Option<u64>,
    vm_rootfs: Option<PathBuf>,
    guest_runtime: Option<RetainedGuestRuntime>,
    vm_retention: VmRetention,
    thinking: String,
    web_search: bool,
    rerun_from: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RetainedBuild {
    version: String,
    git_sha: String,
    built_at: String,
    executable_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RetainedGuestRuntime {
    target: String,
    binary_sha256: String,
    runtime_disk_digest: Option<String>,
    artifact_path: Option<PathBuf>,
    source: String,
    source_path: PathBuf,
    host_git_sha: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RetainedScheduling {
    policy: String,
    automatic_utilization_percent: Option<u8>,
    concurrency_source: String,
    memory_source: String,
}

impl RunInvocation {
    fn same_workload(&self, other: &Self) -> bool {
        self.version == other.version
            && self.nanocodex_build == other.nanocodex_build
            && self.model == other.model
            && self.tool_profile == other.tool_profile
            && self.seed == other.seed
            && self.scheduling.policy == other.scheduling.policy
            && self.trials == other.trials
            && self.vm_rootfs == other.vm_rootfs
            && same_guest_runtime(self.guest_runtime.as_ref(), other.guest_runtime.as_ref())
            && self.vm_retention == other.vm_retention
            && self.thinking == other.thinking
            && self.web_search == other.web_search
            && self.rerun_from == other.rerun_from
    }
}

fn same_guest_runtime(
    left: Option<&RetainedGuestRuntime>,
    right: Option<&RetainedGuestRuntime>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.target == right.target && left.binary_sha256 == right.binary_sha256
        }
        (None, Some(_)) | (Some(_), None) => false,
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LastRun {
    job: PathBuf,
}

#[derive(Debug, Deserialize)]
struct RetainedRun {
    tasks: Vec<RetainedRunTask>,
}

#[derive(Debug, Deserialize)]
struct RetainedRunTask {
    root: PathBuf,
}

#[derive(Debug, Deserialize)]
struct RetainedJobIdentity {
    started_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct RetainedTrialResult {
    task_name: String,
    outcome: EvalOutcome,
    scored: bool,
    verifier_result: Option<RetainedVerifierResult>,
    exception_info: Option<RetainedTrialException>,
}

#[derive(Debug, Deserialize)]
struct RetainedVerifierResult {
    rewards: BTreeMap<String, f64>,
}

#[derive(Debug, Deserialize)]
struct RetainedTrialException {
    exception_type: String,
}

impl HostResources {
    fn detect() -> Self {
        let logical_cpus = std::thread::available_parallelism().map_or(1, usize::from);
        let system = System::new_with_specifics(
            RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram()),
        );
        let physical_memory_bytes = match system.total_memory() {
            0 => None,
            bytes => Some(bytes),
        };
        Self {
            logical_cpus,
            physical_memory_bytes,
        }
    }

    fn scheduling_defaults(self, utilization_percent: u8) -> SchedulingDefaults {
        let logical_cpus = u64::try_from(self.logical_cpus).unwrap_or(u64::MAX);
        let concurrency = percentage(logical_cpus, utilization_percent).max(1);
        let concurrency = u16::try_from(concurrency).unwrap_or(u16::MAX);
        let max_memory_mb = self
            .physical_memory_bytes
            .map(|bytes| (percentage(bytes, utilization_percent) / BYTES_PER_MIB).max(1));
        SchedulingDefaults {
            concurrency,
            max_memory_mb,
        }
    }
}

const fn percentage(value: u64, percent: u8) -> u64 {
    value.saturating_mul(percent as u64) / 100
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RetainedTrialStatus {
    passed: bool,
    failed: bool,
    refused: bool,
    errored: bool,
}

impl Run {
    fn resolve_scheduling(&self, retained: Option<&RunInvocation>) -> ResolvedScheduling {
        let host = HostResources::detect();
        let defaults = host.scheduling_defaults(self.host_utilization);
        let retained_concurrency = retained.map(|invocation| invocation.concurrency);
        let automatic_concurrency = self.concurrency.is_none() && retained_concurrency.is_none();
        let concurrency = self
            .concurrency
            .or(retained_concurrency)
            .unwrap_or(defaults.concurrency);
        let (max_memory_mb, automatic_memory) = if let Some(memory) = self.max_memory_mb {
            (Some(memory), false)
        } else if let Some(invocation) = retained {
            (invocation.max_memory_mb, false)
        } else {
            (defaults.max_memory_mb, defaults.max_memory_mb.is_some())
        };
        let automatic =
            (automatic_concurrency || automatic_memory).then_some(AutomaticScheduling {
                utilization_percent: self.host_utilization,
                host,
                concurrency: automatic_concurrency,
                memory: automatic_memory,
            });
        ResolvedScheduling {
            concurrency,
            max_memory_mb,
            automatic,
        }
    }

    fn resolve_run(&self) -> Result<ResolvedRun> {
        let rerun = self
            .retry
            .rerun
            .then(|| resolve_rerun_source(self))
            .transpose()?;
        let retained_invocation = match &rerun {
            Some(rerun) => Some(load_required_invocation(&rerun.job)?),
            None => None,
        };
        let task_paths = match &rerun {
            Some(rerun) => rerun.tasks.clone(),
            None => load_task_paths(self.tasks.clone(), self.suites.clone())?,
        };
        let output = self.output.clone().unwrap_or_else(|| {
            rerun
                .as_ref()
                .and_then(|rerun| rerun.job.parent())
                .map_or_else(
                    || PathBuf::from(DEFAULT_OUTPUT_DIRECTORY),
                    Path::to_path_buf,
                )
        });
        let retained_thinking = retained_invocation
            .as_ref()
            .map(|invocation| {
                Thinking::from_str(&invocation.thinking).map_err(|error| {
                    eyre!(
                        "invalid thinking level {:?} in {INVOCATION_FILE}: {error}",
                        invocation.thinking
                    )
                })
            })
            .transpose()?;
        let thinking = self
            .agent
            .thinking()
            .or(retained_thinking)
            .unwrap_or_default();
        let web_search = self
            .agent
            .web_search()
            .or_else(|| {
                retained_invocation
                    .as_ref()
                    .map(|invocation| invocation.web_search)
            })
            .unwrap_or(false);
        let vm_rootfs = self.vm_rootfs.clone().or_else(|| {
            retained_invocation
                .as_ref()
                .and_then(|invocation| invocation.vm_rootfs.clone())
        });
        let vm_guest_runtime = self
            .vm_guest_runtime
            .clone()
            .or_else(|| std::env::var_os("NANOCODEX_VM_GUEST_RUNTIME").map(PathBuf::from));
        let scheduling = self.resolve_scheduling(retained_invocation.as_ref());
        Ok(ResolvedRun {
            task_paths,
            output,
            trials: self.trials,
            concurrency: scheduling.concurrency,
            max_memory_mb: scheduling.max_memory_mb,
            vm_rootfs,
            vm_guest_runtime,
            vm_cache: self.vm_cache.clone(),
            vm_retention: self
                .vm_retention
                .or_else(|| {
                    retained_invocation
                        .as_ref()
                        .map(|invocation| invocation.vm_retention)
                })
                .unwrap_or_default(),
            thinking,
            web_search,
            rerun_from: rerun.map(|rerun| rerun.job),
            automatic_scheduling: scheduling.automatic,
        })
    }

    fn resolve_executable_run(&self) -> Result<Option<ResolvedRun>> {
        let resolved = self.resolve_run()?;
        if self.retry.list {
            write_task_names(&resolved.task_paths, self.json)?;
            return Ok(None);
        }
        resolved.report_automatic_scheduling();
        resolved.report_configuration();
        Ok(Some(resolved))
    }

    pub(crate) async fn run(self) -> Result<()> {
        let total_started = Instant::now();
        let Some(resolved) = self.resolve_executable_run()? else {
            return Ok(());
        };
        let observability_started = Instant::now();
        let _observability = self.observability.install(false, Path::new("."))?;
        let observability = observability_started.elapsed();
        let (tasks, task_loading) =
            load_prioritized_tasks(resolved.task_paths.clone(), &resolved.output)?;
        let evaluation_setup_started = Instant::now();
        let new_job = self.lifecycle.new_job;
        let nanocodex = self.agent.builder(resolved.thinking, resolved.web_search)?;
        let (mut evaluator, sweep, attempt_count) =
            Self::build_evaluator(&resolved, tasks.clone(), nanocodex, new_job)?;
        let vm_backend = VmBackend::builder()
            .retain_passed_rootfs(resolved.vm_retention.retains_passes())
            .web_search(resolved.web_search)
            .build();
        evaluator = evaluator.vm(vm_backend.clone());
        let (eval, events) = evaluator.build()?;
        let mut evaluation_setup = evaluation_setup_started.elapsed();
        let remaining_attempts = eval.remaining_attempts(&sweep)?;
        let skipped_attempts = attempt_count.saturating_sub(remaining_attempts);
        let (vmm, runtime_image, guest_runtime, vm_runtime) = prepare_run_vm(
            resolved.vm_rootfs.as_deref(),
            resolved.vm_guest_runtime.as_deref(),
            eval.directory(),
            eval.resumed(),
            resolved.rerun_from.as_deref(),
            eval.resumed() && skipped_attempts == 0,
        )
        .await?;
        let invocation_started = Instant::now();
        persist_invocation(
            eval.directory(),
            &resolved.invocation(guest_runtime.clone())?,
        )?;
        evaluation_setup += invocation_started.elapsed();
        let vm_environments_started = Instant::now();
        let mut resources = VmResources::builder(vmm, runtime_image)
            .tasks(tasks.clone())
            .cache_directory(&resolved.vm_cache)
            .cache_policy(if self.vm_refresh {
                CachePolicy::Refresh
            } else {
                CachePolicy::Reuse
            });
        if let Some(rootfs) = &resolved.vm_rootfs {
            resources = resources.rootfs(rootfs);
        }
        resources.prepare().await?.configure(&vm_backend).await?;
        let vm_environments_duration = vm_environments_started.elapsed();
        report_resume(&eval, skipped_attempts, attempt_count);
        let harbor = Harbor::new(&eval)?.record(events.subscribe())?;
        let (expected_attempts, expected_attempts_rx) = watch::channel(remaining_attempts);
        let progress = tokio::spawn(report_progress(
            events.subscribe(),
            expected_attempts_rx,
            usize::from(resolved.concurrency),
            resolved.max_memory_mb,
        ));
        let attempts_started = Instant::now();
        let interrupts = ctrl_c_interrupt()?;
        let execution = finish_or_drain(eval.sweep(sweep), interrupts, remaining_attempts, || {
            let admitted = eval.begin_drain();
            eprintln!(
                "Interrupt received; stopped admitting new trials after {admitted} \
                     attempt(s), draining admitted work; press Ctrl-C again to abort"
            );
            admitted
        })
        .await?;
        let DrainExecution {
            result: sweep_result,
            terminal_attempts,
            interrupted,
            interrupt,
        } = execution;
        expected_attempts.send_replace(terminal_attempts);
        drop(expected_attempts);
        let attempts = attempts_started.elapsed();
        finish_or_interrupt(
            async move {
                let finished =
                    finish_evaluation(harbor, terminal_attempts, progress, sweep_result).await?;
                tokio::task::yield_now().await;
                let output_started = Instant::now();
                persist_aggregate(&finished.job, vm_environments_duration)?;
                tokio::task::yield_now().await;
                Self::write_report(
                    &finished.job,
                    finished.outcomes,
                    skipped_attempts,
                    vm_environments_duration,
                    self.json,
                )?;
                tokio::task::yield_now().await;
                let output = output_started.elapsed();
                let measurements = RunMeasurements {
                    observability,
                    task_loading,
                    vm_runtime,
                    vm_environments: vm_environments_duration,
                    evaluation_setup,
                    attempts,
                    harbor_finish: finished.harbor_finish,
                    output,
                    total: total_started.elapsed(),
                };
                measurements.persist(finished.job.directory())?;
                measurements.record(&finished.results, attempt_count, finished.failed);
                if !interrupted {
                    record_last_run(finished.job.directory())?;
                }
                finish_run(finished.run_error)?;
                if interrupted {
                    return Err(eyre!(
                        "evaluation interrupted after draining admitted attempts; rerun the same \
                         workload to resume {}",
                        finished.job.directory().display()
                    ));
                }
                Ok(())
            },
            interrupt,
        )
        .await??;
        Ok(())
    }

    fn build_evaluator(
        resolved: &ResolvedRun,
        tasks: Vec<Task>,
        nanocodex: NanocodexBuilder,
        new_job: bool,
    ) -> Result<(EvaluatorBuilder, Sweep, usize)> {
        let sweep = Sweep::builder()
            .tasks(tasks)
            .trials(resolved.trials)
            .agent("default", nanocodex.clone())?
            .build()?;
        let attempt_count = sweep.attempt_count();
        let evaluator = Evaluator::builder(nanocodex)
            .output_directory(&resolved.output)
            .max_concurrency(usize::from(resolved.concurrency));
        let evaluator = configure_memory_limit(evaluator, resolved.max_memory_mb);
        let evaluator = bind_finite_run(evaluator, &sweep, new_job);
        Ok((evaluator, sweep, attempt_count))
    }

    fn write_report(
        job: &HarborJob,
        outcomes: Vec<AttemptOutcome>,
        skipped: usize,
        cold_image_and_cache: Duration,
        json: bool,
    ) -> Result<()> {
        let report = RunReport::new(job, outcomes, skipped, cold_image_and_cache);
        if json {
            serde_json::to_writer_pretty(io::stdout().lock(), &report)?;
            println!();
        } else {
            Self::write_summary(&report);
        }
        Ok(())
    }

    fn write_summary(report: &RunReport) {
        println!(
            "\nScores: {} passed; {} failed; {} scored; {} unscored; {} total",
            Painted::new(report.summary.passed).green(),
            Painted::new(report.summary.failed).red(),
            report.summary.scored,
            report.summary.unscored,
            report.summary.total
        );
        println!(
            "Lifecycle: {} errored attempt{} (including {} safety refusal{})",
            Painted::new(report.summary.errored).red(),
            if report.summary.errored == 1 { "" } else { "s" },
            Painted::new(report.summary.refused).yellow(),
            if report.summary.refused == 1 { "" } else { "s" },
        );
        if report.summary.cleanup_failed > 0 {
            println!(
                "Cleanup health: {} attempt{} failed explicit cleanup",
                Painted::new(report.summary.cleanup_failed).red(),
                if report.summary.cleanup_failed == 1 {
                    ""
                } else {
                    "s"
                }
            );
        }
        if report.summary.billing_unknown > 0 {
            println!(
                "Billing coverage: {} attempt{} unknown and excluded from exact cost",
                Painted::new(report.summary.billing_unknown).yellow(),
                if report.summary.billing_unknown == 1 {
                    ""
                } else {
                    "s"
                }
            );
        }
        if report.summary.billing_missing > 0 {
            println!(
                "Billing coverage: {} attempt{} did not retain a billing snapshot",
                Painted::new(report.summary.billing_missing).yellow(),
                if report.summary.billing_missing == 1 {
                    ""
                } else {
                    "s"
                }
            );
        }
        println!("Harbor job: {}", report.job_directory.display());
        println!(
            "Cold image/cache preparation: {:.3}s",
            report.run_timing.cold_image_and_cache_ns as f64 / 1_000_000_000.0
        );
        match report.summary.known_estimated_cost_usd {
            Some(cost) => println!(
                "Known estimated cost: ${cost:.6} ({} of {} attempt{} priced)",
                report.summary.priced_attempts,
                report.summary.total,
                if report.summary.total == 1 { "" } else { "s" }
            ),
            None => {
                println!("Estimated cost: unavailable (provider usage was unavailable or unpriced)")
            }
        }
        if report.summary.observed_priced_attempts > report.summary.priced_attempts
            && let Some(cost) = report.summary.observed_estimated_cost_lower_bound_usd
        {
            println!(
                "Observed cost lower bound: ${cost:.6} ({} of {} attempt{} reported usage)",
                report.summary.observed_priced_attempts,
                report.summary.total,
                if report.summary.total == 1 { "" } else { "s" }
            );
        }
        if report.skipped > 0 {
            println!(
                "Resumed: {} previously completed attempt{} retained",
                report.skipped,
                if report.skipped == 1 { "" } else { "s" }
            );
        }
        if report.summary.failed + report.summary.refused + report.summary.errored > 0 {
            println!(
                "Inspect failures: nanocodex eval inspect {}",
                report.job_directory.display()
            );
        }
    }
}

fn persist_aggregate(job: &HarborJob, cold_image_and_cache: Duration) -> Result<()> {
    let invocation = load_required_invocation(job.directory())?;
    let aggregate = job
        .aggregate_dataset()?
        .with_run_identity(aggregate_run_identity(&invocation));
    write_json_atomic(
        &job.directory().join("aggregate.json"),
        &aggregate.with_run_timing(AggregateRunTiming {
            cold_image_and_cache_ns: duration_ns(cold_image_and_cache),
        }),
    )
}

fn aggregate_run_identity(invocation: &RunInvocation) -> AggregateRunIdentity {
    let vm = Some(AttemptVmIdentity {
        rootfs: invocation.vm_rootfs.clone(),
        guest_runtime_target: invocation
            .guest_runtime
            .as_ref()
            .map(|runtime| runtime.target.clone()),
        guest_runtime_sha256: invocation
            .guest_runtime
            .as_ref()
            .map(|runtime| runtime.binary_sha256.clone()),
        runtime_disk_digest: invocation
            .guest_runtime
            .as_ref()
            .and_then(|runtime| runtime.runtime_disk_digest.clone()),
    });
    AggregateRunIdentity {
        build: AttemptBuildIdentity {
            version: invocation.nanocodex_build.version.clone(),
            git_sha: Some(invocation.nanocodex_build.git_sha.clone()),
            built_at: Some(invocation.nanocodex_build.built_at.clone()),
            executable_sha256: Some(invocation.nanocodex_build.executable_sha256.clone()),
        },
        dataset_revision: None,
        model: invocation.model.clone(),
        model_tier: None,
        reasoning_effort: invocation.thinking.clone(),
        service_tier: Some("standard".to_owned()),
        tool_profile: invocation.tool_profile.clone(),
        seed: invocation.seed,
        agent_topology: "single_agent".to_owned(),
        vm,
    }
}

impl ResolvedRun {
    fn report_configuration(&self) {
        eprintln!(
            "Run config: thinking={} · trials={} · concurrency={} · environment=microVM · web_search={}",
            self.thinking, self.trials, self.concurrency, self.web_search
        );
        if let Some(runtime) = &self.vm_guest_runtime {
            eprintln!("VM guest runtime: pinned prebuilt {}", runtime.display());
        }
        eprintln!("VM cache: {}", self.vm_cache.display());
    }

    fn report_automatic_scheduling(&self) {
        let Some(automatic) = self.automatic_scheduling else {
            return;
        };
        let memory = automatic.host.physical_memory_bytes.map_or_else(
            || "unknown RAM".to_owned(),
            |bytes| format!("{} MiB RAM", bytes / BYTES_PER_MIB),
        );
        let concurrency_source = if automatic.concurrency {
            "automatic"
        } else {
            "configured"
        };
        let memory_source = if automatic.memory {
            "automatic"
        } else {
            "configured"
        };
        let max_memory = self
            .max_memory_mb
            .map_or_else(|| "unbounded".to_owned(), |memory| format!("{memory} MiB"));
        eprintln!(
            "Host scheduling: target={}%, detected={} logical CPUs/{memory}, \
             concurrency={} ({concurrency_source}), memory={max_memory} ({memory_source})",
            automatic.utilization_percent, automatic.host.logical_cpus, self.concurrency,
        );
    }

    fn invocation(&self, guest_runtime: Option<RetainedGuestRuntime>) -> Result<RunInvocation> {
        let scheduling = self.automatic_scheduling;
        let executable = std::env::current_exe()?;
        let (executable_sha256, _) = stable_file_sha256(&executable)?;
        Ok(RunInvocation {
            version: INVOCATION_VERSION,
            nanocodex_build: RetainedBuild {
                version: env!("NANOCODEX_SEMVER_VERSION").to_owned(),
                git_sha: env!("VERGEN_GIT_SHA").to_owned(),
                built_at: env!("VERGEN_BUILD_TIMESTAMP").to_owned(),
                executable_sha256,
            },
            model: nanocodex::oai::MODEL.to_owned(),
            tool_profile: "microvm_workspace".to_owned(),
            seed: None,
            scheduling: RetainedScheduling {
                policy: SCHEDULING_POLICY.to_owned(),
                automatic_utilization_percent: scheduling
                    .map(|automatic| automatic.utilization_percent),
                concurrency_source: if scheduling.is_some_and(|automatic| automatic.concurrency) {
                    "automatic"
                } else {
                    "configured"
                }
                .to_owned(),
                memory_source: if scheduling.is_some_and(|automatic| automatic.memory) {
                    "automatic"
                } else {
                    "configured"
                }
                .to_owned(),
            },
            trials: self.trials,
            concurrency: self.concurrency,
            max_memory_mb: self.max_memory_mb,
            vm_rootfs: self.vm_rootfs.clone(),
            guest_runtime,
            vm_retention: self.vm_retention,
            thinking: self.thinking.to_string(),
            web_search: self.web_search,
            rerun_from: self.rerun_from.clone(),
        })
    }
}

fn resolve_rerun_source(eval: &Run) -> Result<RerunSelection> {
    let job = match &eval.retry.rerun_from {
        Some(job) => resolve_job_path(job, eval.output.as_deref())?,
        None => latest_completed_job(eval.output.as_deref())?,
    };
    if !job.join("result.json").is_file() {
        return Err(eyre!(
            "rerun source is not a completed Evaluator job: {}",
            job.display()
        ));
    }
    let matcher = retry_matcher(&eval.retry)?;
    let queue = retained_retry_task_names(
        &job,
        eval.retry.statuses.include_refused,
        eval.retry.statuses.include_errored,
        matcher.as_ref(),
    )?;
    let tasks = retained_retry_task_roots(&queue.lineage, &queue.task_names)?;
    if tasks.is_empty() {
        let filter = if eval.retry.match_task.is_empty() && eval.retry.names.is_empty() {
            String::new()
        } else {
            format!(
                " matching names {:?} or regular expressions {:?}",
                eval.retry.names, eval.retry.match_task
            )
        };
        return Err(eyre!(
            "no unresolved tasks{filter}; inspect the queue with \
             `nanocodex eval --rerun --list`"
        ));
    }
    eprintln!(
        "{}",
        retry_selection_summary(eval, &queue, &job, tasks.len())
    );
    if !eval.retry.list && !eval.json {
        for task in &tasks {
            eprintln!("  {}", short_task_name(Task::load(task)?.name()));
        }
    }
    Ok(RerunSelection { job, tasks })
}

fn retry_matcher(retry: &RetryArgs) -> Result<Option<RegexSet>> {
    let mut patterns = retry.match_task.clone();
    patterns.extend(retry.names.iter().map(|name| regex::escape(name)));
    (!patterns.is_empty())
        .then(|| RegexSet::new(patterns))
        .transpose()
        .map_err(Into::into)
}

fn retry_selection_summary(
    eval: &Run,
    queue: &RetainedRetryQueue,
    job: &Path,
    selected: usize,
) -> String {
    let run = if queue.lineage.len() == 1 {
        "run"
    } else {
        "runs"
    };
    let job = job
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("<job>");
    if eval.retry.list {
        if selected == queue.unresolved_tasks {
            format!(
                "{} unresolved task{} across {} {run} (latest {job})",
                queue.unresolved_tasks,
                if queue.unresolved_tasks == 1 { "" } else { "s" },
                queue.lineage.len()
            )
        } else {
            format!(
                "{selected} selected of {} unresolved tasks across {} {run} (latest {job})",
                queue.unresolved_tasks,
                queue.lineage.len()
            )
        }
    } else {
        format!(
            "Retrying {selected} of {} unresolved task{} across {} {run} (latest {job})",
            queue.unresolved_tasks,
            if queue.unresolved_tasks == 1 { "" } else { "s" },
            queue.lineage.len()
        )
    }
}

fn write_task_names(tasks: &[PathBuf], json: bool) -> Result<()> {
    let names = tasks
        .iter()
        .map(|task| Task::load(task).map(|task| task.name().to_owned()))
        .collect::<Result<Vec<_>, _>>()?;
    if json {
        serde_json::to_writer_pretty(io::stdout().lock(), &names)?;
        println!();
    } else {
        for name in names {
            println!("{}", short_task_name(&name));
        }
    }
    Ok(())
}

fn short_task_name(name: &str) -> &str {
    name.rsplit_once('/').map_or(name, |(_, name)| name)
}

fn latest_completed_job(output: Option<&Path>) -> Result<PathBuf> {
    if let Some(job) = completed_job_from_last_run(output, Path::new(LAST_RUN_FILE)) {
        return Ok(job);
    }
    let current = std::env::current_dir()?;
    let mut roots = vec![output.map_or_else(|| current.clone(), Path::to_path_buf)];
    if output.is_none() {
        roots.extend(
            fs::read_dir(&current)?
                .filter_map(Result::ok)
                .filter_map(|entry| entry.file_type().ok()?.is_dir().then_some(entry.path())),
        );
    }
    let mut candidates = Vec::new();
    for root in roots {
        collect_completed_job(&root, &mut candidates);
        let Ok(entries) = fs::read_dir(&root) else {
            continue;
        };
        for entry in entries.filter_map(Result::ok) {
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                collect_completed_job(&entry.path(), &mut candidates);
            }
        }
    }
    candidates.sort_unstable_by_key(|(started_at, _)| *started_at);
    candidates.pop().map(|(_, job)| job).ok_or_else(|| {
        eyre!("no completed Evaluator job was found; run an eval or pass --rerun-from <JOB>")
    })
}

fn completed_job_from_last_run(output: Option<&Path>, last_run: &Path) -> Option<PathBuf> {
    if let Ok(retained) = read_json::<LastRun>(last_run)
        && let Ok(job) = resolve_job_path(&retained.job, output)
        && job.join("result.json").is_file()
    {
        return Some(job);
    }
    None
}

fn collect_completed_job(directory: &Path, candidates: &mut Vec<(DateTime<Utc>, PathBuf)>) {
    if !directory.join("result.json").is_file() || !directory.join("run.json").is_file() {
        return;
    }
    let Ok(identity) = read_json::<RetainedJobIdentity>(&directory.join("job.json")) else {
        return;
    };
    let Ok(directory) = fs::canonicalize(directory) else {
        return;
    };
    candidates.push((identity.started_at, directory));
}

fn resolve_job_path(job: &Path, output: Option<&Path>) -> Result<PathBuf> {
    let candidate = if job.is_dir() {
        job.to_path_buf()
    } else if job.components().count() == 1 {
        output
            .unwrap_or_else(|| Path::new(DEFAULT_OUTPUT_DIRECTORY))
            .join(job)
    } else {
        job.to_path_buf()
    };
    fs::canonicalize(&candidate).map_err(|error| {
        eyre!(
            "Evaluator job does not exist: {}: {error}",
            candidate.display()
        )
    })
}

fn retained_retry_task_names(
    job: &Path,
    include_refused: bool,
    include_errored: bool,
    matcher: Option<&RegexSet>,
) -> Result<RetainedRetryQueue> {
    let lineage = retained_retry_lineage(job)?;
    let mut statuses = BTreeMap::new();
    for job in &lineage {
        for (task_name, status) in retained_task_statuses(job)? {
            statuses.insert(task_name, status);
        }
    }
    let retryable_names = statuses
        .into_iter()
        .filter_map(|(task_name, status)| {
            // A pass at any repetition resolves the task. Otherwise, verifier
            // failures are retried by default and the opt-in lifecycle axes
            // independently select refusals and errors.
            let retryable = !status.passed
                && (status.failed
                    || (include_refused && status.refused)
                    || (include_errored && status.errored));
            retryable.then_some(task_name)
        })
        .collect::<BTreeSet<_>>();
    let unresolved_tasks = retryable_names.len();
    let task_names = retryable_names
        .into_iter()
        .filter(|task_name| matcher.is_none_or(|matcher| matcher.is_match(task_name)))
        .collect();
    Ok(RetainedRetryQueue {
        task_names,
        unresolved_tasks,
        lineage,
    })
}

fn retained_retry_task_roots(
    lineage: &[PathBuf],
    selected_names: &BTreeSet<String>,
) -> Result<Vec<PathBuf>> {
    let mut roots = BTreeMap::new();
    for job in lineage {
        let retained: RetainedRun = read_json(&job.join("run.json"))?;
        for retained_task in retained.tasks {
            let task = Task::load(&retained_task.root)?;
            if selected_names.contains(task.name()) {
                roots.insert(task.name().to_owned(), retained_task.root);
            }
        }
    }
    let missing = selected_names
        .iter()
        .filter(|name| !roots.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(eyre!(
            "retry lineage does not retain task definitions for {}",
            missing.join(", ")
        ));
    }
    Ok(roots.into_values().collect())
}

fn retained_retry_lineage(job: &Path) -> Result<Vec<PathBuf>> {
    let mut current = fs::canonicalize(job)?;
    let mut seen = BTreeSet::new();
    let mut lineage = Vec::new();
    loop {
        if !seen.insert(current.clone()) {
            return Err(eyre!(
                "retry lineage contains a cycle at {}",
                current.display()
            ));
        }
        lineage.push(current.clone());
        let Some(parent) = load_required_invocation(&current)?.rerun_from else {
            break;
        };
        current = fs::canonicalize(&parent).map_err(|error| {
            eyre!(
                "retry parent {} recorded by {} is unavailable: {error}",
                parent.display(),
                current.join(INVOCATION_FILE).display()
            )
        })?;
    }
    lineage.reverse();
    Ok(lineage)
}

fn retained_task_statuses(job: &Path) -> Result<BTreeMap<String, RetainedTrialStatus>> {
    let mut statuses: BTreeMap<String, RetainedTrialStatus> = BTreeMap::new();
    for entry in fs::read_dir(job)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let result_path = entry.path().join("result.json");
        if !result_path.is_file() {
            continue;
        }
        let result: RetainedTrialResult = read_json(&result_path)?;
        let status = result.status();
        statuses
            .entry(result.task_name)
            .and_modify(|retained| *retained = retained.merge(status))
            .or_insert(status);
    }
    Ok(statuses)
}

impl RetainedTrialResult {
    fn status(&self) -> RetainedTrialStatus {
        let scored = self.scored;
        let passed = scored
            && self
                .verifier_result
                .as_ref()
                .is_some_and(|verifier| verifier.rewards.values().all(|reward| *reward > 0.0));
        let exception_type = self
            .exception_info
            .as_ref()
            .map(|exception| exception.exception_type.as_str());
        let (refused, errored) = match exception_type {
            Some(exception) => (
                exception == "AgentSafetyRefusalError",
                exception != "CleanupError",
            ),
            None => (
                self.outcome == EvalOutcome::SafetyRefusal,
                matches!(
                    self.outcome,
                    EvalOutcome::SafetyRefusal
                        | EvalOutcome::AgentTimeout
                        | EvalOutcome::InfrastructureError
                ),
            ),
        };
        RetainedTrialStatus {
            passed,
            failed: scored && !passed,
            refused,
            errored,
        }
    }
}

impl RetainedTrialStatus {
    const fn merge(self, other: Self) -> Self {
        Self {
            passed: self.passed || other.passed,
            failed: self.failed || other.failed,
            refused: self.refused || other.refused,
            errored: self.errored || other.errored,
        }
    }
}

fn load_invocation(job: &Path) -> Result<Option<RunInvocation>> {
    let path = job.join(INVOCATION_FILE);
    match fs::read(&path) {
        Ok(contents) => {
            let invocation: RunInvocation = serde_json::from_slice(&contents)?;
            if invocation.version != INVOCATION_VERSION {
                return Err(eyre!(
                    "unsupported Evaluator invocation version {} in {}",
                    invocation.version,
                    path.display()
                ));
            }
            Ok(Some(invocation))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn load_required_invocation(job: &Path) -> Result<RunInvocation> {
    load_invocation(job)?.ok_or_else(|| {
        eyre!(
            "unsupported retained evaluation in {}; start a new job",
            job.display()
        )
    })
}

fn persist_invocation(job: &Path, invocation: &RunInvocation) -> Result<()> {
    let path = job.join(INVOCATION_FILE);
    if path.is_file() {
        let retained: RunInvocation = read_json(&path)?;
        if retained == *invocation {
            return Ok(());
        }
        if !retained.same_workload(invocation) {
            return Err(eyre!(
                "retry invocation conflicts with durable {}",
                path.display()
            ));
        }
        info!(
            target: "nanocodex_eval",
            previous_concurrency = retained.concurrency,
            concurrency = invocation.concurrency,
            previous_max_memory_mb = retained.max_memory_mb,
            max_memory_mb = invocation.max_memory_mb,
            "updated scheduling for resumed evaluation"
        );
    }
    write_json_atomic(&path, invocation)
}

fn record_last_run(job: &Path) -> Result<()> {
    let job = fs::canonicalize(job)?;
    write_json_atomic(Path::new(LAST_RUN_FILE), &LastRun { job })
}

fn read_json<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| eyre!("JSON path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, value)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .map(|_| ())
        .map_err(Into::into)
}

fn report_resume(eval: &Evaluator, skipped: usize, total: usize) {
    if eval.resumed() {
        eprintln!(
            "Resuming Harbor job {} ({skipped} of {total} attempts already durable)",
            eval.directory().display(),
        );
    }
}

fn finish_run(error: Option<nanocodex_eval::EvalError>) -> Result<()> {
    error.map_or(Ok(()), |error| Err(error.into()))
}

fn load_prioritized_tasks(
    task_paths: Vec<PathBuf>,
    output: &Path,
) -> Result<(Vec<Task>, Duration)> {
    let started_at = Instant::now();
    let mut tasks = load_tasks(task_paths, Vec::new())?;
    prioritize_tasks(&mut tasks, output)?;
    Ok((tasks, started_at.elapsed()))
}

async fn prepare_run_vm(
    rootfs: Option<&Path>,
    guest_runtime: Option<&Path>,
    job: &Path,
    resumed: bool,
    rerun_from: Option<&Path>,
    allow_uninitialized_resume: bool,
) -> Result<(PathBuf, PathBuf, Option<RetainedGuestRuntime>, Duration)> {
    let vmm = std::env::current_exe()?;
    let started_at = Instant::now();
    let origin =
        retained_guest_runtime_origin(job, resumed, rerun_from, allow_uninitialized_resume)?;
    let runtime = prepare_runtime_for_vm(rootfs, guest_runtime, job, origin.as_ref()).await?;
    Ok((vmm, runtime.disk, runtime.identity, started_at.elapsed()))
}

struct RetainedGuestRuntimeOrigin {
    job: PathBuf,
    runtime: RetainedGuestRuntime,
}

fn retained_guest_runtime_origin(
    job: &Path,
    resumed: bool,
    rerun_from: Option<&Path>,
    allow_uninitialized_resume: bool,
) -> Result<Option<RetainedGuestRuntimeOrigin>> {
    let origin = if resumed { Some(job) } else { rerun_from };
    let Some(origin) = origin else {
        return Ok(None);
    };
    let Some(invocation) = load_invocation(origin)? else {
        if resumed && allow_uninitialized_resume {
            return Ok(None);
        }
        return Err(eyre!(
            "VM evaluation provenance is missing from {}; start a new job with --new",
            origin.join(INVOCATION_FILE).display()
        ));
    };
    let runtime = invocation.guest_runtime.ok_or_else(|| {
        eyre!(
            "VM guest runtime provenance is missing from {}; start a new job with --new",
            origin.join(INVOCATION_FILE).display()
        )
    })?;
    Ok(Some(RetainedGuestRuntimeOrigin {
        job: origin.to_path_buf(),
        runtime,
    }))
}

struct FinishedEvaluation {
    job: HarborJob,
    outcomes: Vec<AttemptOutcome>,
    results: Vec<EvalResult>,
    run_error: Option<nanocodex_eval::EvalError>,
    failed: usize,
    harbor_finish: Duration,
}

struct DrainExecution<T, E> {
    result: Result<T, E>,
    terminal_attempts: usize,
    interrupted: bool,
    interrupt: InterruptListener,
}

enum InterruptListener {
    #[cfg(unix)]
    Unix(tokio::signal::unix::Signal),
    #[cfg(windows)]
    Windows(tokio::signal::windows::CtrlC),
    #[cfg(not(any(unix, windows)))]
    Fallback,
    #[cfg(test)]
    Injected(tokio::sync::mpsc::UnboundedReceiver<io::Result<()>>),
}

impl InterruptListener {
    async fn recv(&mut self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Unix(signal) => signal
                .recv()
                .await
                .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Ctrl-C listener closed")),
            #[cfg(windows)]
            Self::Windows(signal) => signal
                .recv()
                .await
                .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Ctrl-C listener closed")),
            #[cfg(not(any(unix, windows)))]
            Self::Fallback => tokio::signal::ctrl_c().await,
            #[cfg(test)]
            Self::Injected(signals) => signals.recv().await.unwrap_or_else(|| {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected Ctrl-C listener closed",
                ))
            }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum EvalInterruptError {
    #[error("failed to listen for Ctrl-C: {0}")]
    Listener(#[source] io::Error),
    #[error("second interrupt received; aborted admitted evaluation work")]
    Forced,
    #[error("interrupt received during evaluation finalization")]
    Finalization,
}

fn ctrl_c_interrupt() -> io::Result<InterruptListener> {
    #[cfg(unix)]
    {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .map(InterruptListener::Unix)
    }
    #[cfg(windows)]
    {
        tokio::signal::windows::ctrl_c().map(InterruptListener::Windows)
    }
    #[cfg(not(any(unix, windows)))]
    {
        Ok(InterruptListener::Fallback)
    }
}

async fn finish_or_drain<T, E, Work, Drain>(
    work: Work,
    mut interrupt: InterruptListener,
    terminal_attempts: usize,
    drain: Drain,
) -> Result<DrainExecution<T, E>, EvalInterruptError>
where
    Work: Future<Output = Result<T, E>>,
    Drain: FnOnce() -> usize,
{
    tokio::pin!(work);
    tokio::select! {
        biased;
        signal = interrupt.recv() => {
            signal.map_err(EvalInterruptError::Listener)?;
            let terminal_attempts = drain();
            tokio::select! {
                biased;
                signal = interrupt.recv() => {
                    signal.map_err(EvalInterruptError::Listener)?;
                    Err(EvalInterruptError::Forced)
                }
                result = &mut work => Ok(DrainExecution {
                    result,
                    terminal_attempts,
                    interrupted: true,
                    interrupt,
                })
            }
        }
        result = &mut work => Ok(DrainExecution {
            result,
            terminal_attempts,
            interrupted: false,
            interrupt,
        }),
    }
}

async fn finish_or_interrupt<T, Work>(
    work: Work,
    mut interrupt: InterruptListener,
) -> Result<T, EvalInterruptError>
where
    Work: Future<Output = T>,
{
    tokio::pin!(work);
    tokio::select! {
        biased;
        signal = interrupt.recv() => {
            signal.map_err(EvalInterruptError::Listener)?;
            Err(EvalInterruptError::Finalization)
        }
        output = &mut work => Ok(output),
    }
}

async fn finish_evaluation(
    harbor: HarborRecorder,
    remaining_attempts: usize,
    progress: tokio::task::JoinHandle<Result<Progress>>,
    sweep_result: Result<SweepResults, nanocodex_eval::EvalError>,
) -> Result<FinishedEvaluation> {
    let started_at = Instant::now();
    let job = harbor.finish_all(remaining_attempts).await?;
    let progress = progress.await??;
    let (outcomes, results, run_error, failed) = match sweep_result {
        Ok(results) => {
            let outcomes = results
                .into_outcomes()
                .into_iter()
                .map(AttemptOutcome::from_terminal)
                .collect::<Vec<_>>();
            let failed = outcomes
                .iter()
                .filter(|outcome| outcome.has_lifecycle_error())
                .count();
            let results = scored_results(&outcomes);
            (outcomes, results, None, failed)
        }
        Err(error) => {
            let results = progress.scored_results();
            (progress.outcomes, results, Some(error), progress.failed)
        }
    };
    Ok(FinishedEvaluation {
        job,
        outcomes,
        results,
        run_error,
        failed,
        harbor_finish: started_at.elapsed(),
    })
}

fn bind_finite_run(evaluator: EvaluatorBuilder, sweep: &Sweep, fresh: bool) -> EvaluatorBuilder {
    if fresh {
        evaluator.fresh_run(sweep)
    } else {
        evaluator.resume_incomplete(sweep)
    }
}

const fn configure_memory_limit(
    evaluator: EvaluatorBuilder,
    max_memory_mb: Option<u64>,
) -> EvaluatorBuilder {
    match max_memory_mb {
        Some(max_memory_mb) => evaluator.max_memory_mb(max_memory_mb),
        None => evaluator,
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum VmRetention {
    /// Retain disks only for failures, refusals, and errors.
    #[default]
    Failures,
    /// Retain disks for every attempt, including passes.
    All,
}

impl VmRetention {
    const fn retains_passes(self) -> bool {
        matches!(self, Self::All)
    }
}

fn load_tasks(paths: Vec<PathBuf>, suites: Vec<PathBuf>) -> Result<Vec<Task>> {
    load_task_paths(paths, suites)?
        .into_iter()
        .map(Task::load)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn prioritize_tasks(tasks: &mut [Task], output: &Path) -> Result<()> {
    let estimates = retained_task_durations(output)?;
    tasks.sort_by_key(|task| {
        let declared_floor = task
            .agent_timeout()
            .div_f64(4.0)
            .min(Duration::from_mins(10));
        let estimate = estimates
            .get(task.name())
            .copied()
            .unwrap_or(declared_floor);
        Reverse((estimate, task.agent_timeout(), task.verifier().timeout()))
    });
    Ok(())
}

fn retained_task_durations(output: &Path) -> Result<BTreeMap<String, Duration>> {
    let jobs = match fs::read_dir(output) {
        Ok(jobs) => jobs,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error.into()),
    };
    let mut samples = BTreeMap::<String, Vec<Duration>>::new();
    for job in jobs {
        let job = job?;
        if !job.file_type()?.is_dir() {
            continue;
        }
        for trial in fs::read_dir(job.path())? {
            let trial = trial?;
            if !trial.file_type()?.is_dir() {
                continue;
            }
            let Ok(bytes) = fs::read(trial.path().join("result.json")) else {
                continue;
            };
            let Ok(result) = serde_json::from_slice::<RetainedTrialTiming>(&bytes) else {
                continue;
            };
            if result.exception_info.as_ref().is_some_and(|exception| {
                matches!(
                    exception.exception_type.as_str(),
                    "EnvironmentError" | "VerifierError" | "NanocodexEvalError"
                )
            }) {
                continue;
            }
            let Ok(duration) = result
                .finished_at
                .signed_duration_since(result.started_at)
                .to_std()
            else {
                continue;
            };
            samples.entry(result.task_name).or_default().push(duration);
        }
    }
    Ok(samples
        .into_iter()
        .map(|(task, mut durations)| {
            durations.sort_unstable();
            let median = durations[durations.len() / 2];
            (task, median)
        })
        .collect())
}

#[derive(Deserialize)]
struct RetainedTrialTiming {
    task_name: String,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    exception_info: Option<RetainedException>,
}

#[derive(Deserialize)]
struct RetainedException {
    exception_type: String,
}

pub(crate) fn load_task_paths(
    mut paths: Vec<PathBuf>,
    suites: Vec<PathBuf>,
) -> Result<Vec<PathBuf>> {
    for suite in suites {
        let mut suite_tasks = fs::read_dir(&suite)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir() && path.join("task.toml").is_file())
            .collect::<Vec<_>>();
        suite_tasks.sort();
        if suite_tasks.is_empty() {
            return Err(eyre!(
                "suite contains no immediate task directories: {}",
                suite.display()
            ));
        }
        paths.extend(suite_tasks);
    }
    Ok(paths)
}

struct RunMeasurements {
    observability: Duration,
    task_loading: Duration,
    vm_runtime: Duration,
    vm_environments: Duration,
    evaluation_setup: Duration,
    attempts: Duration,
    harbor_finish: Duration,
    output: Duration,
    total: Duration,
}

#[derive(Default)]
struct AttemptMeasurementTotals {
    runtime_complete_attempts: usize,
    runtime_lower_bound_attempts: usize,
    runtime_missing_attempts: usize,
    usage_complete_attempts: usize,
    usage_lower_bound_attempts: usize,
    usage_missing_attempts: usize,
    exact_model_ns: u64,
    exact_warmup_ns: u64,
    exact_tool_work_ns: u64,
    exact_tool_wall_ns: u64,
    exact_response_retries: u64,
    exact_input_tokens: u64,
    exact_cached_input_tokens: u64,
    observed_model_ns: u64,
    observed_warmup_ns: u64,
    observed_tool_work_ns: u64,
    observed_tool_wall_ns: u64,
    observed_response_retries: u64,
    observed_input_tokens: u64,
    observed_cached_input_tokens: u64,
}

impl AttemptMeasurementTotals {
    fn from_results(results: &[EvalResult]) -> Self {
        let mut totals = Self::default();
        for result in results {
            let Some(agent) = result.agent.as_ref() else {
                totals.runtime_missing_attempts = totals.runtime_missing_attempts.saturating_add(1);
                totals.usage_missing_attempts = totals.usage_missing_attempts.saturating_add(1);
                continue;
            };
            let metadata = &agent.metadata;
            totals.observed_model_ns = totals
                .observed_model_ns
                .saturating_add(metadata.model_duration_ns);
            totals.observed_warmup_ns = totals
                .observed_warmup_ns
                .saturating_add(metadata.warmup_duration_ns);
            totals.observed_tool_work_ns = totals
                .observed_tool_work_ns
                .saturating_add(metadata.tool_work_duration_ns);
            totals.observed_tool_wall_ns = totals
                .observed_tool_wall_ns
                .saturating_add(metadata.tool_wall_duration_ns);
            totals.observed_response_retries = totals
                .observed_response_retries
                .saturating_add(u64::from(metadata.response_retries));
            if metadata.runtime_completeness == MeasurementCompleteness::Complete {
                totals.runtime_complete_attempts =
                    totals.runtime_complete_attempts.saturating_add(1);
                totals.exact_model_ns = totals
                    .exact_model_ns
                    .saturating_add(metadata.model_duration_ns);
                totals.exact_warmup_ns = totals
                    .exact_warmup_ns
                    .saturating_add(metadata.warmup_duration_ns);
                totals.exact_tool_work_ns = totals
                    .exact_tool_work_ns
                    .saturating_add(metadata.tool_work_duration_ns);
                totals.exact_tool_wall_ns = totals
                    .exact_tool_wall_ns
                    .saturating_add(metadata.tool_wall_duration_ns);
                totals.exact_response_retries = totals
                    .exact_response_retries
                    .saturating_add(u64::from(metadata.response_retries));
            } else {
                totals.runtime_lower_bound_attempts =
                    totals.runtime_lower_bound_attempts.saturating_add(1);
            }

            if !agent.has_observed_usage() {
                totals.usage_missing_attempts = totals.usage_missing_attempts.saturating_add(1);
                continue;
            }
            totals.observed_input_tokens = totals
                .observed_input_tokens
                .saturating_add(agent.usage.input_tokens);
            totals.observed_cached_input_tokens = totals
                .observed_cached_input_tokens
                .saturating_add(agent.usage.cached_input_tokens);
            if agent.billing_completeness == BillingCompleteness::Complete {
                totals.usage_complete_attempts = totals.usage_complete_attempts.saturating_add(1);
                totals.exact_input_tokens = totals
                    .exact_input_tokens
                    .saturating_add(agent.usage.input_tokens);
                totals.exact_cached_input_tokens = totals
                    .exact_cached_input_tokens
                    .saturating_add(agent.usage.cached_input_tokens);
            } else {
                totals.usage_lower_bound_attempts =
                    totals.usage_lower_bound_attempts.saturating_add(1);
            }
        }
        totals
    }
}

#[derive(Serialize)]
struct RetainedRunMeasurements {
    schema_version: u32,
    observability_ns: u64,
    task_loading_ns: u64,
    vm_runtime_build_ns: u64,
    cold_image_and_cache_ns: u64,
    evaluation_setup_ns: u64,
    attempts_wall_ns: u64,
    harbor_finish_ns: u64,
    output_ns: u64,
    total_wall_ns: u64,
}

impl RunMeasurements {
    fn persist(&self, job: &Path) -> Result<()> {
        write_json_atomic(
            &job.join("timing.json"),
            &RetainedRunMeasurements {
                schema_version: 1,
                observability_ns: duration_ns(self.observability),
                task_loading_ns: duration_ns(self.task_loading),
                vm_runtime_build_ns: duration_ns(self.vm_runtime),
                cold_image_and_cache_ns: duration_ns(self.vm_environments),
                evaluation_setup_ns: duration_ns(self.evaluation_setup),
                attempts_wall_ns: duration_ns(self.attempts),
                harbor_finish_ns: duration_ns(self.harbor_finish),
                output_ns: duration_ns(self.output),
                total_wall_ns: duration_ns(self.total),
            },
        )
    }

    fn record(&self, results: &[EvalResult], attempt_count: usize, errored_attempt_count: usize) {
        let measurements = AttemptMeasurementTotals::from_results(results);
        let verifier_ns = results
            .iter()
            .map(|result| {
                result
                    .timing
                    .verifier
                    .finished_at
                    .signed_duration_since(result.timing.verifier.started_at)
                    .num_nanoseconds()
                    .and_then(|duration| u64::try_from(duration).ok())
                    .unwrap_or_default()
            })
            .sum::<u64>();
        info!(
            target: "nanocodex_eval",
            duration_ns = duration_ns(self.total),
            observability_duration_ns = duration_ns(self.observability),
            task_loading_duration_ns = duration_ns(self.task_loading),
            vm_runtime_duration_ns = duration_ns(self.vm_runtime),
            vm_environments_duration_ns = duration_ns(self.vm_environments),
            evaluation_setup_duration_ns = duration_ns(self.evaluation_setup),
            attempts_wall_duration_ns = duration_ns(self.attempts),
            harbor_finish_duration_ns = duration_ns(self.harbor_finish),
            output_duration_ns = duration_ns(self.output),
            attempt_count,
            scored_attempt_count = results.len(),
            errored_attempt_count,
            runtime_complete_attempt_count = measurements.runtime_complete_attempts,
            runtime_lower_bound_attempt_count = measurements.runtime_lower_bound_attempts,
            runtime_missing_attempt_count = measurements.runtime_missing_attempts,
            usage_complete_attempt_count = measurements.usage_complete_attempts,
            usage_lower_bound_attempt_count = measurements.usage_lower_bound_attempts,
            usage_missing_attempt_count = measurements.usage_missing_attempts,
            attempts_model_duration_ns = measurements.exact_model_ns,
            attempts_warmup_duration_ns = measurements.exact_warmup_ns,
            attempts_tool_work_duration_ns = measurements.exact_tool_work_ns,
            attempts_tool_wall_duration_ns = measurements.exact_tool_wall_ns,
            attempts_observed_model_duration_lower_bound_ns = measurements.observed_model_ns,
            attempts_observed_warmup_duration_lower_bound_ns = measurements.observed_warmup_ns,
            attempts_observed_tool_work_duration_lower_bound_ns =
                measurements.observed_tool_work_ns,
            attempts_observed_tool_wall_duration_lower_bound_ns =
                measurements.observed_tool_wall_ns,
            attempts_verifier_duration_ns = verifier_ns,
            response_retries = measurements.exact_response_retries,
            observed_response_retries_lower_bound = measurements.observed_response_retries,
            input_tokens = measurements.exact_input_tokens,
            cached_input_tokens = measurements.exact_cached_input_tokens,
            observed_input_tokens_lower_bound = measurements.observed_input_tokens,
            observed_cached_input_tokens_lower_bound =
                measurements.observed_cached_input_tokens,
            "evaluation run completed"
        );
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Debug)]
struct PreparedGuestRuntime {
    disk: PathBuf,
    identity: Option<RetainedGuestRuntime>,
}

async fn prepare_runtime_for_vm(
    rootfs: Option<&Path>,
    guest_runtime: Option<&Path>,
    job: &Path,
    origin: Option<&RetainedGuestRuntimeOrigin>,
) -> Result<PreparedGuestRuntime> {
    let embedded_runtime = rootfs
        .filter(|rootfs| rootfs.is_dir())
        .map(|rootfs| rootfs.join(EMBEDDED_GUEST_TOOL_RUNTIME.trim_start_matches('/')));
    if let Some(rootfs) = rootfs.filter(|rootfs| rootfs.is_dir())
        && guest_runtime.is_some()
    {
        return Err(eyre!(
            "--vm-guest-runtime cannot override the runtime embedded in directory rootfs {}",
            rootfs.display()
        ));
    }
    let block_runtime = embedded_runtime.is_none();
    if let Some(origin) = origin {
        return prepare_retained_guest_runtime(job, origin, guest_runtime, block_runtime);
    }

    let source = match embedded_runtime {
        Some(runtime) => SourceGuestRuntime {
            path: fs::canonicalize(&runtime).map_err(|error| {
                eyre!(
                    "failed to resolve VM guest runtime embedded in {}: {error}",
                    runtime.display()
                )
            })?,
            build_status: "embedded",
            source: "embedded_rootfs",
        },
        None => resolve_vm_guest_runtime_source(guest_runtime).await?,
    };
    prepare_new_guest_runtime(job, source, block_runtime)
}

const EMBEDDED_GUEST_TOOL_RUNTIME: &str = "/usr/local/bin/nanocodex-vm-guest";
const GUEST_RUNTIME_DISK_BINARY_PATH: &str = "/nanocodex-vm-guest";
const GUEST_RUNTIME_ARTIFACT_ROOT: &str = "guest-runtime/artifacts";
const GUEST_RUNTIME_CACHE_ROOT: &str = "guest-runtime/cache";
const DEFAULT_VM_CACHE: &str = ".cache/vm";
#[cfg(target_arch = "aarch64")]
const VM_GUEST_TARGET: &str = "aarch64-unknown-linux-musl";
#[cfg(target_arch = "x86_64")]
const VM_GUEST_TARGET: &str = "x86_64-unknown-linux-musl";
#[cfg(target_arch = "aarch64")]
const VM_GUEST_ELF_MACHINE: u16 = 183;
#[cfg(target_arch = "x86_64")]
const VM_GUEST_ELF_MACHINE: u16 = 62;
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
compile_error!("Evaluator VM guests are only supported on aarch64 and x86_64 hosts");
const VM_GUEST_BUILD_RECORD_VERSION: u32 = 1;

pub(crate) async fn prepare_vm_guest_runtime() -> Result<PathBuf> {
    prepare_vm_guest_runtime_from(None, Path::new(DEFAULT_VM_CACHE)).await
}

pub(crate) async fn prepare_vm_guest_runtime_from(
    prebuilt: Option<&Path>,
    cache: &Path,
) -> Result<PathBuf> {
    let started_at = Instant::now();
    let environment_prebuilt = std::env::var_os("NANOCODEX_VM_GUEST_RUNTIME").map(PathBuf::from);
    let prebuilt = prebuilt.or(environment_prebuilt.as_deref());
    let source = resolve_vm_guest_runtime_source(prebuilt).await?;
    let (bytes, _) = stable_file_bytes(&source.path)?;
    validate_vm_guest_elf(&bytes, &source.path)?;
    let runtime_disk = GuestRuntimeDisk::prepare(&source.path, cache)?;
    record_guest_runtime_ready(
        started_at,
        source.build_status,
        source.source,
        &runtime_disk,
    );
    Ok(runtime_disk.path().to_path_buf())
}

struct SourceGuestRuntime {
    path: PathBuf,
    build_status: &'static str,
    source: &'static str,
}

async fn resolve_vm_guest_runtime_source(prebuilt: Option<&Path>) -> Result<SourceGuestRuntime> {
    if let Some(prebuilt) = prebuilt {
        let runtime = fs::canonicalize(prebuilt).map_err(|error| {
            eyre!(
                "failed to resolve prebuilt VM guest runtime {}: {error}",
                prebuilt.display()
            )
        })?;
        if !runtime.is_file() {
            return Err(eyre!(
                "prebuilt VM guest runtime is not a file: {}",
                runtime.display()
            ));
        }
        return Ok(SourceGuestRuntime {
            path: runtime,
            build_status: "prebuilt",
            source: "explicit_binary",
        });
    }

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| eyre!("nanocodex binary crate is not inside its Cargo workspace"))?;
    validate_vm_guest_source_identity(workspace).await?;
    let runtime = workspace
        .join("target")
        .join(VM_GUEST_TARGET)
        .join("debug/nanocodex-vm-guest");
    let build_status = if vm_guest_runtime_is_fresh(workspace, &runtime)? {
        "hit"
    } else {
        let previous_runtime = file_metadata_snapshot(&runtime)?;
        let exit = vm_guest_build_command(workspace).status().await?;
        if !exit.success() {
            return Err(eyre!("building the VM guest runtime failed with {exit}"));
        }
        let current_runtime = file_metadata_snapshot(&runtime)?;
        let build_status = if previous_runtime.is_some() && previous_runtime == current_runtime {
            "indexed"
        } else {
            "rebuilt"
        };
        write_vm_guest_build_record(workspace, &runtime)?;
        build_status
    };
    if !runtime.is_file() {
        return Err(eyre!(
            "Cargo completed without producing {}",
            runtime.display()
        ));
    }
    Ok(SourceGuestRuntime {
        path: fs::canonicalize(runtime)?,
        build_status,
        source: "host_commit_source",
    })
}

fn prepare_new_guest_runtime(
    job: &Path,
    source: SourceGuestRuntime,
    block_runtime: bool,
) -> Result<PreparedGuestRuntime> {
    let started_at = Instant::now();
    let (bytes, _) = stable_file_bytes(&source.path)?;
    validate_vm_guest_elf(&bytes, &source.path)?;
    let (artifact_path, artifact) = retain_guest_runtime_bytes(job, &bytes)?;
    let (disk, runtime_disk_digest, cache_status) = if block_runtime {
        let runtime_disk = prepare_job_guest_runtime_disk(job, &artifact)?;
        let cache_status = runtime_disk.status();
        (
            runtime_disk.path().to_path_buf(),
            Some(runtime_disk.digest().to_owned()),
            Some(cache_status),
        )
    } else {
        (artifact, None, None)
    };
    let binary_sha256 = hex::encode(Sha256::digest(&bytes));
    if let Some(cache_status) = cache_status {
        let runtime_disk = GuestRuntimeDiskView {
            path: &disk,
            digest: runtime_disk_digest.as_deref().unwrap_or_default(),
            status: cache_status,
        };
        record_guest_runtime_view(started_at, source.build_status, source.source, runtime_disk);
    }
    Ok(PreparedGuestRuntime {
        disk,
        identity: Some(RetainedGuestRuntime {
            target: VM_GUEST_TARGET.to_owned(),
            binary_sha256,
            runtime_disk_digest,
            artifact_path: Some(artifact_path),
            source: source.source.to_owned(),
            source_path: source.path,
            host_git_sha: env!("VERGEN_GIT_SHA").to_owned(),
        }),
    })
}

fn prepare_retained_guest_runtime(
    job: &Path,
    origin: &RetainedGuestRuntimeOrigin,
    requested: Option<&Path>,
    block_runtime: bool,
) -> Result<PreparedGuestRuntime> {
    if origin.runtime.target != VM_GUEST_TARGET {
        return Err(eyre!(
            "retained VM guest runtime targets {}, but this host requires {}",
            origin.runtime.target,
            VM_GUEST_TARGET
        ));
    }
    let bytes = retained_guest_runtime_bytes(origin, requested)?;
    validate_vm_guest_elf(&bytes, Path::new("<retained VM guest runtime>"))?;
    let binary_sha256 = hex::encode(Sha256::digest(&bytes));
    if binary_sha256 != origin.runtime.binary_sha256 {
        return Err(eyre!(
            "retained VM guest runtime bytes hash to {binary_sha256}, expected {}",
            origin.runtime.binary_sha256
        ));
    }
    let (artifact_path, artifact) = retain_guest_runtime_bytes(job, &bytes)?;
    let (disk, runtime_disk_digest) = if block_runtime {
        let expected = origin
            .runtime
            .runtime_disk_digest
            .as_deref()
            .ok_or_else(|| {
                eyre!("retained block VM guest runtime is missing its runtime disk digest")
            })?;
        let runtime_disk = prepare_job_guest_runtime_disk(job, &artifact)?;
        if runtime_disk.digest() != expected {
            return Err(eyre!(
                "retained VM guest runtime disk digest is {}, expected {expected}",
                runtime_disk.digest()
            ));
        }
        record_guest_runtime_ready(Instant::now(), "retained", "job_artifact", &runtime_disk);
        (
            runtime_disk.path().to_path_buf(),
            Some(runtime_disk.digest().to_owned()),
        )
    } else {
        if origin.runtime.runtime_disk_digest.is_some() {
            return Err(eyre!(
                "retained directory-rootfs guest runtime unexpectedly has a disk digest"
            ));
        }
        (artifact, None)
    };
    let mut identity = origin.runtime.clone();
    identity.artifact_path = Some(artifact_path);
    identity.runtime_disk_digest = runtime_disk_digest;
    Ok(PreparedGuestRuntime {
        disk,
        identity: Some(identity),
    })
}

fn retained_guest_runtime_bytes(
    origin: &RetainedGuestRuntimeOrigin,
    requested: Option<&Path>,
) -> Result<Vec<u8>> {
    let requested_bytes = if let Some(requested) = requested {
        let requested = fs::canonicalize(requested)?;
        let (bytes, _) = stable_file_bytes(&requested)?;
        validate_vm_guest_elf(&bytes, &requested)?;
        let digest = hex::encode(Sha256::digest(&bytes));
        if digest != origin.runtime.binary_sha256 {
            return Err(eyre!(
                "requested VM guest runtime {} hashes to {digest}, but the retained workload \
                 requires {}",
                requested.display(),
                origin.runtime.binary_sha256
            ));
        }
        Some(bytes)
    } else {
        None
    };
    if let Some(artifact_path) = &origin.runtime.artifact_path {
        let expected = guest_runtime_artifact_path(&origin.runtime.binary_sha256)?;
        if artifact_path != &expected {
            return Err(eyre!(
                "retained VM guest runtime artifact path {} does not match its content address {}",
                artifact_path.display(),
                expected.display()
            ));
        }
        let artifact = origin.job.join(artifact_path);
        let artifact_parent = artifact
            .parent()
            .ok_or_else(|| eyre!("VM guest runtime artifact path has no parent"))?;
        ensure_job_owned_path(&origin.job, artifact_parent)?;
        match fs::symlink_metadata(&artifact) {
            Ok(metadata) if metadata.file_type().is_file() => {
                let (bytes, _) = stable_file_bytes(&artifact)?;
                return Ok(bytes);
            }
            Ok(_) => {
                return Err(eyre!(
                    "retained VM guest runtime artifact is not a regular job-owned file: {}",
                    artifact.display()
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        return requested_bytes.map_or_else(|| recover_retained_guest_runtime_disk(origin), Ok);
    }
    if let Some(bytes) = requested_bytes {
        return Ok(bytes);
    }
    recover_retained_guest_runtime_disk(origin)
}

fn recover_retained_guest_runtime_disk(origin: &RetainedGuestRuntimeOrigin) -> Result<Vec<u8>> {
    let digest = origin
        .runtime
        .runtime_disk_digest
        .as_deref()
        .ok_or_else(|| {
            eyre!(
                "retained VM guest runtime has no immutable artifact; pass --vm-guest-runtime with \
             the exact ELF or start a new job with --new"
            )
        })?;
    validate_sha256_digest(digest, "runtime disk digest")?;
    let disks = [
        origin
            .job
            .join(GUEST_RUNTIME_CACHE_ROOT)
            .join("runtimes")
            .join(digest)
            .join("runtime.ext4"),
        Path::new(DEFAULT_VM_CACHE)
            .join("runtimes")
            .join(digest)
            .join("runtime.ext4"),
    ];
    for disk in disks {
        let Ok(mut reader) = Reader::new(&disk) else {
            continue;
        };
        if let Ok(bytes) = reader.read_file(GUEST_RUNTIME_DISK_BINARY_PATH, 0, None) {
            return Ok(bytes);
        }
    }
    Err(eyre!(
        "retained VM guest runtime artifact and runtime disk {digest} are unavailable; pass \
         --vm-guest-runtime with the exact ELF or start a new job with --new"
    ))
}

fn prepare_job_guest_runtime_disk(job: &Path, artifact: &Path) -> Result<GuestRuntimeDisk> {
    let cache = job.join(GUEST_RUNTIME_CACHE_ROOT);
    ensure_job_owned_path(job, &cache)?;
    let runtime_disk = GuestRuntimeDisk::prepare(artifact, &cache)?;
    ensure_job_owned_path(job, &cache)?;
    Ok(runtime_disk)
}

fn retain_guest_runtime_bytes(job: &Path, bytes: &[u8]) -> Result<(PathBuf, PathBuf)> {
    validate_vm_guest_elf(bytes, Path::new("<VM guest runtime artifact>"))?;
    let digest = hex::encode(Sha256::digest(bytes));
    let relative = guest_runtime_artifact_path(&digest)?;
    let artifact = job.join(&relative);
    let parent = artifact
        .parent()
        .ok_or_else(|| eyre!("VM guest runtime artifact path has no parent"))?;
    ensure_job_owned_path(job, parent)?;
    fs::create_dir_all(parent)?;
    ensure_job_owned_path(job, parent)?;
    match fs::symlink_metadata(&artifact) {
        Ok(metadata) if metadata.file_type().is_file() => {
            let (retained, _) = stable_file_bytes(&artifact)?;
            if retained != bytes {
                return Err(eyre!(
                    "content-addressed VM guest runtime artifact has conflicting bytes: {}",
                    artifact.display()
                ));
            }
            return Ok((relative, artifact));
        }
        Ok(_) => {
            return Err(eyre!(
                "content-addressed VM guest runtime artifact is not a regular file: {}",
                artifact.display()
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    #[cfg(unix)]
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o755))?;
    match temporary.persist_noclobber(&artifact) {
        Ok(file) => file.sync_all()?,
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            let (retained, _) = stable_file_bytes(&artifact)?;
            if retained != bytes {
                return Err(eyre!(
                    "content-addressed VM guest runtime artifact has conflicting bytes: {}",
                    artifact.display()
                ));
            }
        }
        Err(error) => return Err(error.error.into()),
    }
    fs::File::open(parent)?.sync_all()?;
    Ok((relative, artifact))
}

// Eval job directories are application-owned. This rejects pre-existing path and symlink escapes
// and rechecks created paths, but does not claim a capability boundary against hostile concurrent
// mutation of the job tree.
fn ensure_job_owned_path(job: &Path, path: &Path) -> Result<()> {
    let relative = path.strip_prefix(job).map_err(|_| {
        eyre!(
            "VM guest runtime path {} escapes job {}",
            path.display(),
            job.display()
        )
    })?;
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(eyre!(
            "VM guest runtime path {} escapes job {}",
            path.display(),
            job.display()
        ));
    }
    let job = fs::canonicalize(job)?;
    let mut existing = path;
    let resolved = loop {
        match fs::symlink_metadata(existing) {
            Ok(_) => break fs::canonicalize(existing)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                existing = existing.parent().ok_or_else(|| {
                    eyre!(
                        "VM guest runtime path has no existing ancestor: {}",
                        path.display()
                    )
                })?;
            }
            Err(error) => return Err(error.into()),
        }
    };
    if !resolved.starts_with(&job) {
        return Err(eyre!(
            "VM guest runtime path {} escapes job {}",
            resolved.display(),
            job.display()
        ));
    }
    Ok(())
}

fn guest_runtime_artifact_path(binary_sha256: &str) -> Result<PathBuf> {
    validate_sha256_digest(binary_sha256, "guest runtime binary digest")?;
    Ok(Path::new(GUEST_RUNTIME_ARTIFACT_ROOT)
        .join(binary_sha256)
        .join("nanocodex-vm-guest"))
}

fn validate_sha256_digest(digest: &str, label: &str) -> Result<()> {
    if digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(eyre!("{label} is not a lowercase SHA-256 digest: {digest}"))
    }
}

fn validate_vm_guest_elf(bytes: &[u8], path: &Path) -> Result<()> {
    let header = bytes.get(..20).ok_or_else(|| {
        eyre!(
            "VM guest runtime is too short to contain an ELF header: {}",
            path.display()
        )
    })?;
    if &header[..4] != b"\x7fELF" {
        return Err(eyre!(
            "VM guest runtime is not an ELF executable: {}",
            path.display()
        ));
    }
    let class = header[4];
    let byte_order = header[5];
    let machine = u16::from_le_bytes([header[18], header[19]]);
    if class != 2 || byte_order != 1 || machine != VM_GUEST_ELF_MACHINE {
        return Err(eyre!(
            "VM guest runtime {} has ELF class {class}, byte order {byte_order}, and e_machine \
             {machine}; target {VM_GUEST_TARGET} requires 64-bit little-endian e_machine \
             {VM_GUEST_ELF_MACHINE}",
            path.display()
        ));
    }
    Ok(())
}

fn stable_file_bytes(path: &Path) -> Result<(Vec<u8>, FileMetadataSnapshot)> {
    let snapshot = file_metadata_snapshot(path)?
        .ok_or_else(|| eyre!("identity input is not a regular file: {}", path.display()))?;
    let mut bytes = Vec::with_capacity(usize::try_from(snapshot.bytes).unwrap_or(0));
    fs::File::open(path)?.read_to_end(&mut bytes)?;
    if file_metadata_snapshot(path)? != Some(snapshot) {
        return Err(eyre!(
            "identity input changed while it was being read: {}",
            path.display()
        ));
    }
    Ok((bytes, snapshot))
}

fn stable_file_sha256(path: &Path) -> Result<(String, FileMetadataSnapshot)> {
    let (bytes, snapshot) = stable_file_bytes(path)?;
    let digest = hex::encode(Sha256::digest(bytes));
    Ok((digest, snapshot))
}

fn record_guest_runtime_ready(
    started_at: Instant,
    build_status: &str,
    source: &str,
    runtime_disk: &GuestRuntimeDisk,
) {
    record_guest_runtime_view(
        started_at,
        build_status,
        source,
        GuestRuntimeDiskView {
            path: runtime_disk.path(),
            digest: runtime_disk.digest(),
            status: runtime_disk.status(),
        },
    );
}

struct GuestRuntimeDiskView<'a> {
    path: &'a Path,
    digest: &'a str,
    status: GuestRuntimeDiskStatus,
}

fn record_guest_runtime_view(
    started_at: Instant,
    build_status: &str,
    source: &str,
    runtime_disk: GuestRuntimeDiskView<'_>,
) {
    let cache_status = match runtime_disk.status {
        GuestRuntimeDiskStatus::Hit => "hit",
        GuestRuntimeDiskStatus::Created => "created",
    };
    info!(
        target: "nanocodex_vm",
        duration_ns = u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX),
        vm_guest_build_status = build_status,
        vm_guest_target = VM_GUEST_TARGET,
        vm_guest_runtime_source = source,
        vm_guest_runtime_cache_status = cache_status,
        vm_guest_runtime_digest = runtime_disk.digest,
        vm_guest_runtime_disk = %runtime_disk.path.display(),
        "VM guest runtime ready"
    );
}

const VM_GUEST_SOURCE_PATHS: [&str; 10] = [
    "Cargo.toml",
    "Cargo.lock",
    ".cargo/config.toml",
    "crates/nanocodex-oai-api",
    "crates/nanocodex-tools",
    "crates/experimental/nanocodex-vm",
    "scripts/aarch64-unknown-linux-musl-linker",
    "scripts/aarch64-unknown-linux-musl-ar",
    "scripts/x86_64-unknown-linux-musl-linker",
    "scripts/x86_64-unknown-linux-musl-ar",
];

async fn validate_vm_guest_source_identity(workspace: &Path) -> Result<()> {
    let head = Command::new("git")
        .current_dir(workspace)
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .await?;
    if !head.status.success() {
        return Err(eyre!(
            "cannot bind VM guest source to host commit {}; pass \
             --vm-guest-runtime with a pinned prebuilt ELF",
            env!("VERGEN_GIT_SHA")
        ));
    }
    let head = String::from_utf8_lossy(&head.stdout).trim().to_owned();
    validate_vm_guest_commit(env!("VERGEN_GIT_SHA"), &head)?;

    let status = Command::new("git")
        .current_dir(workspace)
        .arg("status")
        .arg("--porcelain=v1")
        .arg("--untracked-files=all")
        .arg("--")
        .args(VM_GUEST_SOURCE_PATHS)
        .output()
        .await?;
    if !status.status.success() {
        return Err(eyre!(
            "cannot inspect VM guest source at {}; pass --vm-guest-runtime \
             with a pinned prebuilt ELF",
            workspace.display()
        ));
    }
    let dirty = String::from_utf8_lossy(&status.stdout);
    if !dirty.trim().is_empty() {
        return Err(eyre!(
            "refusing to build the VM guest runtime from source that differs from host commit {}: \
             {}; pass --vm-guest-runtime with a pinned prebuilt ELF",
            env!("VERGEN_GIT_SHA"),
            dirty.lines().take(8).collect::<Vec<_>>().join(", ")
        ));
    }
    Ok(())
}

fn validate_vm_guest_commit(host: &str, source: &str) -> Result<()> {
    if host == source {
        return Ok(());
    }
    Err(eyre!(
        "refusing to build the VM guest runtime from source commit {source}; \
         host binary was built from {host}. Pass --vm-guest-runtime with a pinned prebuilt ELF"
    ))
}

fn vm_guest_build_command(workspace: &Path) -> Command {
    let mut command = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command
        .current_dir(workspace)
        .arg("build")
        .arg("--quiet")
        .arg("--locked")
        .arg("--target")
        .arg(VM_GUEST_TARGET)
        .arg("--package")
        .arg("nanocodex-vm")
        .arg("--bin")
        .arg("nanocodex-vm-guest")
        .arg("--no-default-features")
        .arg("--features")
        .arg("guest-runtime");
    command
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct VmGuestBuildRecord {
    version: u32,
    target: String,
    runtime_bytes: u64,
    runtime_modified_unix_ns: u64,
    input_count: usize,
    input_metadata_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileMetadataSnapshot {
    bytes: u64,
    modified_unix_ns: u64,
}

fn vm_guest_runtime_is_fresh(workspace: &Path, runtime: &Path) -> Result<bool> {
    let path = vm_guest_build_record_path(workspace);
    let record = match fs::read(&path) {
        Ok(contents) => match serde_json::from_slice::<VmGuestBuildRecord>(&contents) {
            Ok(record) => record,
            Err(error) => {
                warn!(
                    target: "nanocodex_eval",
                    cache_record = %path.display(),
                    %error,
                    "ignoring invalid VM guest build record"
                );
                return Ok(false);
            }
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    Ok(vm_guest_build_record(workspace, runtime)?.as_ref() == Some(&record))
}

fn write_vm_guest_build_record(workspace: &Path, runtime: &Path) -> Result<()> {
    let record = vm_guest_build_record(workspace, runtime)?.ok_or_else(|| {
        eyre!(
            "Cargo completed without producing {} and its dependency record",
            runtime.display()
        )
    })?;
    write_json_atomic(&vm_guest_build_record_path(workspace), &record)
}

fn vm_guest_build_record(workspace: &Path, runtime: &Path) -> Result<Option<VmGuestBuildRecord>> {
    let Some(runtime_metadata) = file_metadata_snapshot(runtime)? else {
        return Ok(None);
    };
    let dependency_path = runtime.with_extension("d");
    let dependencies = match fs::read_to_string(&dependency_path) {
        Ok(contents) => parse_cargo_dep_info(&contents)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut inputs = dependencies;
    inputs.extend([
        workspace.join("Cargo.toml"),
        workspace.join("Cargo.lock"),
        workspace.join(".cargo/config.toml"),
        workspace.join("crates/nanocodex-oai-api/Cargo.toml"),
        workspace.join("crates/nanocodex-tools/Cargo.toml"),
        workspace.join("crates/experimental/nanocodex-vm/Cargo.toml"),
    ]);
    for script in [
        format!("scripts/{VM_GUEST_TARGET}-linker"),
        format!("scripts/{VM_GUEST_TARGET}-ar"),
    ] {
        let path = workspace.join(script);
        if path.exists() {
            inputs.push(path);
        }
    }
    inputs.sort_unstable();
    inputs.dedup();

    let mut digest = Sha256::new();
    digest.update(b"nanocodex-vm-guest-build-inputs-v1\0");
    for input in &inputs {
        let Some(metadata) = file_metadata_snapshot(input)? else {
            return Ok(None);
        };
        digest.update(input.as_os_str().as_encoded_bytes());
        digest.update([0]);
        digest.update(metadata.bytes.to_le_bytes());
        digest.update(metadata.modified_unix_ns.to_le_bytes());
    }
    Ok(Some(VmGuestBuildRecord {
        version: VM_GUEST_BUILD_RECORD_VERSION,
        target: VM_GUEST_TARGET.to_owned(),
        runtime_bytes: runtime_metadata.bytes,
        runtime_modified_unix_ns: runtime_metadata.modified_unix_ns,
        input_count: inputs.len(),
        input_metadata_digest: hex::encode(digest.finalize()),
    }))
}

fn vm_guest_build_record_path(workspace: &Path) -> PathBuf {
    workspace
        .join(DEFAULT_VM_CACHE)
        .join("runtime-build-records")
        .join(format!("{VM_GUEST_TARGET}.json"))
}

fn file_metadata_snapshot(path: &Path) -> io::Result<Option<FileMetadataSnapshot>> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() {
        return Ok(None);
    }
    let modified = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    Ok(Some(FileMetadataSnapshot {
        bytes: metadata.len(),
        modified_unix_ns: u64::try_from(modified.as_nanos()).map_err(io::Error::other)?,
    }))
}

fn parse_cargo_dep_info(contents: &str) -> io::Result<Vec<PathBuf>> {
    let (_, dependencies) = contents
        .split_once(": ")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid Cargo dep-info"))?;
    let mut paths = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for character in dependencies.chars() {
        if escaped {
            if character != '\n' && character != '\r' {
                current.push(character);
            }
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character.is_whitespace() {
            if !current.is_empty() {
                paths.push(PathBuf::from(std::mem::take(&mut current)));
            }
        } else {
            current.push(character);
        }
    }
    if escaped {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cargo dep-info ends with an escape",
        ));
    }
    if !current.is_empty() {
        paths.push(PathBuf::from(current));
    }
    if paths.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cargo dep-info contains no dependencies",
        ));
    }
    Ok(paths)
}

#[derive(Serialize)]
struct RunReport {
    job_id: uuid::Uuid,
    job_directory: PathBuf,
    skipped: usize,
    run_timing: RunReportTiming,
    summary: RunSummary,
    attempts: Vec<AttemptOutcome>,
}

impl RunReport {
    fn new(
        job: &HarborJob,
        mut attempts: Vec<AttemptOutcome>,
        skipped: usize,
        cold_image_and_cache: Duration,
    ) -> Self {
        attempts.sort_by(|left, right| left.trial_name().cmp(right.trial_name()));
        Self {
            job_id: job.id(),
            job_directory: job.directory().to_path_buf(),
            skipped,
            run_timing: RunReportTiming {
                cold_image_and_cache_ns: duration_ns(cold_image_and_cache),
            },
            summary: RunSummary::from_attempts(&attempts),
            attempts,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
struct RunReportTiming {
    cold_image_and_cache_ns: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
struct RunSummary {
    total: usize,
    scored: usize,
    unscored: usize,
    passed: usize,
    failed: usize,
    refused: usize,
    errored: usize,
    cleanup_failed: usize,
    billing_unknown: usize,
    billing_missing: usize,
    known_estimated_cost_usd: Option<f64>,
    priced_attempts: usize,
    observed_estimated_cost_lower_bound_usd: Option<f64>,
    observed_priced_attempts: usize,
}

impl RunSummary {
    fn from_attempts(attempts: &[AttemptOutcome]) -> Self {
        let mut summary = Self {
            total: attempts.len(),
            ..Self::default()
        };
        for attempt in attempts {
            match attempt {
                AttemptOutcome::Passed(result) => {
                    summary.scored += 1;
                    summary.passed += 1;
                    summary.record_exception(result.exception.as_ref().map(|error| error.kind));
                    summary.record_agent_snapshot(result.agent.as_ref(), true);
                    summary.record_cleanup(result.cleanup.is_failed());
                }
                AttemptOutcome::Failed(result) => {
                    summary.scored += 1;
                    summary.failed += 1;
                    summary.record_exception(result.exception.as_ref().map(|error| error.kind));
                    summary.record_agent_snapshot(result.agent.as_ref(), true);
                    summary.record_cleanup(result.cleanup.is_failed());
                }
                AttemptOutcome::Refused(failure) => {
                    summary.unscored += 1;
                    summary.refused += 1;
                    summary.errored += 1;
                    summary.record_agent_snapshot(
                        failure.agent.as_ref(),
                        failure.timing.agent_execution.is_some(),
                    );
                    summary.record_cleanup(failure.cleanup.is_failed());
                }
                AttemptOutcome::Errored(failure) => {
                    summary.unscored += 1;
                    summary.errored += 1;
                    summary.record_agent_snapshot(
                        failure.agent.as_ref(),
                        failure.timing.agent_execution.is_some(),
                    );
                    summary.record_cleanup(failure.cleanup.is_failed());
                }
            }
        }
        summary
    }

    fn record_agent(&mut self, agent: &nanocodex_eval::AgentResult) {
        self.record_estimated_cost(agent.cost_usd, agent.billing_completeness);
    }

    fn record_exception(&mut self, kind: Option<EvalExceptionKind>) {
        self.errored += usize::from(kind.is_some());
        self.refused += usize::from(kind == Some(EvalExceptionKind::AgentSafetyRefusal));
    }

    fn record_agent_snapshot(
        &mut self,
        agent: Option<&nanocodex_eval::AgentResult>,
        expected: bool,
    ) {
        match agent {
            Some(agent) => {
                self.record_agent(agent);
                self.billing_missing += usize::from(expected && !agent.has_observed_usage());
            }
            None if expected => self.billing_missing += 1,
            None => {}
        }
    }

    fn record_estimated_cost(
        &mut self,
        cost_usd: Option<f64>,
        billing_completeness: BillingCompleteness,
    ) {
        if let Some(cost_usd) = cost_usd {
            self.observed_estimated_cost_lower_bound_usd = Some(
                self.observed_estimated_cost_lower_bound_usd
                    .unwrap_or_default()
                    + cost_usd,
            );
            self.observed_priced_attempts += 1;
        }
        if billing_completeness == BillingCompleteness::Unknown {
            self.billing_unknown += 1;
            return;
        }
        let Some(cost_usd) = cost_usd else {
            return;
        };
        self.known_estimated_cost_usd =
            Some(self.known_estimated_cost_usd.unwrap_or_default() + cost_usd);
        self.priced_attempts += 1;
    }

    fn record_cleanup(&mut self, failed: bool) {
        self.cleanup_failed += usize::from(failed);
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "status", content = "details", rename_all = "snake_case")]
enum AttemptOutcome {
    Passed(EvalResult),
    Failed(EvalResult),
    Refused(EvalFailure),
    Errored(EvalFailure),
}

impl AttemptOutcome {
    fn from_terminal(outcome: EvalAttemptOutcome) -> Self {
        match outcome {
            EvalAttemptOutcome::Scored(result) => Self::from_result(result),
            EvalAttemptOutcome::Unscored(failure) => Self::from_failure(failure),
        }
    }

    const fn from_result(result: EvalResult) -> Self {
        match result.status {
            EvalStatus::Passed => Self::Passed(result),
            EvalStatus::Failed => Self::Failed(result),
        }
    }

    fn trial_name(&self) -> &str {
        match self {
            Self::Passed(result) | Self::Failed(result) => &result.trial_name,
            Self::Refused(failure) | Self::Errored(failure) => &failure.trial_name,
        }
    }

    fn from_failure(failure: EvalFailure) -> Self {
        if failure.exception.kind == EvalExceptionKind::AgentSafetyRefusal {
            Self::Refused(failure)
        } else {
            Self::Errored(failure)
        }
    }

    const fn has_lifecycle_error(&self) -> bool {
        match self {
            Self::Passed(result) | Self::Failed(result) => result.exception.is_some(),
            Self::Refused(_) | Self::Errored(_) => true,
        }
    }
}

struct Progress {
    outcomes: Vec<AttemptOutcome>,
    failed: usize,
}

impl Progress {
    fn scored_results(&self) -> Vec<EvalResult> {
        scored_results(&self.outcomes)
    }
}

fn scored_results(outcomes: &[AttemptOutcome]) -> Vec<EvalResult> {
    outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            AttemptOutcome::Passed(result) | AttemptOutcome::Failed(result) => Some(result.clone()),
            AttemptOutcome::Refused(_) | AttemptOutcome::Errored(_) => None,
        })
        .collect()
}

async fn report_progress(
    mut events: EvalEventStream,
    mut expected_attempts: watch::Receiver<usize>,
    concurrency: usize,
    max_memory_mb: Option<u64>,
) -> Result<Progress> {
    let mut expected = *expected_attempts.borrow_and_update();
    let count = if expected == 1 { "" } else { "s" };
    if let Some(max_memory_mb) = max_memory_mb {
        eprintln!(
            "Running {expected} evaluation{count} (up to {concurrency} concurrent, \
             {max_memory_mb} MiB task-declared memory)"
        );
    } else {
        eprintln!("Running {expected} evaluation{count} (up to {concurrency} concurrent)");
    }
    let mut completed = 0;
    let mut outcomes = Vec::with_capacity(expected);
    let mut failed = 0;
    let mut expected_updates_open = true;
    while completed < expected {
        let event = if expected_updates_open {
            tokio::select! {
                update = expected_attempts.changed() => {
                    if update.is_ok() {
                        expected = *expected_attempts.borrow_and_update();
                    } else {
                        expected_updates_open = false;
                    }
                    continue;
                }
                event = events.recv() => event?,
            }
        } else {
            events.recv().await?
        }
        .ok_or_else(|| eyre!("event stream closed after {completed} of {expected} attempts"))?;
        match &event.kind {
            EvalEventKind::Completed(result) => {
                completed += 1;
                failed += usize::from(result.exception.is_some());
                let outcome = AttemptOutcome::from_result(result.as_ref().clone());
                write_progress_line(&outcome, completed, expected);
                outcomes.push(outcome);
            }
            EvalEventKind::Failed(failure) => {
                completed += 1;
                failed += 1;
                let outcome = AttemptOutcome::from_failure(failure.as_ref().clone());
                write_progress_line(&outcome, completed, expected);
                outcomes.push(outcome);
            }
            EvalEventKind::AttemptStarted { .. }
            | EvalEventKind::Agent(_)
            | EvalEventKind::VerifierStarted
            | EvalEventKind::VerifierOutput { .. }
            | EvalEventKind::VerifierCompleted(_) => {}
        }
    }
    Ok(Progress { outcomes, failed })
}

fn write_progress_line(outcome: &AttemptOutcome, completed: usize, expected: usize) {
    match outcome {
        AttemptOutcome::Passed(result) => {
            let status = if result.exception.is_some() {
                Painted::new(format!("[PASS+ERROR {completed}/{expected}]")).yellow()
            } else {
                Painted::new(format!("[PASS {completed}/{expected}]")).green()
            };
            eprintln!(
                "{status} {} ({}){}{}",
                result.trial_name,
                result_duration(result),
                result_exception_suffix(result),
                cleanup_suffix(result.cleanup.is_failed()),
            );
        }
        AttemptOutcome::Failed(result) => {
            let status = if result.exception.is_some() {
                Painted::new(format!("[FAIL+ERROR {completed}/{expected}]")).red()
            } else {
                Painted::new(format!("[FAIL {completed}/{expected}]")).red()
            };
            eprintln!(
                "{status} {} ({}, reward={:.3}){}{}",
                result.trial_name,
                result_duration(result),
                result.verifier.rewards.values().sum::<f64>(),
                result_exception_suffix(result),
                cleanup_suffix(result.cleanup.is_failed()),
            );
        }
        AttemptOutcome::Refused(failure) => {
            let message = failure.exception.message.lines().next().unwrap_or_default();
            let status = Painted::new(format!("[REFUSED {completed}/{expected}]")).yellow();
            eprintln!(
                "{status} {} ({}): {message}{}",
                failure.trial_name,
                failure_duration(failure),
                cleanup_suffix(failure.cleanup.is_failed()),
            );
        }
        AttemptOutcome::Errored(failure) => {
            let message = failure.exception.message.lines().next().unwrap_or_default();
            let status = Painted::new(format!("[ERROR {completed}/{expected}]")).red();
            eprintln!(
                "{status} {} ({:?}, {}): {message}{}",
                failure.trial_name,
                failure.exception.kind,
                failure_duration(failure),
                cleanup_suffix(failure.cleanup.is_failed()),
            );
        }
    }
}

fn result_exception_suffix(result: &EvalResult) -> String {
    result
        .exception
        .as_ref()
        .map_or_else(String::new, |exception| {
            format!(", agent error={:?}", exception.kind)
        })
}

fn result_duration(result: &EvalResult) -> String {
    let phases = [
        Some(&result.timing.queue_wait),
        Some(&result.timing.environment_setup),
        Some(&result.timing.environment_readiness),
        Some(&result.timing.agent_setup),
        Some(&result.timing.agent_execution),
        Some(&result.timing.verifier),
        result.cleanup.agent.timing.as_ref(),
        result.cleanup.verifier.timing.as_ref(),
    ];
    format_milliseconds(sum_phase_milliseconds(phases))
}

fn failure_duration(failure: &EvalFailure) -> String {
    let phases = [
        Some(&failure.timing.queue_wait),
        failure.timing.environment_setup.as_ref(),
        failure.timing.environment_readiness.as_ref(),
        failure.timing.agent_setup.as_ref(),
        failure.timing.agent_execution.as_ref(),
        failure.timing.verifier.as_ref(),
        failure.cleanup.agent.timing.as_ref(),
        failure.cleanup.verifier.timing.as_ref(),
    ];
    format_milliseconds(sum_phase_milliseconds(phases))
}

fn sum_phase_milliseconds<'a>(phases: impl IntoIterator<Item = Option<&'a PhaseTiming>>) -> i64 {
    phases
        .into_iter()
        .flatten()
        .map(|phase| {
            phase
                .finished_at
                .signed_duration_since(phase.started_at)
                .num_milliseconds()
                .max(0)
        })
        .fold(0_i64, i64::saturating_add)
}

const fn cleanup_suffix(failed: bool) -> &'static str {
    if failed { " [cleanup failed]" } else { "" }
}

fn format_milliseconds(milliseconds: i64) -> String {
    let seconds = milliseconds / 1_000;
    let millis = milliseconds.unsigned_abs() % 1_000;
    format!("{seconds}.{millis:03}s")
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        collections::BTreeMap,
        convert::Infallible,
        fs,
        future::{self, Future},
        io,
        path::{Path, PathBuf},
        pin::Pin,
        time::Duration,
    };

    use chrono::Utc;
    use clap::Parser;
    use futures_util::{SinkExt, StreamExt};
    use nanocodex::{Nanocodex, OpenAi, Thinking};
    use nanocodex_eval::{
        AttemptAgent, AttemptVerification, AttemptVerificationFailure, AttemptVerifier,
        BillingCompleteness, CleanupPhase, CleanupStatus, EvalAttempt, EvalOutcome, Evaluator,
        Sweep, Task, VerifierResult,
    };
    use nanocodex_vm::tools::{VmToolSession, VmToolSessionError};
    use serde_json::json;
    use sha2::Digest as _;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    use super::{
        DEFAULT_HOST_UTILIZATION_PERCENT, DEFAULT_TRIALS, EvalInterruptError, HostResources,
        InterruptListener, RetainedBuild, RetainedScheduling, Run, RunInvocation, RunMeasurements,
        RunSummary, VmRetention, finish_or_drain, finish_or_interrupt, load_tasks,
        retained_retry_task_names, retained_task_durations,
    };

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        eval: Run,
    }

    struct VmCapabilityVerifier {
        session: Option<VmToolSession>,
    }

    impl AttemptVerifier for VmCapabilityVerifier {
        fn verify<'a>(
            &'a mut self,
            _task: &'a Task,
            attempt: EvalAttempt<'a>,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<AttemptVerification, AttemptVerificationFailure>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                let session = self.session.take().ok_or_else(|| {
                    AttemptVerificationFailure::new(
                        io::Error::other("VM capability verifier session was already consumed"),
                        CleanupPhase::not_required(),
                    )
                })?;
                let cleanup_started = Utc::now();
                let shutdown = session.shutdown().await;
                assert!(
                    !matches!(shutdown, Err(VmToolSessionError::ActiveCapabilities(_))),
                    "the timed-out driver must release every VM capability before verification"
                );
                if let Err(error) = shutdown {
                    let cleanup = CleanupPhase::failed(cleanup_started, &error);
                    return Err(AttemptVerificationFailure::new(error, cleanup));
                }
                fs::write(attempt.directory().join("verifier/test-stdout.txt"), []).map_err(
                    |error| {
                        AttemptVerificationFailure::new(
                            error,
                            CleanupPhase::completed(cleanup_started),
                        )
                    },
                )?;
                Ok(AttemptVerification {
                    result: VerifierResult {
                        exit_code: 0,
                        rewards: BTreeMap::from([("reward".to_owned(), 1.0)]),
                    },
                    stdout: String::new(),
                    stderr: String::new(),
                    cleanup: CleanupPhase::completed(cleanup_started),
                })
            })
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = CleanupPhase> + Send + '_>> {
            Box::pin(async move {
                let Some(session) = self.session.take() else {
                    return CleanupPhase::not_required();
                };
                let cleanup_started = Utc::now();
                match session.shutdown().await {
                    Ok(()) => CleanupPhase::completed(cleanup_started),
                    Err(error) => CleanupPhase::failed(cleanup_started, &error),
                }
            })
        }
    }

    #[tokio::test]
    async fn injected_interrupt_closes_admission_then_waits_for_admitted_work() {
        let (release, released) = tokio::sync::oneshot::channel();
        let (signal, interrupts) = injected_interrupts();
        signal.send(Ok(())).unwrap();
        let execution = finish_or_drain(
            async {
                released.await.unwrap();
                Ok::<_, &'static str>(17)
            },
            interrupts,
            9,
            || {
                release.send(()).unwrap();
                3
            },
        )
        .await
        .unwrap();

        assert_eq!(execution.result.unwrap(), 17);
        assert_eq!(execution.terminal_attempts, 3);
        assert!(execution.interrupted);
    }

    #[tokio::test]
    async fn immediate_second_interrupt_is_not_lost_and_drops_draining_work() {
        let (started_sender, started) = tokio::sync::oneshot::channel();
        let (dropped_sender, dropped) = tokio::sync::oneshot::channel();
        let (signals, interrupts) = injected_interrupts();
        let send_signals = tokio::spawn(async move {
            started.await.unwrap();
            signals.send(Ok(())).unwrap();
            signals.send(Ok(())).unwrap();
        });
        let drain_count = Cell::new(0);
        let result = finish_or_drain(
            async move {
                let _drop_signal = DropSignal(Some(dropped_sender));
                started_sender.send(()).unwrap();
                future::pending::<Result<(), &'static str>>().await
            },
            interrupts,
            9,
            || {
                drain_count.set(drain_count.get() + 1);
                3
            },
        )
        .await;

        assert!(matches!(result, Err(EvalInterruptError::Forced)));
        assert_eq!(drain_count.get(), 1);
        dropped.await.unwrap();
        send_signals.await.unwrap();
    }

    #[tokio::test]
    async fn pending_interrupt_listener_remains_actionable_during_finalization() {
        let (interrupt_sender, interrupts) = injected_interrupts();
        let execution = finish_or_drain(
            future::ready(Ok::<_, &'static str>(17)),
            interrupts,
            1,
            || unreachable!(),
        )
        .await
        .unwrap();
        let (dropped_sender, dropped) = tokio::sync::oneshot::channel();
        let result = finish_or_interrupt(
            async move {
                let _drop_signal = DropSignal(Some(dropped_sender));
                interrupt_sender.send(Ok(())).unwrap();
                future::pending::<()>().await
            },
            execution.interrupt,
        )
        .await;

        assert!(matches!(result, Err(EvalInterruptError::Finalization)));
        dropped.await.unwrap();
    }

    fn injected_interrupts() -> (
        tokio::sync::mpsc::UnboundedSender<std::io::Result<()>>,
        InterruptListener,
    ) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (sender, InterruptListener::Injected(receiver))
    }

    struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[test]
    fn run_measurements_retain_cold_and_warm_phase_boundaries() {
        let output = tempfile::tempdir().unwrap();
        RunMeasurements {
            observability: Duration::from_nanos(1),
            task_loading: Duration::from_nanos(2),
            vm_runtime: Duration::from_nanos(3),
            vm_environments: Duration::from_nanos(4),
            evaluation_setup: Duration::from_nanos(5),
            attempts: Duration::from_nanos(6),
            harbor_finish: Duration::from_nanos(7),
            output: Duration::from_nanos(8),
            total: Duration::from_nanos(36),
        }
        .persist(output.path())
        .unwrap();

        let timing: serde_json::Value =
            serde_json::from_slice(&fs::read(output.path().join("timing.json")).unwrap()).unwrap();
        assert_eq!(timing["vm_runtime_build_ns"], 3);
        assert_eq!(timing["cold_image_and_cache_ns"], 4);
        assert_eq!(timing["attempts_wall_ns"], 6);
        assert_eq!(timing["total_wall_ns"], 36);
    }

    #[test]
    fn vm_guest_build_targets_the_unified_vm_package() {
        let command = super::vm_guest_build_command(Path::new("/tmp/nanocodex-workspace"));
        let arguments = command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(
            arguments,
            [
                "build",
                "--quiet",
                "--locked",
                "--target",
                super::VM_GUEST_TARGET,
                "--package",
                "nanocodex-vm",
                "--bin",
                "nanocodex-vm-guest",
                "--no-default-features",
                "--features",
                "guest-runtime",
            ]
        );
        assert_eq!(
            command.as_std().get_current_dir(),
            Some(Path::new("/tmp/nanocodex-workspace"))
        );
    }

    #[test]
    fn guest_build_record_tracks_exact_cargo_dependencies() {
        let workspace = tempfile::tempdir().unwrap();
        for path in [
            "Cargo.toml",
            "Cargo.lock",
            ".cargo/config.toml",
            "crates/nanocodex-oai-api/Cargo.toml",
            "crates/nanocodex-tools/Cargo.toml",
            "crates/experimental/nanocodex-vm/Cargo.toml",
        ] {
            let path = workspace.path().join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, "fixture").unwrap();
        }
        let source = workspace
            .path()
            .join("crates/experimental/nanocodex-vm/src/tools/guest.rs");
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        fs::write(&source, "first guest source").unwrap();
        let runtime = workspace.path().join("target/guest/debug/guest");
        fs::create_dir_all(runtime.parent().unwrap()).unwrap();
        fs::write(&runtime, "guest binary").unwrap();
        fs::write(
            runtime.with_extension("d"),
            format!("{}: {}\n", runtime.display(), source.display()),
        )
        .unwrap();

        super::write_vm_guest_build_record(workspace.path(), &runtime).unwrap();
        assert!(super::vm_guest_runtime_is_fresh(workspace.path(), &runtime).unwrap());

        fs::write(source, "changed guest source with a different size").unwrap();
        assert!(!super::vm_guest_runtime_is_fresh(workspace.path(), &runtime).unwrap());
    }

    #[test]
    fn cargo_dep_info_parser_preserves_escaped_paths() {
        let paths = super::parse_cargo_dep_info(
            "/tmp/guest: /tmp/plain.rs /tmp/with\\ space.rs /tmp/back\\\\slash.rs\n",
        )
        .unwrap();

        assert_eq!(
            paths,
            [
                PathBuf::from("/tmp/plain.rs"),
                PathBuf::from("/tmp/with space.rs"),
                PathBuf::from("/tmp/back\\slash.rs"),
            ]
        );
    }

    #[test]
    fn accepts_repeated_tasks_with_per_task_trials() {
        let cli = TestCli::try_parse_from([
            "nanoeval",
            "--task",
            "tasks/first",
            "--task",
            "tasks/second",
            "--trials",
            "5",
            "--concurrency",
            "10",
            "--max-memory-mb",
            "24576",
        ])
        .unwrap();

        assert_eq!(
            cli.eval.tasks,
            [PathBuf::from("tasks/first"), PathBuf::from("tasks/second")]
        );
        assert_eq!(cli.eval.trials, 5);
        assert_eq!(cli.eval.concurrency, Some(10));
        assert_eq!(cli.eval.max_memory_mb, Some(24_576));
        assert_eq!(cli.eval.host_utilization, DEFAULT_HOST_UTILIZATION_PERCENT);
        assert!(!cli.eval.vm_retention.unwrap_or_default().retains_passes());
        assert!(cli.eval.suites.is_empty());
    }

    #[test]
    fn defaults_to_five_independent_trials_per_task() {
        let cli = TestCli::try_parse_from(["nanoeval", "--task", "tasks/first"]).unwrap();

        let resolved = cli.eval.resolve_run().unwrap();

        assert_eq!(cli.eval.trials, DEFAULT_TRIALS);
        assert_eq!(resolved.trials, DEFAULT_TRIALS);
        assert!(!resolved.web_search);
    }

    #[test]
    fn web_search_is_an_explicit_eval_capability() {
        let cli =
            TestCli::try_parse_from(["nanoeval", "--task", "tasks/first", "--web-search", "true"])
                .unwrap();

        let resolved = cli.eval.resolve_run().unwrap();

        assert!(resolved.web_search);
    }

    #[test]
    fn prebuilt_guest_runtime_is_an_explicit_eval_artifact() {
        let cli = TestCli::try_parse_from([
            "nanoeval",
            "--task",
            "tasks/first",
            "--vm-guest-runtime",
            "/opt/nanocodex-vm-guest",
        ])
        .unwrap();

        let resolved = cli.eval.resolve_run().unwrap();

        assert_eq!(
            resolved.vm_guest_runtime,
            Some(PathBuf::from("/opt/nanocodex-vm-guest"))
        );
    }

    #[test]
    fn shared_vm_cache_is_an_explicit_eval_resource() {
        let cli = TestCli::try_parse_from([
            "nanoeval",
            "--task",
            "tasks/first",
            "--vm-cache",
            "/var/cache/nanocodex-vm",
        ])
        .unwrap();

        let resolved = cli.eval.resolve_run().unwrap();

        assert_eq!(resolved.vm_cache, PathBuf::from("/var/cache/nanocodex-vm"));
    }

    #[tokio::test]
    async fn explicit_guest_runtime_rejects_the_wrong_elf_machine() {
        let job = tempfile::tempdir().unwrap();
        let runtime = job.path().join("wrong-architecture");
        let wrong_machine = if super::VM_GUEST_ELF_MACHINE == 62 {
            183
        } else {
            62
        };
        fs::write(&runtime, guest_elf(wrong_machine)).unwrap();

        let error = super::prepare_runtime_for_vm(None, Some(&runtime), job.path(), None)
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&format!("e_machine {wrong_machine}"))
        );
        assert!(error.to_string().contains(super::VM_GUEST_TARGET));
        assert!(!job.path().join(super::GUEST_RUNTIME_ARTIFACT_ROOT).exists());
    }

    #[tokio::test]
    async fn implicit_resume_rebuilds_from_the_job_owned_guest_artifact() {
        let root = tempfile::tempdir().unwrap();
        let task = write_test_task(&root.path().join("task"));
        let output = root.path().join("jobs");
        let agent = Nanocodex::builder(OpenAi::new("test").unwrap());
        let sweep = Sweep::builder()
            .tasks(vec![task.clone()])
            .trials(1)
            .agent("default", agent.clone())
            .unwrap()
            .build()
            .unwrap();
        let (first, first_events) = Evaluator::builder(agent.clone())
            .output_directory(&output)
            .resume_incomplete(&sweep)
            .build()
            .unwrap();
        assert!(!first.resumed());

        let source = root.path().join("mutable-workspace-guest");
        fs::write(&source, guest_elf(super::VM_GUEST_ELF_MACHINE)).unwrap();
        let (_, first_disk, first_runtime, _) = super::prepare_run_vm(
            None,
            Some(&source),
            first.directory(),
            first.resumed(),
            None,
            false,
        )
        .await
        .unwrap();
        let first_runtime = first_runtime.unwrap();
        let artifact = first
            .directory()
            .join(first_runtime.artifact_path.as_ref().unwrap());
        assert!(artifact.is_file());
        assert!(first_disk.starts_with(first.directory()));

        let resolved = super::ResolvedRun {
            task_paths: vec![task.root().to_path_buf()],
            output: output.clone(),
            trials: 1,
            concurrency: 1,
            max_memory_mb: None,
            vm_rootfs: None,
            vm_guest_runtime: Some(source.clone()),
            vm_cache: PathBuf::from(".cache/vm"),
            vm_retention: VmRetention::Failures,
            thinking: Thinking::Low,
            web_search: false,
            rerun_from: None,
            automatic_scheduling: None,
        };
        super::persist_invocation(
            first.directory(),
            &resolved.invocation(Some(first_runtime.clone())).unwrap(),
        )
        .unwrap();
        let job = first.directory().to_path_buf();
        drop(first_events);
        drop(first);

        fs::write(&source, b"overwritten mutable build output").unwrap();
        fs::remove_dir_all(job.join(super::GUEST_RUNTIME_CACHE_ROOT)).unwrap();

        let (resumed, resumed_events) = Evaluator::builder(agent)
            .output_directory(&output)
            .resume_incomplete(&sweep)
            .build()
            .unwrap();
        assert!(resumed.resumed());
        assert_eq!(resumed.directory(), job);
        let (_, resumed_disk, resumed_runtime, _) = super::prepare_run_vm(
            None,
            None,
            resumed.directory(),
            resumed.resumed(),
            None,
            true,
        )
        .await
        .unwrap();

        assert!(resumed_disk.is_file());
        assert!(resumed_disk.starts_with(resumed.directory()));
        assert_eq!(resumed_runtime.unwrap(), first_runtime);
        drop(resumed_events);
        drop(resumed);
    }

    #[test]
    fn retained_resume_rehydrates_missing_job_runtime_from_exact_requested_elf() {
        let root = tempfile::tempdir().unwrap();
        let job = root.path().join("job");
        fs::create_dir(&job).unwrap();
        let bytes = guest_elf(super::VM_GUEST_ELF_MACHINE);
        let requested = root.path().join("exact-guest-runtime");
        fs::write(&requested, &bytes).unwrap();
        let (artifact_path, artifact) = super::retain_guest_runtime_bytes(&job, &bytes).unwrap();
        let runtime_disk = nanocodex_vm::tools::GuestRuntimeDisk::prepare(
            &artifact,
            job.join(super::GUEST_RUNTIME_CACHE_ROOT),
        )
        .unwrap();
        let origin = super::RetainedGuestRuntimeOrigin {
            job: job.clone(),
            runtime: super::RetainedGuestRuntime {
                target: super::VM_GUEST_TARGET.to_owned(),
                binary_sha256: hex::encode(sha2::Sha256::digest(&bytes)),
                runtime_disk_digest: Some(runtime_disk.digest().to_owned()),
                artifact_path: Some(artifact_path),
                source: "explicit_binary".to_owned(),
                source_path: PathBuf::from("/diagnostic/source"),
                host_git_sha: "test".to_owned(),
            },
        };
        fs::remove_dir_all(job.join("guest-runtime")).unwrap();
        assert!(!artifact.parent().unwrap().exists());

        let prepared =
            super::prepare_retained_guest_runtime(&job, &origin, Some(&requested), true).unwrap();

        assert_eq!(fs::read(&artifact).unwrap(), bytes);
        assert!(prepared.disk.is_file());
        assert!(prepared.disk.starts_with(&job));
        assert_eq!(prepared.identity.unwrap(), origin.runtime);
    }

    #[cfg(unix)]
    #[test]
    fn guest_runtime_retention_rejects_a_symlink_escape_before_creating_directories() {
        let root = tempfile::tempdir().unwrap();
        let job = root.path().join("job");
        let outside = root.path().join("outside");
        fs::create_dir(&job).unwrap();
        fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, job.join("guest-runtime")).unwrap();

        let error =
            super::retain_guest_runtime_bytes(&job, &guest_elf(super::VM_GUEST_ELF_MACHINE))
                .unwrap_err();

        assert!(error.to_string().contains("escapes job"));
        assert!(!outside.join("artifacts").exists());
    }

    #[cfg(unix)]
    #[test]
    fn guest_runtime_disk_rejects_a_sibling_cache_symlink_escape() {
        let root = tempfile::tempdir().unwrap();
        let job = root.path().join("job");
        let outside = root.path().join("outside");
        fs::create_dir(&job).unwrap();
        fs::create_dir(&outside).unwrap();
        let (_, artifact) =
            super::retain_guest_runtime_bytes(&job, &guest_elf(super::VM_GUEST_ELF_MACHINE))
                .unwrap();
        std::os::unix::fs::symlink(&outside, job.join(super::GUEST_RUNTIME_CACHE_ROOT)).unwrap();

        let error = super::prepare_job_guest_runtime_disk(&job, &artifact).unwrap_err();

        assert!(error.to_string().contains("escapes job"));
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
    }

    #[test]
    fn guest_runtime_job_paths_reject_parent_components() {
        let job = tempfile::tempdir().unwrap();
        let path = job
            .path()
            .join(super::GUEST_RUNTIME_CACHE_ROOT)
            .join("..")
            .join("outside");

        let error = super::ensure_job_owned_path(job.path(), &path).unwrap_err();

        assert!(error.to_string().contains("escapes job"));
    }

    #[test]
    fn retained_runtime_disk_digest_is_enforced() {
        let job = tempfile::tempdir().unwrap();
        let bytes = guest_elf(super::VM_GUEST_ELF_MACHINE);
        let (artifact_path, artifact) =
            super::retain_guest_runtime_bytes(job.path(), &bytes).unwrap();
        let runtime_disk = nanocodex_vm::tools::GuestRuntimeDisk::prepare(
            &artifact,
            job.path().join(super::GUEST_RUNTIME_CACHE_ROOT),
        )
        .unwrap();
        let origin = super::RetainedGuestRuntimeOrigin {
            job: job.path().to_path_buf(),
            runtime: super::RetainedGuestRuntime {
                target: super::VM_GUEST_TARGET.to_owned(),
                binary_sha256: hex::encode(sha2::Sha256::digest(&bytes)),
                runtime_disk_digest: Some("0".repeat(64)),
                artifact_path: Some(artifact_path),
                source: "explicit_binary".to_owned(),
                source_path: PathBuf::from("/diagnostic/source"),
                host_git_sha: "test".to_owned(),
            },
        };

        let error =
            super::prepare_retained_guest_runtime(job.path(), &origin, None, true).unwrap_err();

        assert!(error.to_string().contains(runtime_disk.digest()));
        assert!(error.to_string().contains(&"0".repeat(64)));
    }

    #[test]
    fn guest_source_commit_must_match_the_host_binary() {
        assert!(super::validate_vm_guest_commit("abc123", "abc123").is_ok());
        let error = super::validate_vm_guest_commit("host123", "source456").unwrap_err();
        assert!(error.to_string().contains("host123"));
        assert!(error.to_string().contains("source456"));
        assert!(error.to_string().contains("--vm-guest-runtime"));
    }

    #[test]
    fn cost_summary_distinguishes_known_and_unpriced_attempts() {
        let mut summary = RunSummary {
            total: 3,
            ..RunSummary::default()
        };

        summary.record_estimated_cost(Some(0.125), BillingCompleteness::Complete);
        summary.record_estimated_cost(None, BillingCompleteness::Complete);
        summary.record_estimated_cost(Some(0.375), BillingCompleteness::Complete);
        summary.record_estimated_cost(Some(4.304_052), BillingCompleteness::Unknown);

        assert_eq!(summary.known_estimated_cost_usd, Some(0.5));
        assert_eq!(summary.priced_attempts, 2);
        assert_eq!(
            summary.observed_estimated_cost_lower_bound_usd,
            Some(4.804_052)
        );
        assert_eq!(summary.observed_priced_attempts, 3);
        assert_eq!(summary.billing_unknown, 1);
    }

    #[test]
    fn scored_safety_refusal_overlaps_refusal_and_error_axes() {
        let mut summary = RunSummary {
            total: 1,
            scored: 1,
            failed: 1,
            ..RunSummary::default()
        };

        summary.record_exception(Some(nanocodex_eval::EvalExceptionKind::AgentSafetyRefusal));

        assert_eq!(summary.scored, 1);
        assert_eq!(summary.unscored, 0);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.refused, 1);
        assert_eq!(summary.errored, 1);
    }

    #[test]
    fn host_defaults_use_the_configured_share_of_cpu_and_memory() {
        let host = HostResources {
            logical_cpus: 10,
            physical_memory_bytes: Some(32 * 1024 * 1024 * 1024),
        };

        let defaults = host.scheduling_defaults(80);

        assert_eq!(defaults.concurrency, 8);
        assert_eq!(defaults.max_memory_mb, Some(26_214));
    }

    #[test]
    fn host_defaults_keep_at_least_one_execution_slot() {
        let host = HostResources {
            logical_cpus: 1,
            physical_memory_bytes: None,
        };

        let defaults = host.scheduling_defaults(1);

        assert_eq!(defaults.concurrency, 1);
        assert_eq!(defaults.max_memory_mb, None);
    }

    #[test]
    fn explicit_scheduler_limits_disable_automatic_resolution() {
        let cli = TestCli::try_parse_from([
            "nanoeval",
            "--task",
            "tasks/first",
            "--concurrency",
            "3",
            "--max-memory-mb",
            "4096",
        ])
        .unwrap();

        let resolved = cli.eval.resolve_run().unwrap();

        assert_eq!(resolved.concurrency, 3);
        assert_eq!(resolved.max_memory_mb, Some(4_096));
        assert_eq!(resolved.automatic_scheduling, None);
    }

    #[test]
    fn resumed_workload_allows_scheduler_changes_only() {
        let retained = RunInvocation {
            version: super::INVOCATION_VERSION,
            nanocodex_build: RetainedBuild {
                version: "test".to_owned(),
                git_sha: "0123456789abcdef".to_owned(),
                built_at: "2026-07-28T00:00:00Z".to_owned(),
                executable_sha256: "abc123".to_owned(),
            },
            model: "gpt-5.6-sol".to_owned(),
            tool_profile: "microvm_workspace".to_owned(),
            seed: None,
            scheduling: RetainedScheduling {
                policy: super::SCHEDULING_POLICY.to_owned(),
                automatic_utilization_percent: Some(80),
                concurrency_source: "automatic".to_owned(),
                memory_source: "automatic".to_owned(),
            },
            trials: 5,
            concurrency: 16,
            max_memory_mb: Some(49_152),
            vm_rootfs: None,
            guest_runtime: Some(super::RetainedGuestRuntime {
                target: super::VM_GUEST_TARGET.to_owned(),
                binary_sha256: "guest123".to_owned(),
                runtime_disk_digest: Some("disk123".to_owned()),
                artifact_path: None,
                source: "explicit_binary".to_owned(),
                source_path: PathBuf::from("/opt/nanocodex-vm-guest"),
                host_git_sha: "0123456789abcdef".to_owned(),
            }),
            vm_retention: VmRetention::Failures,
            thinking: "xhigh".to_owned(),
            web_search: false,
            rerun_from: None,
        };
        let aggregate_identity = super::aggregate_run_identity(&retained);
        assert_eq!(aggregate_identity.model, "gpt-5.6-sol");
        assert_eq!(aggregate_identity.reasoning_effort, "xhigh");
        assert_eq!(aggregate_identity.tool_profile, "microvm_workspace");
        assert_eq!(
            aggregate_identity.build.executable_sha256.as_deref(),
            Some("abc123")
        );
        assert_eq!(
            aggregate_identity
                .vm
                .as_ref()
                .and_then(|vm| vm.guest_runtime_sha256.as_deref()),
            Some("guest123")
        );
        let mut resumed = retained.clone();
        resumed.concurrency = 30;
        resumed.max_memory_mb = Some(58_000);

        assert!(retained.same_workload(&resumed));

        resumed.thinking = "high".to_owned();
        assert!(!retained.same_workload(&resumed));

        let mut changed_guest = retained.clone();
        changed_guest.guest_runtime.as_mut().unwrap().binary_sha256 = "different".to_owned();
        assert!(!retained.same_workload(&changed_guest));
    }

    #[test]
    fn passed_vm_retention_is_explicit() {
        let cli =
            TestCli::try_parse_from(["nanoeval", "--task", "tasks/first", "--vm-retention", "all"])
                .unwrap();

        assert!(cli.eval.vm_retention.unwrap().retains_passes());
    }

    #[test]
    fn rerun_is_a_task_source_with_foundry_style_name_filters() {
        let cli = TestCli::try_parse_from([
            "nanoeval",
            "--rerun",
            "webserver",
            "--rerun-from",
            "job-id",
            "--match-task",
            "torch-.*",
            "--match-task",
            "mteb",
            "--include-errored",
            "--list",
        ])
        .unwrap();

        assert!(cli.eval.retry.rerun);
        assert_eq!(cli.eval.retry.rerun_from, Some(PathBuf::from("job-id")));
        assert_eq!(cli.eval.retry.names, ["webserver"]);
        assert_eq!(cli.eval.retry.match_task, ["torch-.*", "mteb"]);
        assert!(cli.eval.retry.statuses.include_errored);
        assert!(cli.eval.retry.list);
        assert!(cli.eval.tasks.is_empty());
        assert!(cli.eval.suites.is_empty());
    }

    #[test]
    fn positional_rerun_names_are_literal_substrings() {
        let cli = TestCli::try_parse_from(["nanoeval", "--rerun", "task.+", "--list"]).unwrap();
        let matcher = super::retry_matcher(&cli.eval.retry).unwrap().unwrap();

        assert!(matcher.is_match("terminal-bench/task.+example"));
        assert!(!matcher.is_match("terminal-bench/taskXYZexample"));
    }

    #[test]
    fn suite_loads_immediate_tasks_in_name_order() {
        let suite = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tasks");
        let cli = TestCli::try_parse_from([
            "nanoeval",
            "--suite",
            suite.to_str().unwrap(),
            "--concurrency",
            "3",
        ])
        .unwrap();
        let tasks = load_tasks(cli.eval.tasks, cli.eval.suites).unwrap();
        let names = tasks.iter().map(Task::name).collect::<Vec<_>>();

        assert_eq!(
            names,
            [
                "nanoeval/extract-todos",
                "nanoeval/uppercase-message",
                "nanoeval/write-greeting"
            ]
        );
    }

    #[test]
    fn retained_task_duration_uses_the_median_completed_trial() {
        let output = tempfile::tempdir().unwrap();
        let job = output.path().join("job");
        for (trial, finished_at) in [
            ("first", "2026-07-23T00:00:10Z"),
            ("second", "2026-07-23T00:00:30Z"),
            ("third", "2026-07-23T00:00:20Z"),
        ] {
            let directory = job.join(trial);
            fs::create_dir_all(&directory).unwrap();
            fs::write(
                directory.join("result.json"),
                format!(
                    r#"{{"task_name":"terminal-bench/example","started_at":"2026-07-23T00:00:00Z","finished_at":"{finished_at}"}}"#
                ),
            )
            .unwrap();
        }

        let estimates = retained_task_durations(output.path()).unwrap();
        assert_eq!(
            estimates["terminal-bench/example"],
            std::time::Duration::from_secs(20)
        );
    }

    #[test]
    fn retry_selection_distinguishes_scores_refusals_and_errors() {
        let job = tempfile::tempdir().unwrap();
        super::write_json_atomic(
            &job.path().join(super::INVOCATION_FILE),
            &retained_invocation(None),
        )
        .unwrap();
        for (trial, result) in [
            (
                "passed",
                r#"{"task_name":"terminal-bench/passed","outcome":"passed","scored":true,"verifier_result":{"rewards":{"reward":1.0}},"exception_info":null}"#,
            ),
            (
                "passed-with-cleanup-failure",
                r#"{"task_name":"terminal-bench/passed-with-cleanup-failure","outcome":"passed","scored":true,"verifier_result":{"rewards":{"reward":1.0}},"exception_info":{"exception_type":"CleanupError"}}"#,
            ),
            (
                "cleanup-only",
                r#"{"task_name":"terminal-bench/cleanup-only","outcome":"infrastructure_error","scored":false,"verifier_result":null,"exception_info":{"exception_type":"CleanupError"}}"#,
            ),
            (
                "partially-failed",
                r#"{"task_name":"terminal-bench/partially-failed","outcome":"verifier_failed","scored":true,"verifier_result":{"rewards":{"first":1.0,"second":0.0}},"exception_info":null}"#,
            ),
            (
                "failed",
                r#"{"task_name":"terminal-bench/torch-failed","outcome":"verifier_failed","scored":true,"verifier_result":{"rewards":{"reward":0.0}},"exception_info":null}"#,
            ),
            (
                "refused",
                r#"{"task_name":"terminal-bench/refused","outcome":"safety_refusal","scored":false,"verifier_result":null,"exception_info":{"exception_type":"AgentSafetyRefusalError"}}"#,
            ),
            (
                "explicit-non-refusal",
                r#"{"task_name":"terminal-bench/explicit-non-refusal","outcome":"safety_refusal","scored":false,"verifier_result":null,"exception_info":{"exception_type":"VerifierError"}}"#,
            ),
            (
                "errored",
                r#"{"task_name":"terminal-bench/errored","outcome":"infrastructure_error","scored":false,"verifier_result":null,"exception_info":{"exception_type":"VerifierError"}}"#,
            ),
            (
                "scored-timeout-pass",
                r#"{"task_name":"terminal-bench/scored-timeout-pass","outcome":"agent_timeout","scored":true,"verifier_result":{"rewards":{"reward":1.0}},"exception_info":{"exception_type":"AgentTimeoutError"}}"#,
            ),
            (
                "scored-timeout-fail",
                r#"{"task_name":"terminal-bench/scored-timeout-fail","outcome":"agent_timeout","scored":true,"verifier_result":{"rewards":{"reward":0.0}},"exception_info":{"exception_type":"AgentTimeoutError"}}"#,
            ),
            (
                "unscored-with-reward",
                r#"{"task_name":"terminal-bench/unscored-with-reward","outcome":"passed","scored":false,"verifier_result":{"rewards":{"reward":1.0}},"exception_info":null}"#,
            ),
        ] {
            let directory = job.path().join(trial);
            fs::create_dir(&directory).unwrap();
            fs::write(directory.join("result.json"), result).unwrap();
        }

        let failed = retained_retry_task_names(job.path(), false, false, None).unwrap();
        assert_eq!(
            failed.task_names,
            [
                "terminal-bench/partially-failed".to_owned(),
                "terminal-bench/scored-timeout-fail".to_owned(),
                "terminal-bench/torch-failed".to_owned()
            ]
            .into()
        );

        let matcher =
            regex::RegexSet::new(["torch|errored|scored-timeout|refused|unscored"]).unwrap();
        let selected = retained_retry_task_names(job.path(), true, true, Some(&matcher)).unwrap();
        assert_eq!(
            selected.task_names,
            [
                "terminal-bench/errored".to_owned(),
                "terminal-bench/refused".to_owned(),
                "terminal-bench/scored-timeout-fail".to_owned(),
                "terminal-bench/torch-failed".to_owned(),
            ]
            .into()
        );

        let errors_only = retained_retry_task_names(job.path(), false, true, None).unwrap();
        assert!(errors_only.task_names.contains("terminal-bench/refused"));
        assert!(
            errors_only
                .task_names
                .contains("terminal-bench/explicit-non-refusal")
        );
        assert!(
            !errors_only
                .task_names
                .contains("terminal-bench/cleanup-only")
        );
        assert!(
            !errors_only
                .task_names
                .contains("terminal-bench/scored-timeout-pass")
        );

        let refusals_only = retained_retry_task_names(job.path(), true, false, None).unwrap();
        assert!(refusals_only.task_names.contains("terminal-bench/refused"));
        assert!(
            !refusals_only
                .task_names
                .contains("terminal-bench/explicit-non-refusal")
        );
        assert!(
            !refusals_only
                .task_names
                .contains("terminal-bench/cleanup-only")
        );
    }

    #[test]
    fn retry_selection_uses_pass_at_k_across_trials() {
        let job = tempfile::tempdir().unwrap();
        super::write_json_atomic(
            &job.path().join(super::INVOCATION_FILE),
            &retained_invocation(None),
        )
        .unwrap();
        for (trial, task, outcome, scored, verifier_result, exception_info) in [
            (
                "eventual-pass-failed",
                "terminal-bench/eventual-pass",
                "verifier_failed",
                true,
                r#"{"rewards":{"reward":0.0}}"#,
                "null",
            ),
            (
                "eventual-pass-passed",
                "terminal-bench/eventual-pass",
                "passed",
                true,
                r#"{"rewards":{"reward":1.0}}"#,
                "null",
            ),
            (
                "scored-failure",
                "terminal-bench/scored-failure",
                "verifier_failed",
                true,
                r#"{"rewards":{"reward":0.0}}"#,
                "null",
            ),
            (
                "scored-failure-error",
                "terminal-bench/scored-failure",
                "agent_timeout",
                false,
                "null",
                r#"{"exception_type":"AgentTimeoutError"}"#,
            ),
        ] {
            let directory = job.path().join(trial);
            fs::create_dir(&directory).unwrap();
            fs::write(
                directory.join("result.json"),
                format!(
                    r#"{{"task_name":"{task}","outcome":"{outcome}","scored":{scored},"verifier_result":{verifier_result},"exception_info":{exception_info}}}"#
                ),
            )
            .unwrap();
        }

        let queue = retained_retry_task_names(job.path(), false, false, None).unwrap();

        assert_eq!(
            queue.task_names,
            ["terminal-bench/scored-failure".to_owned()].into()
        );
    }

    #[test]
    fn retry_lineage_overlays_only_tasks_present_in_the_child_job() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("base");
        let child = root.path().join("child");
        for (job, trial, task, reward) in [
            (&base, "first", "terminal-bench/first", 0.0),
            (&base, "second", "terminal-bench/second", 0.0),
            (&child, "first-retry", "terminal-bench/first", 1.0),
        ] {
            let directory = job.join(trial);
            fs::create_dir_all(&directory).unwrap();
            fs::write(
                directory.join("result.json"),
                format!(
                    r#"{{"task_name":"{task}","outcome":"{}","scored":true,"verifier_result":{{"rewards":{{"reward":{reward}}}}},"exception_info":null}}"#,
                    if reward > 0.0 { "passed" } else { "verifier_failed" },
                ),
            )
            .unwrap();
        }
        super::write_json_atomic(
            &base.join(super::INVOCATION_FILE),
            &retained_invocation(None),
        )
        .unwrap();
        super::write_json_atomic(
            &child.join(super::INVOCATION_FILE),
            &retained_invocation(Some(base.canonicalize().unwrap())),
        )
        .unwrap();

        let queue = retained_retry_task_names(&child, false, false, None).unwrap();

        assert_eq!(queue.lineage.len(), 2);
        assert_eq!(
            queue.task_names,
            ["terminal-bench/second".to_owned()].into()
        );
    }

    fn retained_invocation(rerun_from: Option<PathBuf>) -> super::RunInvocation {
        super::RunInvocation {
            version: super::INVOCATION_VERSION,
            nanocodex_build: super::RetainedBuild {
                version: "test".to_owned(),
                git_sha: "0123456789abcdef".to_owned(),
                built_at: "2026-07-28T00:00:00Z".to_owned(),
                executable_sha256: "abc123".to_owned(),
            },
            model: "gpt-5.6-sol".to_owned(),
            tool_profile: "microvm_workspace".to_owned(),
            seed: None,
            scheduling: super::RetainedScheduling {
                policy: super::SCHEDULING_POLICY.to_owned(),
                automatic_utilization_percent: None,
                concurrency_source: "configured".to_owned(),
                memory_source: "configured".to_owned(),
            },
            trials: 1,
            concurrency: 1,
            max_memory_mb: None,
            vm_rootfs: None,
            guest_runtime: None,
            vm_retention: super::VmRetention::Failures,
            thinking: "low".to_owned(),
            web_search: false,
            rerun_from,
        }
    }

    #[test]
    fn requires_at_least_one_task() {
        let Err(error) = TestCli::try_parse_from(["nanoeval"]) else {
            panic!("a task should be required");
        };
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[tokio::test]
    async fn timed_out_driver_releases_vm_capabilities_before_verification() {
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
        let openai = OpenAi::builder("test")
            .websocket_url(endpoint)
            .build()
            .unwrap();
        let output = tempfile::tempdir().unwrap();
        let task_directory = tempfile::tempdir().unwrap();
        let task = write_timeout_task(task_directory.path(), 1.0);
        let (evaluator, _events) = Evaluator::builder(Nanocodex::builder(openai))
            .output_directory(output.path())
            .attempt_agent(|_attempt, builder| {
                let script = r#"
IFS= read -r request
case "$request" in
    *'"kind":"shutdown"'*)
        printf '%s\n' '{"kind":"shutdown","payload":{"id":0,"error":null}}'
        ;;
    *)
        exit 91
        ;;
esac
"#;
                let mut command = tokio::process::Command::new("/bin/sh");
                command.arg("-c").arg(script).arg("nanocodex-timeout-vm");
                let session = VmToolSession::spawn(&mut command).unwrap();
                let tools = session.tools().tools_builder().build().unwrap();
                Ok::<_, Infallible>(AttemptAgent::new(builder.tools(tools)).verifier(
                    VmCapabilityVerifier {
                        session: Some(session),
                    },
                ))
            })
            .build()
            .unwrap();

        let outcome = evaluator.task(task).await.unwrap();
        let result = outcome
            .scored()
            .expect("the VM verifier must run after the timed-out driver joins");

        assert_eq!(result.outcome, EvalOutcome::AgentTimeout);
        assert_eq!(result.cleanup.agent.status, CleanupStatus::Completed);
        assert_eq!(result.cleanup.verifier.status, CleanupStatus::Completed);
        let measurements =
            super::AttemptMeasurementTotals::from_results(std::slice::from_ref(result));
        assert_eq!(measurements.runtime_complete_attempts, 0);
        assert_eq!(measurements.runtime_lower_bound_attempts, 1);
        assert_eq!(measurements.runtime_missing_attempts, 0);
        assert_eq!(measurements.usage_complete_attempts, 0);
        assert_eq!(measurements.usage_lower_bound_attempts, 0);
        assert_eq!(measurements.usage_missing_attempts, 1);
        let summary =
            RunSummary::from_attempts(&[super::AttemptOutcome::from_result(result.clone())]);
        assert_eq!(summary.billing_missing, 1);
        server.await.unwrap();
    }

    fn guest_elf(machine: u16) -> Vec<u8> {
        let mut bytes = vec![0_u8; 64];
        bytes[..4].copy_from_slice(b"\x7fELF");
        bytes[4] = 2;
        bytes[5] = 1;
        bytes[6] = 1;
        bytes[16..18].copy_from_slice(&2_u16.to_le_bytes());
        bytes[18..20].copy_from_slice(&machine.to_le_bytes());
        bytes[20..24].copy_from_slice(&1_u32.to_le_bytes());
        bytes
    }

    fn write_timeout_task(root: &Path, timeout_seconds: f64) -> Task {
        fs::create_dir_all(root.join("environment")).unwrap();
        fs::create_dir_all(root.join("tests")).unwrap();
        fs::write(root.join("instruction.md"), "Complete the task.\n").unwrap();
        fs::write(root.join("tests/test.sh"), "exit 0\n").unwrap();
        fs::write(
            root.join("task.toml"),
            format!(
                r#"
schema_version = "1.1"
[task]
name = "terminal-bench/timeout-vm-capability"
description = "timeout VM capability fixture"
[agent]
timeout_sec = {timeout_seconds}
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

    fn write_test_task(root: &Path) -> Task {
        fs::create_dir_all(root.join("environment")).unwrap();
        fs::create_dir_all(root.join("tests")).unwrap();
        fs::write(root.join("instruction.md"), "Complete the task.\n").unwrap();
        fs::write(root.join("tests/test.sh"), "exit 0\n").unwrap();
        fs::write(
            root.join("task.toml"),
            r#"
schema_version = "1.1"
[task]
name = "terminal-bench/runtime-resume"
description = "runtime resume fixture"
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
"#,
        )
        .unwrap();
        Task::load(root).unwrap()
    }
}
