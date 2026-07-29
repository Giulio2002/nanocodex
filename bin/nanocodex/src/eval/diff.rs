use std::{
    io::{self, Write as _},
    path::{Path, PathBuf},
};

use clap::Args;
use eyre::{Result, eyre};
use nanocodex::Thinking;
use nanocodex_eval::*;

use super::run;
use crate::{
    config::{EvalAgentArgs, SharedAuth},
    observability::ObservabilityArgs,
};

const DEFAULT_OUTPUT_DIRECTORY: &str = ".nanocodex/eval-diff";

#[derive(Args)]
pub(crate) struct Diff {
    /// Rebuild the derived API/event-loop comparison from retained raw captures.
    ///
    /// This performs no agent, model, VM, or verifier work.
    #[arg(
        long,
        value_name = "COMPARISON_DIRECTORY",
        conflicts_with_all = ["task", "codex_bin", "vm_guest_runtime", "vm_refresh"]
    )]
    reanalyze: Option<PathBuf>,

    /// One evaluator task directory to run through both agents.
    #[arg(
        long,
        value_name = "DIRECTORY",
        required_unless_present = "reanalyze",
        conflicts_with = "reanalyze"
    )]
    task: Option<PathBuf>,

    /// Exact stock-Codex executable to compare against Nanocodex.
    ///
    /// This must be the Linux executable that runs in the guest.
    #[arg(
        long,
        value_name = "EXECUTABLE",
        required_unless_present = "reanalyze",
        conflicts_with = "reanalyze"
    )]
    codex_bin: Option<PathBuf>,

    /// Parent directory for paired evaluator artifacts.
    #[arg(long, default_value = DEFAULT_OUTPUT_DIRECTORY)]
    output: PathBuf,

    /// Use this prebuilt Nanocodex guest-runtime ELF.
    #[arg(long, value_name = "ELF")]
    vm_guest_runtime: Option<PathBuf>,

    /// Resolve the task image at the registry instead of reusing its local resolution.
    #[arg(long)]
    vm_refresh: bool,

    /// Print the complete comparison record as JSON.
    #[arg(long)]
    json: bool,

    #[command(flatten)]
    observability: ObservabilityArgs,

    #[command(flatten)]
    agent: EvalAgentArgs,
}

impl Diff {
    pub(crate) async fn run(self) -> Result<()> {
        let _observability = self.observability.install(false, Path::new("."))?;
        if let Some(directory) = self.reanalyze {
            let reanalysis = reanalyze(directory)?;
            if self.json {
                write_json(reanalysis.comparison())?;
            } else {
                print!("{}", reanalysis.human_summary());
            }
            return Ok(());
        }

        let task = Task::load(
            self.task
                .as_deref()
                .ok_or_else(|| eyre!("--task is required unless --reanalyze is used"))?,
        )?;
        let thinking = self.agent.thinking().unwrap_or(Thinking::Medium);
        let web_search = self.agent.web_search().unwrap_or(false);
        let (nanocodex, auth) = self.agent.shared_builder(thinking, web_search)?;
        let codex_auth = match auth {
            SharedAuth::ApiKey(api_key) => CodexAuth::api_key(api_key),
            SharedAuth::AuthFile(path) => CodexAuth::auth_file(path),
        };
        let current_executable = std::env::current_exe()?;
        let runtime_image =
            run::prepare_vm_guest_runtime_from(self.vm_guest_runtime.as_deref()).await?;
        let vm = VmResources::builder(&current_executable, runtime_image)
            .task(task.clone())
            .cache_policy(if self.vm_refresh {
                CachePolicy::Refresh
            } else {
                CachePolicy::Reuse
            })
            .prepare()
            .await?;
        let report = DifferentialEval::builder(task, nanocodex)
            .codex(
                self.codex_bin
                    .ok_or_else(|| eyre!("--codex-bin is required unless --reanalyze is used"))?,
                codex_auth,
            )
            .vm(vm)
            .output_directory(self.output)
            .thinking(thinking)
            .web_search(web_search)
            .nanocodex_executable(
                ExecutableIdentity::new(current_executable, env!("NANOCODEX_SEMVER_VERSION"))
                    .git_sha(env!("VERGEN_GIT_SHA"))
                    .built_at(env!("VERGEN_BUILD_TIMESTAMP")),
            )
            .build()?
            .run()
            .await?;

        if self.json {
            write_json(&report)?;
        } else {
            print!("{}", report.human_summary());
        }
        if report.has_operational_error() {
            return Err(eyre!(
                "one or more comparison runners failed; evidence retained at {}",
                report.comparison_path().display()
            ));
        }
        Ok(())
    }
}

fn write_json(value: &impl serde::Serialize) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, value)?;
    writeln!(stdout)?;
    Ok(())
}
