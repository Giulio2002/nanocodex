# nanocodex-eval

`nanocodex-eval` is the owned benchmark lifecycle for Nanocodex. It composes
`nanocodex-agent` with `nanocodex-vm`, schedules task × agent × repetition
sweeps, runs canonical verifiers, and retains exact events, API evidence,
trajectories, timing, usage, and results.

Harbor-compatible JSONL and ATIF are output formats. Harbor does not run the
task, agent, VM, or verifier.

Local eval artifacts use one current schema. Resume requires an exact current
run manifest, and rerun requires the current invocation record. The crate does
not decode or upgrade old nanoeval run directories; start a new job instead.
The published-Harbor reader is a separate external-interoperability boundary.

## VM backend

The benchmark CLI always uses a microVM-backed task environment. The
Nanocodex driver stays in the host process while its workspace tools execute
in the guest; a stock Codex comparison binary executes inside a separate
matched guest. The native backend remains only as a small library test fixture.

Applications install VM execution through the eval facade:

```rust,ignore
use nanocodex_agent::{Nanocodex, OpenAi};
use nanocodex_eval::{
    Evaluator, Task, VmResources,
    vm::VmBackend,
};

let task = Task::load("terminal-bench/tasks/example")?;
let agent = Nanocodex::builder(OpenAi::new(std::env::var("OPENAI_API_KEY")?)?);
let resources = VmResources::builder(
    "target/debug/nanocodex",
    ".cache/vm/runtime.ext4",
)
    .task(task.clone())
    .prepare()
    .await?;
let backend = VmBackend::builder()
    .web_search(false)
    .retain_passed_rootfs(false)
    .build();
resources.configure(&backend).await?;

let (evaluator, events) = Evaluator::builder(agent)
    .output_directory(".nanocodex/evals")
    .max_concurrency(8)
    .max_memory_mb(32_768)
    .vm(backend)
    .build()?;
let _ = events;
let result = evaluator.task(task).await?;
```

`EvaluatorBuilder::vm` sets the durable environment identity to `micro_vm`
itself. A caller cannot install this backend while accidentally recording the
attempt as native.

`VmResources` owns OCI image materialization, public-network helper discovery,
task-to-environment mapping, and verifier-cache preparation. The detailed
`VmBackendConfiguration` and `VmEnvironment` types remain available under
`nanocodex_eval::vm` for custom runtimes, but the normal evaluator and
differential paths do not assemble them.

## Differential runner

`DifferentialEval` owns the matched two-arm lifecycle. The binary supplies the
already configured Nanocodex recipe, shared auth selection, one prepared VM
resource set, and executable identities:

```rust,ignore
use nanocodex_eval::{
    CodexAuth, DifferentialEval, ExecutableIdentity, Task, VmResources,
};

let task = Task::load("terminal-bench/tasks/example")?;
let vm = VmResources::builder("nanocodex", "runtime.ext4")
    .task(task.clone())
    .prepare()
    .await?;
let report = DifferentialEval::builder(task, nanocodex)
    .codex("codex-linux", CodexAuth::auth_file("~/.codex/auth.json"))
    .vm(vm)
    .thinking(thinking)
    .web_search(false)
    .nanocodex_executable(ExecutableIdentity::new("nanocodex", version))
    .build()?
    .run()
    .await?;
```

The library stages the Codex release, creates matched isolated backends, runs
both arms concurrently, streams the live divergence record, projects ATIF,
compares API event loops, and returns one typed retained report. Clap,
observability installation, process build metadata, terminal formatting, and
exit-code policy stay in the binary.

## CLI

Run Nanocodex normally over one task or a complete suite:

```sh
nanocodex eval \
  --suite /data/terminal-bench-2.1/tasks \
  --trials 5 \
  --concurrency 24 \
  --max-memory-mb 98304 \
  --thinking medium \
  --web-search false
```

Run one paired, concurrent `code_mode_only` comparison against a released
Linux Codex binary:

```sh
nanocodex eval diff \
  --task /data/terminal-bench-2.1/tasks/adaptive-rejection-sampler \
  --codex-bin /opt/codex/codex-x86_64-unknown-linux-musl \
  --thinking medium \
  --web-search false
```

Both commands use the same central CLI auth and model flags. Authentication
selection is, in order: `--api-key`, `--auth-file`, the default Codex auth
file, `OPENAI_API_KEY`, then the default auth-file path. Normal agent sessions
retain their standard defaults; eval defaults to medium thinking with web
search disabled unless the flags or a resumed invocation say otherwise.

The remaining command surface manages the same retained evidence:

```sh
nanocodex eval prepare --suite /data/terminal-bench-2.1/tasks
nanocodex eval task /data/terminal-bench-2.1/tasks/example --prompt
nanocodex eval inspect .nanocodex/evals/JOB --full
nanocodex eval compare .nanocodex/evals/JOB
nanocodex eval cleanup .nanocodex/evals/JOB --dry-run
nanocodex eval vm --help
```

`eval diff` writes interleaved live progress while both lanes execute and
retains the complete comparison, API exchanges, raw streams, derived ATIF,
verifier output, and final workspaces for offline reanalysis.

The ATIF summaries label their projection scope. Nanocodex lifecycle events
contain both the model-visible Code Mode call and its nested tools, while the
stock CLI stream exposes completed inner items. Those raw tool counts remain
available but are not treated as directly comparable. The API event-loop
comparison separately records the exact model-visible tool-call sequence for
both arms and uses that sequence for parity claims.
