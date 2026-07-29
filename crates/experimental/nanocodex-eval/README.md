# nanocodex-eval

`nanocodex-eval` is the owned benchmark lifecycle for Nanocodex. It composes
`nanocodex-agent` with `nanocodex-vm`, schedules task × agent × repetition
sweeps, runs canonical verifiers, and retains exact events, API evidence,
trajectories, timing, usage, and results.

Harbor-compatible JSONL and ATIF are output formats. Harbor does not run the
task, agent, VM, or verifier.

## VM backend

The benchmark CLI always uses a microVM-backed task environment. The
Nanocodex driver stays in the host process while its workspace tools execute
in the guest; a stock Codex comparison binary executes inside a separate
matched guest. The native backend remains only as a small library test fixture.

Applications install VM execution through the eval facade:

```rust,ignore
use nanocodex_agent::{Nanocodex, OpenAi};
use nanocodex_eval::{
    Evaluator, Task,
    vm::{VmBackend, VmBackendConfiguration, VmEnvironment},
};

let task = Task::load("terminal-bench/tasks/example")?;
let agent = Nanocodex::builder(OpenAi::new(std::env::var("OPENAI_API_KEY")?)?);
let backend = VmBackend::builder()
    .web_search(false)
    .retain_passed_rootfs(false)
    .build();
backend.configure(
    VmBackendConfiguration::builder("target/debug/nanocodex", "runtime.ext4")
        .environment(
            task.root(),
            VmEnvironment::new("task-root.ext4", "/app", "/bin/bash"),
        )
        .build(),
)?;

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
