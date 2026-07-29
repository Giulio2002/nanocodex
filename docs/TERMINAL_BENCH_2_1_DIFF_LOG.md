# Terminal-Bench 2.1 differential log

Started: 2026-07-28

This is the running task-by-task comparison between Nanocodex and the stock
Codex CLI on Terminal-Bench 2.1.

## Run protocol

- Run each task through `nanocodex eval diff` and the native `nanocodex-eval`
  lifecycle. Do not use Harbor as a runner.
- Give both agents the same task package, model, reasoning effort, web-search
  policy, canonical verifier, and matched Code Mode-only profile. Disable
  multi-agent execution and require both first model requests to expose exactly
  `exec` and `wait`.
- Start one attempt per agent concurrently in separate disposable
  environments so wall-clock progress is directly comparable.
- Before recording a result, inspect `comparison.json`, both derived ATIF
  trajectories, both evaluator JSONL logs, the raw Codex JSONL stream,
  verifier output, and final workspace.
- Keep raw artifacts outside Git and link their retained local paths from the
  table.
- Record a diagnosis only from retained evidence. Never modify a task or
  verifier to turn a failure into a pass.

## Dataset

- Name: `terminal-bench/terminal-bench-2-1`
- Revision:
  `sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699a`
- Tasks: 89
- Order: alphabetical by canonical task ID

The inventory below was checked against two independently retained complete
selections of this pinned revision. Their sets of 89 unique task IDs match
exactly. This metadata check does not make Harbor part of task execution.

## Runner validation

- 2026-07-28: the rebuilt native differential runner passed both arms on
  `nanoeval/write-greeting` with reward `1`.
- Both ATIF-v1.7 projections reconciled with their retained raw streams,
  verifier outputs, and final workspace bytes.
- Evidence:
  `/private/tmp/nanocodex-pr58-atif-final2.hpNk61/019faa7d-1250-7743-98bc-1675c9a85ebf/comparison.json`
- 2026-07-28: the pre-pinning VM-backed runner passed both arms on
  `terminal-bench/adaptive-rejection-sampler`. Both independent microVM
  attempts used `gpt-5.6-sol` at medium effort, began within 100 ms, used the
  same task image and canonical verifier, and passed all 9 verifier tests.
- The stock arm used the released static Linux binary `codex-cli 0.145.0`,
  SHA-256
  `57d79900fe95df2ab854adf581a28ec46d7442f07445032d86453a44b577dced`.
  Its release, credentials, cloud policy cache, and host CA bundle were staged
  into its guest without using Harbor.
- The comparison retained 137 ordered live lane records while both arms ran.
  The runner now also emits ten-second lane heartbeats during quiet model or
  command work.
- Evidence:
  `/private/tmp/nanocodex-tbench-2.1-diff-live/019faab6-b441-74f2-bde9-93f21f052121/comparison.json`
- This run is retained as exploratory runner evidence, not a matched-profile
  benchmark result: it predates the API capture and Code Mode-only catalog pin,
  so the stock arm's model-visible configuration was not established.
- 2026-07-28: the eval-owned host capture proxy was validated end to end with
  two independent microVM attempts on `nanoeval/write-greeting`. The runner
  automatically gave the stock Codex guest a per-attempt
  `http://host.containers.internal:<port>` base URL; the proxy forwarded that
  traffic to the ChatGPT Codex API using the workspace's standard TLS clients.
  No proxy executable or TLS provider was staged in the guest, and Harbor was
  not involved.
- Both arms used `gpt-5.6-sol` at medium effort and passed with reward `1`.
  The live lane exposed request and terminal-response boundaries while the
  agents ran. The retained Codex capture contains all 225 observed records
  (1,118,748 JSONL bytes), including two complete auxiliary
  `GET /models?client_version=0.145.0` exchanges and four complete Responses
  exchanges. The Nanocodex capture contains 53 Responses records (214,326 JSONL
  bytes) and four complete Responses exchanges. The normalized comparison
  correctly aligns Nanocodex request indexes 1–4 with Codex request indexes
  3–6 while preserving the auxiliary traffic in the raw Codex log.
- Evidence:
  `/private/tmp/nanocodex-eval-proxy-smoke.A7HIZe/019fab33-c4c8-7001-a28c-24fb3a4869b7/comparison.json`;
  `/private/tmp/nanocodex-eval-proxy-smoke.A7HIZe/019fab33-c4c8-7001-a28c-24fb3a4869b7/api-comparison.json`
- 2026-07-28: the same retained captures were reanalyzed in 0.25 seconds with
  no agent, model, VM, or verifier work. The normalized event-loop comparison
  masks volatile session, response, item, and call identities while preserving
  and checking their relationships. Both arms have 4/4 terminal Responses
  turns, 3 valid `previous_response_id` links, 2 valid tool-result links, no
  broken links, and one stable prompt-cache key.
- That stronger comparison changes the diagnosis for the pre-pinning
  `write-greeting` capture: the
  first actionable drift is not event-loop control flow. It is first-turn
  configuration. Nanocodex exposes only `exec` and `wait` and requests
  `reasoning.summary=auto`; stock Codex exposes its broader tool inventory and
  omits that summary field. Its generation request also contains additional
  apps, skills, collaboration, plugin, and environment context. Those inputs
  explain the later model/tool-shape drift and must remain classified
  separately from response-chain, retry, tool-result, and terminal-loop parity.
- 2026-07-28: the matched-profile VM self-check passed end to end on
  `nanoeval/write-greeting` with released `codex-cli 0.145.0`. Both arms used
  `gpt-5.6-sol` at medium effort, sent `reasoning.summary=auto`, exposed only
  `exec` and `wait`, made four complete Responses turns, preserved all three
  `previous_response_id` links and both tool-result links, and passed with
  reward `1`.
- Stock Codex fetched `/models` twice. The retained upstream bytes advertised
  `gpt-5.6-sol` with default medium effort, `tool_mode=code_mode`, and
  `multi_agent_version=v2`; the exact bytes delivered by the eval proxy kept
  the same model and effort but selected `code_mode_only` and `disabled`.
  `/models` is Codex client behavior metadata, not a substitution for the
  `model=gpt-5.6-sol` field sent to the Responses API.
- The stock event stream contains no error item. The first normalized
  event-loop divergence is now the `exec` description bytes under
  `tool_configuration`, rather than model, reasoning, tool-mode, multi-agent,
  chaining, or terminal-loop policy.
- Evidence:
  `/private/tmp/nanocodex-eval-code-mode-only-smoke/019fab68-5c2b-75a3-8d5a-7cebf1410146/comparison.json`;
  `/private/tmp/nanocodex-eval-code-mode-only-smoke/019fab68-5c2b-75a3-8d5a-7cebf1410146/api-comparison.json`
- 2026-07-28: the first matched-profile Terminal-Bench 2.1 rerun completed on
  `adaptive-rejection-sampler`. Both arms passed all 9 canonical verifier tests
  with reward `1`. The retained self-check observes `gpt-5.6-sol`, medium
  effort, `reasoning.summary=auto`, and only `exec`/`wait` in both first
  Responses requests.
- Nanocodex completed agent work in 314.1 seconds and used 316,327 total
  tokens; stock Codex completed in 363.2 seconds and used 447,126. Stock Codex
  therefore used 130,799 more tokens (+41.4%) and 49.1 more seconds (+15.6%).
- Both response chains are internally complete: Nanocodex has 12/12 terminal
  turns, 11 valid previous-response links, and 10 valid tool-result links;
  Codex has 15/15 terminal turns, 14 valid previous-response links, and 13
  valid tool-result links. Neither arm has a broken link and both keep one
  stable prompt-cache key.
- The raw API view detects two Nanocodex and three Codex polling-only model
  turns. The two install-wait polls align live on turns 4 and 5. Codex later
  takes a third polling turn and finishes with three unpaired Responses turns
  while adding a Laplace edge-case fix and static/usage checks after both
  implementations already passed their own formal suites.
- Evidence:
  `/private/tmp/nanocodex-tbench-2.1-diff-code-mode-only/019fab6b-4421-73a0-8f59-a14809735a9b/comparison.json`;
  `/private/tmp/nanocodex-tbench-2.1-diff-code-mode-only/019fab6b-4421-73a0-8f59-a14809735a9b/api-comparison.json`;
  `/private/tmp/nanocodex-tbench-2.1-diff-code-mode-only/019fab6b-4421-73a0-8f59-a14809735a9b/progress.jsonl`

## Task 1 diagnosis: `adaptive-rejection-sampler`

- The pin matched model/runtime policy, not the complete model-visible prompt.
  Both arms used `gpt-5.6-sol`, medium effort, `reasoning.summary=auto`,
  Code Mode-only, disabled multi-agent execution, and visible `exec`/`wait`
  tools. The warm-up request's first remaining difference is the `exec`
  description: 9,274 bytes for Nanocodex and 14,209 for stock Codex.
- The first real generation request has additional controlled input drift.
  Stock Codex receives extra apps (646 bytes), skills (2,932 bytes), and
  recommended-plugin (329 bytes) context. Its 295-byte environment block also
  reports `2026-07-29` in UTC, while Nanocodex's 341-byte block reports
  `2026-07-28` in America/Los_Angeles. The benchmark task text is identical.
  This run is therefore profile-matched but not prompt-byte-matched.
- Despite that prompt drift, both agents take the same early path: inspect the
  workspace, discover R is absent, start the same `apt-get` installation, and
  spend two model turns polling it. The third Codex polling-only turn occurs
  later while waiting for a post-fix R test; it is not an extra installation
  poll.
- Nanocodex separates the initial solution into an 89.1-second detailed
  planning turn and a 131.7-second implementation turn. Stock Codex produces
  its initial implementation in one 173.8-second turn. Stock Codex's formal
  suite passes at 242.5 seconds, 40.6 seconds before Nanocodex's formal suite
  passes at 283.1 seconds.
- Nanocodex then runs one compact supplemental check covering gamma, a bounded
  uniform, formula input, zero samples, parsing, and the sample file, and ends
  its agent turn 33.7 seconds after its first passing suite. Stock Codex instead
  checks gamma, beta, Laplace, truncated normal, and an analytic hull integral.
  The Laplace check exposes a real zero-width tangent edge case at a
  non-differentiable mode; Codex patches it, reruns the suite, performs static
  checks, repairs its own mistaken `tools::checkUsageEnv` namespace, updates
  its plan, and ends 123.7 seconds after its first passing suite.
- The three Codex-only tail Responses turns consume 123,546 tokens, which is
  94.5% of the total 130,799-token excess. Removing those tail turns leaves
  323,580 Codex tokens versus 316,327 Nanocodex tokens, only 7,253 (+2.3%)
  apart. The large total-token delta is therefore repeated retained context
  during post-pass validation/finalization, not a response-chain or retry
  failure.
- Both final workspaces pass all 9 canonical verifier tests. Stock Codex adds
  robustness for a valid Laplace density that Nanocodex's supplemental tests
  do not exercise; that robustness is beyond what changed the benchmark score.
  The primary observed behavioral difference is stopping/validation policy,
  seeded by non-identical first-turn context, rather than the Code Mode event
  loop getting stuck.

## Results

| # | Task | Model / effort | Nanocodex | Codex | Classification | Evidence | Diagnosis | Next action |
| ---: | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `adaptive-rejection-sampler` | `gpt-5.6-sol` / medium; Code Mode-only | pass (`1.0`); 314.1s agent; 316,327 tokens; 11 generation turns; 2 API-detected poll turns | pass (`1.0`); 363.2s agent; 447,126 tokens; 14 generation turns; 3 API-detected poll turns | both passed; model/tool profile matched; prompt context drift | [`comparison.json`](/private/tmp/nanocodex-tbench-2.1-diff-code-mode-only/019fab6b-4421-73a0-8f59-a14809735a9b/comparison.json); [`api-comparison.json`](/private/tmp/nanocodex-tbench-2.1-diff-code-mode-only/019fab6b-4421-73a0-8f59-a14809735a9b/api-comparison.json); [`progress.jsonl`](/private/tmp/nanocodex-tbench-2.1-diff-code-mode-only/019fab6b-4421-73a0-8f59-a14809735a9b/progress.jsonl) | Both passed all 9 tests. Stock Codex receives extra apps/skills/plugin context, reaches its first passing formal suite 40.6s earlier, then spends 90.0s more post-pass agent time finding and fixing a real Laplace edge case and performing static/final checks. Three Codex-only tail turns explain 94.5% of its 130,799-token excess. Polling and response chains are healthy. | Make first-generation context byte-equivalent, then rerun this task before using `bn-fit-modify` to judge whether the extra Codex validation tail recurs. |
| 2 | `bn-fit-modify` |  |  |  | pending |  |  |  |
| 3 | `break-filter-js-from-html` |  |  |  | pending |  |  |  |
| 4 | `build-cython-ext` |  |  |  | pending |  |  |  |
| 5 | `build-pmars` |  |  |  | pending |  |  |  |
| 6 | `build-pov-ray` |  |  |  | pending |  |  |  |
| 7 | `caffe-cifar-10` |  |  |  | pending |  |  |  |
| 8 | `cancel-async-tasks` |  |  |  | pending |  |  |  |
| 9 | `chess-best-move` |  |  |  | pending |  |  |  |
| 10 | `circuit-fibsqrt` |  |  |  | pending |  |  |  |
| 11 | `cobol-modernization` |  |  |  | pending |  |  |  |
| 12 | `code-from-image` |  |  |  | pending |  |  |  |
| 13 | `compile-compcert` |  |  |  | pending |  |  |  |
| 14 | `configure-git-webserver` |  |  |  | pending |  |  |  |
| 15 | `constraints-scheduling` |  |  |  | pending |  |  |  |
| 16 | `count-dataset-tokens` |  |  |  | pending |  |  |  |
| 17 | `crack-7z-hash` |  |  |  | pending |  |  |  |
| 18 | `custom-memory-heap-crash` |  |  |  | pending |  |  |  |
| 19 | `db-wal-recovery` |  |  |  | pending |  |  |  |
| 20 | `distribution-search` |  |  |  | pending |  |  |  |
| 21 | `dna-assembly` |  |  |  | pending |  |  |  |
| 22 | `dna-insert` |  |  |  | pending |  |  |  |
| 23 | `extract-elf` |  |  |  | pending |  |  |  |
| 24 | `extract-moves-from-video` |  |  |  | pending |  |  |  |
| 25 | `feal-differential-cryptanalysis` |  |  |  | pending |  |  |  |
| 26 | `feal-linear-cryptanalysis` |  |  |  | pending |  |  |  |
| 27 | `filter-js-from-html` |  |  |  | pending |  |  |  |
| 28 | `financial-document-processor` |  |  |  | pending |  |  |  |
| 29 | `fix-code-vulnerability` |  |  |  | pending |  |  |  |
| 30 | `fix-git` |  |  |  | pending |  |  |  |
| 31 | `fix-ocaml-gc` |  |  |  | pending |  |  |  |
| 32 | `gcode-to-text` |  |  |  | pending |  |  |  |
| 33 | `git-leak-recovery` |  |  |  | pending |  |  |  |
| 34 | `git-multibranch` |  |  |  | pending |  |  |  |
| 35 | `gpt2-codegolf` |  |  |  | pending |  |  |  |
| 36 | `headless-terminal` |  |  |  | pending |  |  |  |
| 37 | `hf-model-inference` |  |  |  | pending |  |  |  |
| 38 | `install-windows-3.11` |  |  |  | pending |  |  |  |
| 39 | `kv-store-grpc` |  |  |  | pending |  |  |  |
| 40 | `large-scale-text-editing` |  |  |  | pending |  |  |  |
| 41 | `largest-eigenval` |  |  |  | pending |  |  |  |
| 42 | `llm-inference-batching-scheduler` |  |  |  | pending |  |  |  |
| 43 | `log-summary-date-ranges` |  |  |  | pending |  |  |  |
| 44 | `mailman` |  |  |  | pending |  |  |  |
| 45 | `make-doom-for-mips` |  |  |  | pending |  |  |  |
| 46 | `make-mips-interpreter` |  |  |  | pending |  |  |  |
| 47 | `mcmc-sampling-stan` |  |  |  | pending |  |  |  |
| 48 | `merge-diff-arc-agi-task` |  |  |  | pending |  |  |  |
| 49 | `model-extraction-relu-logits` |  |  |  | pending |  |  |  |
| 50 | `modernize-scientific-stack` |  |  |  | pending |  |  |  |
| 51 | `mteb-leaderboard` |  |  |  | pending |  |  |  |
| 52 | `mteb-retrieve` |  |  |  | pending |  |  |  |
| 53 | `multi-source-data-merger` |  |  |  | pending |  |  |  |
| 54 | `nginx-request-logging` |  |  |  | pending |  |  |  |
| 55 | `openssl-selfsigned-cert` |  |  |  | pending |  |  |  |
| 56 | `overfull-hbox` |  |  |  | pending |  |  |  |
| 57 | `password-recovery` |  |  |  | pending |  |  |  |
| 58 | `path-tracing` |  |  |  | pending |  |  |  |
| 59 | `path-tracing-reverse` |  |  |  | pending |  |  |  |
| 60 | `polyglot-c-py` |  |  |  | pending |  |  |  |
| 61 | `polyglot-rust-c` |  |  |  | pending |  |  |  |
| 62 | `portfolio-optimization` |  |  |  | pending |  |  |  |
| 63 | `protein-assembly` |  |  |  | pending |  |  |  |
| 64 | `prove-plus-comm` |  |  |  | pending |  |  |  |
| 65 | `pypi-server` |  |  |  | pending |  |  |  |
| 66 | `pytorch-model-cli` |  |  |  | pending |  |  |  |
| 67 | `pytorch-model-recovery` |  |  |  | pending |  |  |  |
| 68 | `qemu-alpine-ssh` |  |  |  | pending |  |  |  |
| 69 | `qemu-startup` |  |  |  | pending |  |  |  |
| 70 | `query-optimize` |  |  |  | pending |  |  |  |
| 71 | `raman-fitting` |  |  |  | pending |  |  |  |
| 72 | `regex-chess` |  |  |  | pending |  |  |  |
| 73 | `regex-log` |  |  |  | pending |  |  |  |
| 74 | `reshard-c4-data` |  |  |  | pending |  |  |  |
| 75 | `rstan-to-pystan` |  |  |  | pending |  |  |  |
| 76 | `sam-cell-seg` |  |  |  | pending |  |  |  |
| 77 | `sanitize-git-repo` |  |  |  | pending |  |  |  |
| 78 | `schemelike-metacircular-eval` |  |  |  | pending |  |  |  |
| 79 | `sparql-university` |  |  |  | pending |  |  |  |
| 80 | `sqlite-db-truncate` |  |  |  | pending |  |  |  |
| 81 | `sqlite-with-gcov` |  |  |  | pending |  |  |  |
| 82 | `torch-pipeline-parallelism` |  |  |  | pending |  |  |  |
| 83 | `torch-tensor-parallelism` |  |  |  | pending |  |  |  |
| 84 | `train-fasttext` |  |  |  | pending |  |  |  |
| 85 | `tune-mjcf` |  |  |  | pending |  |  |  |
| 86 | `video-processing` |  |  |  | pending |  |  |  |
| 87 | `vulnerable-secret` |  |  |  | pending |  |  |  |
| 88 | `winning-avg-corewars` |  |  |  | pending |  |  |  |
| 89 | `write-compressor` |  |  |  | pending |  |  |  |
