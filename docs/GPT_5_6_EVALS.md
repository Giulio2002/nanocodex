# GPT-5.6 evaluation inventory and Nanocodex run plan

Research snapshot: 2026-07-27. This is preparation for Part 2; it does not
claim that Nanocodex has run these evaluations yet.

Primary sources:

- [OpenAI GPT-5.6 launch and benchmark tables](https://openai.com/index/gpt-5-6/)
- [GPT-5.6 system card](https://deploymentsafety.openai.com/gpt-5-6)
- [OpenAI destructive-actions section and Table 3](https://deploymentsafety.openai.com/gpt-5-6/avoiding-accidental-data-destructive-actions)
- [Artificial Analysis GPT-5.6 pre-release evaluation](https://artificialanalysis.ai/articles/gpt-5-6-has-landed)
- [ARC Prize verified GPT-5.6 results](https://arcprize.org/results/openai-gpt-5-6)
- [Agents' Last Exam](https://agents-last-exam.org/)
- [provisional Terminal-Bench 2.1 submission PR #174](https://github.com/harbor-framework/terminal-bench-2-1/pull/174)

## What should enter the first Nanocodex loop

The first useful sweep is not every benchmark in the launch post. It is the
smallest set that exercises Nanocodex's actual agent, VM, tool, and data
boundaries:

1. **Terminal-Bench 2.1** is the immediate end-to-end comparator. The evaluator
   already understands its task/verifier shape, and public GPT-5.6 trajectories
   can diagnose task-level mismatches.
2. **A destructive-workspace suite** should be a completion gate for the normal
   coding-agent boundary. It must score overwrite avoidance and task
   correctness independently and retain the exact pre-existing workspace
   mutation that the agent encountered.
3. **DeepSWE v1.1, SWE-Bench Pro, and SWE-Atlas-QnA** are the next repository
   engineering candidates. Their exact public revisions, harnesses, and
   licenses still need to be pinned before adapters are added.
4. **BrowseComp** is the first browsing candidate after a deterministic browser
   consumer exists. It also exercises multi-agent score/latency frontiers.
5. **Agents' Last Exam** is a later broad professional-workflow target. Its
   public site exposes tasks, traces, a leaderboard, and open code/data
   licensing, but many workflows depend on specialized applications outside
   the current VM slice.

The remaining official evaluations are useful coverage and plot references,
not a reason to expand Part 2 into browser, computer-use, biology, cyber, or
generic multiple-choice infrastructure prematurely.

## Runnable and access classification

The classification below is the adapter backlog for every evaluation in the
official launch table. `Runnable now` means this repository has a pinned task
and verifier path today. `Bounded adapter` means public data or a published
harness exists, but Nanocodex must pin and audit it before claiming a run.
`Published comparison` means only reported aggregate or partner results are
available to this project. `Unavailable/private` means the named dataset or
verifier is not public. `Out of scope` means the evaluation does not exercise
the Part 2 coding-agent/VM boundary.

Only Terminal-Bench 2.1 is currently pinned end to end:
dataset/task revision
`sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699`
with Harbor's task-package checksum, canonical verifier, raw verifier reward,
and leaderboard normalization retained separately. No other row below is
silently assigned that revision or substituted with a similarly named suite.

| Evaluation | Classification | Revision, license, or access blocker |
| --- | --- | --- |
| Agents' Last Exam | Bounded adapter | Public tasks, traces, and code/data are advertised as Apache-2.0/CC BY 4.0; the exact OpenAI subset and harness revision are not published or pinned here. |
| GDPval-AA v2 | Published comparison | OpenAI/Artificial Analysis publish results; the exact launch-run task and grader package is not available here. |
| Management Consulting Tasks (Internal) | Unavailable/private | OpenAI identifies the suite as internal; no public dataset or verifier revision is recorded. |
| Big Finance Bench | Published comparison | Rogo publishes aggregate results, not the exact launch-run dataset, rubric, and grader revision. |
| Artificial Analysis Intelligence Index v4.1 | Published comparison | Version 4.1 scores and cost estimates are public; the complete evaluated task/grader snapshot is not pinned here. |
| Artificial Analysis Coding Agent Index v1.1 | Published comparison | The composite result is public; its exact DeepSWE, Terminal-Bench v2, and SWE-Atlas-QnA run inputs are not one published Nanocodex-runnable package. |
| SWE-Bench Pro | Bounded adapter | A dataset/harness adapter requires a pinned public revision and verified redistribution/use terms; neither is recorded yet. |
| DeepSWE v1.1 | Bounded adapter | A dataset/harness adapter requires a pinned public revision and license audit; neither is recorded yet. |
| Terminal-Bench 2.1 | Runnable now | Pinned Harbor revision above; PR 174 is a debugging comparator, not ground truth. |
| GeneBench Pro | Out of scope | Science/health workflow and verifier are outside the coding-agent VM slice; no revision is pinned. |
| LifeSciBench | Out of scope | Science/health workflow is outside this slice; no exact launch revision is pinned. |
| MedChemBench (Internal) | Unavailable/private | OpenAI identifies the suite as internal. |
| HealthBench Professional | Out of scope | Medical evaluation is outside this slice and requires its own safety/grader review. |
| OSWorld 2.0 | Out of scope | Requires computer-use/browser machinery explicitly excluded from Part 2. |
| BrowseComp | Out of scope | Requires a deterministic browsing consumer explicitly deferred from Part 2. |
| BenchCAD | Out of scope | Requires CAD/computer-use tooling not owned by this coding-agent slice. |
| BenchCAD (Python tool) | Out of scope | Requires the BenchCAD task/grader and specialized Python tool contract. |
| Capture-the-Flag Challenges | Out of scope | Cyber capability evaluation is safety-gated and outside the normal coding workspace contract. |
| SEC-Bench Pro | Out of scope | Cyber exploitation work is outside this slice; no task/verifier revision is pinned. |
| ExploitBench | Out of scope | Cyber exploitation work is outside this slice; no task/verifier revision is pinned. |
| ExploitGym | Out of scope | Cyber exploitation work is outside this slice; no task/verifier revision is pinned. |
| Internal Research Debugging Evaluation | Unavailable/private | OpenAI identifies the suite as internal. |
| KernelGen 1P | Bounded adapter | A public/published harness may permit an adapter, but its exact launch dataset, verifier revision, and execution requirements are not pinned here. |
| NanoGPT | Bounded adapter | A bounded training-task adapter is possible after pinning the exact launch task, scoring policy, compute budget, and code revision. |
| PostTrainBench Lite | Bounded adapter | Requires a pinned public dataset/harness, verifier, and compute policy; none is recorded yet. |
| RSI Index | Published comparison | The launch result is public, but the exact task and grader package is not available here. |
| MMMU Pro (no tools) | Out of scope | Academic multimodal QA is not a coding-agent/VM evaluation. |
| MMMU Pro (with tools) | Out of scope | Multimodal tool-use harness is outside this slice. |
| gdp.pdf | Out of scope | PDF/multimodal professional-work evaluation is outside this slice. |
| GPQA Diamond | Out of scope | Academic question answering does not exercise the Part 2 workspace-agent boundary. |
| FrontierMath Tier 1–3 (v2) | Out of scope | Mathematics benchmark and grader are outside this slice. |
| FrontierMath Tier 4 (v2) | Out of scope | Mathematics benchmark and grader are outside this slice. |
| AutomationBench | Bounded adapter | Tool-use adapter requires a pinned public task/application harness and verifier revision; none is recorded yet. |
| Toolathlon | Bounded adapter | Tool-use adapter requires a pinned public service/task snapshot and verifier revision; none is recorded yet. |
| OpenAI MRCR v2, 8-needle, 256K–512K | Unavailable/private | The named OpenAI run dataset and grader revision are not published here. |
| OpenAI MRCR v2, 8-needle, 512K–1M | Unavailable/private | The named OpenAI run dataset and grader revision are not published here. |
| GraphWalks BFS 256K F1 | Bounded adapter | Requires the exact public generator, seed set, prompt format, and F1 grader revision used by the launch run. |
| GraphWalks BFS 1M F1 | Bounded adapter | Requires the exact public generator, seed set, prompt format, and F1 grader revision used by the launch run. |
| ARC-AGI-3 | Bounded adapter | ARC Prize publishes tasks/results under its own terms, but the exact OpenAI harness and per-environment policy are not pinned in this repository. |

The system-card destructive-action suite is `Unavailable/private`: OpenAI did
not publish its tasks or grader. A separately authored Nanocodex destructive
workspace suite may be publicly runnable, but it must carry a distinct dataset
name and revision and must never be reported as a reproduction. Other
system-card safety, health, biology, cyber, deployment-simulation, alignment,
and computer-use suites are either private/safety-gated or out of Part 2 scope
until their exact inputs, access terms, and verifiers are available.

## Official launch table

The following values transcribe the GPT-5.6 columns from OpenAI's launch table.
They are source anchors, not Nanocodex acceptance thresholds. A reproduced run
must also match the dataset revision, harness, reasoning effort, tool access,
agent topology, time limit, and score policy.

| Area | Evaluation | Sol | Sol Ultra | Terra | Luna |
| --- | --- | ---: | ---: | ---: | ---: |
| Professional | Agents' Last Exam | 52.7% | — | 50.4% | 50.3% |
| Professional | GDPval-AA v2 | 1,747.8 Elo | — | 1,593 Elo | 1,591.8 Elo |
| Professional | Management Consulting Tasks (Internal) | 43.2% | — | 37.2% | 35.4% |
| Professional | Big Finance Bench | 53% | — | 51% | 36% |
| Professional | Artificial Analysis Intelligence Index v4.1 | 58.9 | — | 55 | 51.2 |
| Coding | Artificial Analysis Coding Agent Index v1.1 | 80 | — | 77.4 | 74.6 |
| Coding | SWE-Bench Pro | 64.6% | — | 63.4% | 62.7% |
| Coding | DeepSWE v1.1 | 72.7% | — | 69.6% | 67.2% |
| Coding | Terminal-Bench 2.1 | 88.8% | 91.9% | 87.4% | 84.7% |
| Science and health | GeneBench Pro | 28.7% | — | 23.3% | 10.8% |
| Science and health | LifeSciBench | 59.9% | — | 56% | 51.2% |
| Science and health | MedChemBench (Internal) | 48.3% | — | 35% | 30.4% |
| Science and health | HealthBench Professional | 60.5% | — | 57.7% | 55.7% |
| Computer use | OSWorld 2.0 | 62.6% | — | 50.2% | 45.6% |
| Computer use | BrowseComp | 90.4% | 92.2% | 87.5% | 83.3% |
| Computer use | BenchCAD | 70.6% | — | 62.3% | 63.1% |
| Computer use | BenchCAD (Python tool) | 83.4% | — | 78.2% | 73.9% |
| Cybersecurity | Capture-the-Flag Challenges | 96.7% | — | 91.8% | 85.2% |
| Cybersecurity | SEC-Bench Pro | 71.2% | 74.3% | 57.7% | 48.9% |
| Cybersecurity | ExploitBench | 73.5% | — | 52.9% | 33.2% |
| Cybersecurity | ExploitGym | 33.7% | — | 23.2% | 12.4% |
| Self-improvement | Internal Research Debugging Evaluation | 68.3% | — | 67.8% | 50.8% |
| Self-improvement | KernelGen 1P | 61.1% | — | 49.2% | 22.4% |
| Self-improvement | NanoGPT | 9.69% | — | 14.5% | 1.66% |
| Self-improvement | PostTrainBench Lite | 50.3% | — | 51.5% | 29.6% |
| Self-improvement | RSI Index | 57.9% | — | 56.3% | 41.9% |
| Multimodal | MMMU Pro (no tools) | 83% | — | 80.7% | 78.4% |
| Multimodal | MMMU Pro (with tools) | 84.6% | — | 82% | 79.5% |
| Multimodal | gdp.pdf | 30.7% | — | 24.7% | 22.7% |
| Academic | GPQA Diamond | 94.6% | — | 92.9% | 92.3% |
| Academic | FrontierMath Tier 1–3 (v2) | 89% | — | 84.9% | 78.6% |
| Academic | FrontierMath Tier 4 (v2) | 83% | — | 68.3% | 58.5% |
| Tool use | AutomationBench | 18.1% | — | 15.2% | 14.9% |
| Tool use | Toolathlon | 58% | — | 53.1% | 53.4% |
| Long context | OpenAI MRCR v2, 8-needle, 256K–512K | 91.5% | — | 89.6% | 41.3% |
| Long context | OpenAI MRCR v2, 8-needle, 512K–1M | 73.8% | — | 72.5% | 41.3% |
| Long context | GraphWalks BFS 256K F1 | 90.7% | — | 76.9% | 81.3% |
| Long context | GraphWalks BFS 1M F1 | 77.1% | — | 71.2% | 51.2% |
| Abstract reasoning | ARC-AGI-3 | 7.78% | — | 0.8% | 0.18% |

The launch narrative reports 53.6 for Agents' Last Exam while its summary
table reports 52.7%. Until OpenAI publishes the run-level distinction, retain
both source values and do not silently choose one as canonical.

## Safety and agent-integrity evaluations

### Accidental destructive actions

OpenAI describes an internal coding-agent evaluation in which user changes and
data are adversarially injected into a task environment. The model must finish
the requested task without overwriting that protected state. Table 3 reports:

| Model | Avoidance only | Avoidance + correctness |
| --- | ---: | ---: |
| GPT-5.5 | 0.88 | 0.44 |
| GPT-5.6 Sol | 0.83 | 0.44 |
| GPT-5.6 Terra | 0.81 | 0.37 |
| GPT-5.6 Luna | 0.73 | 0.32 |

This is a particularly important schema test. One scalar `success` field is
insufficient: a run can preserve user state and fail the task, or pass a task
verifier after destroying protected state. Nanocodex should retain both grader
dimensions, the initial and injected workspace state, the final diff, all
destructive commands attempted, and whether authorization actually covered
each target.

The public system card does not publish the dataset. A Nanocodex analogue must
therefore be clearly labeled as a separate suite rather than presented as a
reproduction of OpenAI's number.

### Other system-card suites to track

The GPT-5.6 system card also discusses these evaluation families:

- challenging disallowed-content prompts, deployment simulation, image-input
  safety, user confirmation during computer use, jailbreak robustness, and
  prompt injection;
- HealthBench, adversarial mental-health simulations, and hallucinations in
  user-flagged cases;
- deployment simulations for misalignment in ChatGPT and internal coding
  traffic;
- chain-of-thought monitorability and controllability, destructive actions,
  confirmation consent, unsupported background-work promises, impossible
  coding tasks, flaky tools, and verbalized metagaming;
- first-person fairness;
- biology and chemistry capability evaluations, including Multimodal
  Troubleshooting Virology, ProtocolQA Open-Ended, TroubleshootingBench, AAV
  capsid packaging, protein binding, and DNA sequence design;
- cyber capability evaluations including CTF, CVE-Bench 1.0, VulnLMP,
  ExploitBench, ExploitGym, SEC-Bench Pro, and external work by Irregular and
  UK AISI;
- self-improvement evaluations including Internal Research Debugging,
  KernelGen 1P, NanoGPT, PostTrainBench Lite, MLE-Bench Revised, and external
  METR work; and
- external alignment, monitorability, and sandbagging evaluations by UK AISI
  and Apollo Research, plus automated and third-party safeguard red teaming.

Most are internal, safety-gated, application-specific, or externally operated.
They belong in the inventory so the result schema does not preclude them; they
are not all Part 2 adapters.

## Public third-party data

### Terminal-Bench 2.1

The detailed comparator, source jobs, task distributions, error-normalization
caveat, token totals, cost, and hardest tasks are recorded in
[`benchmarks/refactor_eval_baseline_2026-07-26.md`](../benchmarks/refactor_eval_baseline_2026-07-26.md#public-gpt-56-sol-terminal-bench-21-comparator).
The submission is provisional and uses fast-agent 0.9.24, not Nanocodex, so it
is diagnostic evidence rather than a score target.

### Artificial Analysis

Artificial Analysis reports its own pre-release measurements:

- Intelligence Index v4.1: Sol 59, Terra 55, Luna 51;
- estimated Intelligence Index cost per task: Sol $1.04, Terra $0.55,
  Luna $0.21;
- Sol averages roughly 15K output tokens per Intelligence Index task;
- Coding Agent Index: Sol 80, Terra 77, Luna 75, spanning DeepSWE,
  Terminal-Bench v2, and SWE-Atlas-QnA;
- AA-Briefcase: Sol's rubric score is 42% and analytical-quality Elo is 1,592;
  it also has the highest presentation Elo reported in that article; and
- the article reports a small AA-Omniscience accuracy uplift accompanied by a
  higher hallucination rate, without publishing those values in the text.

These results are useful plot references because they report cost per task,
output tokens per task, and harness identity alongside quality.

### ARC Prize

ARC Prize publishes verified scores for all 15 combinations of Sol, Terra, and
Luna with low through max reasoning. The max rows are:

| Model | ARC-AGI-1 | ARC-AGI-2 | ARC-AGI-3 |
| --- | ---: | ---: | ---: |
| Sol max | 96.5% | 92.5% | 7.78% |
| Terra max | 96.5% | 83.9% | 0.80% |
| Luna max | 88.0% | 59.5% | 0.18% |

Its public result page also gives per-task/per-environment scores and every
reasoning-effort row. That makes it a good future test of parameter-sweep
drilldown even though it is not a VM coding benchmark.

### Agents' Last Exam

Agents' Last Exam reports coverage across 55 sub-industries and more than 1,500
collected tasks, with public task, trace, leaderboard, and repository links.
Its data is CC BY 4.0 and code is Apache-2.0 according to the project site.
Pin the exact evaluated subset before comparing with OpenAI's launch number.

### Partner-reported evaluations in OpenAI's launch post

These are published observations, but most do not expose tasks or raw runs:

- Qodo: higher code-review F1 than GPT-5.5, about 3× fewer tokens per PR, and
  about 2× lower median latency.
- Rogo Big Finance Benchmark: +6.2 rubric-quality points and +3.6 answer-
  accuracy points over GPT-5.5; programmatic tool calling kept quality while
  using 24% fewer output tokens and finishing 28% faster.
- Clio: 14% fewer tokens with higher combined legal-workflow quality;
  programmatic tool calling reduced prompt tokens by 38% without quality loss.
- Balyasny: 1.72× greater token efficiency, leadership in three headline
  categories, and 88% on multi-hop finance tasks.
- Lovable: roughly 25% fewer steps, 35–48% fewer tool calls, and 15% fewer
  stuck runs while improving project success.
- Model ML FinBench: 20 client workflows and hundreds of decks; 39% fewer
  tokens per deck than Claude Fable with qualitative presentation gains.
- Triple Whale: 4.4/5 on a seven-task frontend benchmark versus 4.0 for
  GPT-5.5 and 3.5 for Claude 4.8.
- PlayCo: programmatic tool calling used 63.5% fewer total tokens and 50.1%
  fewer model turns than direct calls at comparable visual quality.
- Base44: 30 app-building conversations, 22% fewer input tokens and 23% fewer
  output tokens than GPT-5.5.
- Legora: improved or held steady on five of seven internal legal tasks.

Keep these as directional product evidence unless a provider publishes its
dataset, grader, harness version, and attempt-level records.

## Required retained data

One immutable attempt record should be sufficient to regenerate every useful
aggregate without rerunning a model. It needs:

- dataset, task, image, verifier, and scoring-policy identifiers and revisions;
- Nanocodex build, model slug, tier, reasoning effort/mode, service tier,
  prompt/instruction/tool configuration, seed, trial, and agent topology;
- VM identity, cold image/bootstrap time, warm startup time, queue wait, and
  isolated guest workspace;
- ordered model, reasoning, tool, and verifier events with raw terminal status;
- input, cache-write, cache-read, output, and reasoning token accounting per
  response and across the complete agent tree;
- price-catalog revision and separate input/cache-write/cache-read/output cost;
- wall time for the root agent plus summed and critical-path model/tool time;
- raw verifier rewards, benchmark-normalized rewards, correctness, safety or
  avoidance dimensions, refusal, timeout, and infrastructure-error classes;
  and
- complete artifacts needed to inspect or replay one failed task.

`aggregate.json` schema v4 is the typed boundary for this contract. Every
attempt row carries structured task/package/image/verifier, model/configuration,
build, VM, reward, exception, token, pricing, cost, latency, and artifact
identities. Build-, tool-, seed-, topology-, and VM-specific application
provenance is attached through `AggregateDataset::with_run_identity`, which
copies it into each row rather than leaving a non-self-contained sidecar.
Verifier-conditioned rates with no scored attempts serialize their rate and
confidence bounds as `null`, not a misleading zero.

Schema v4 makes partial-measurement semantics explicit. Missing billing
snapshots serialize token usage as `null`; a provider-reported true zero remains
a zero-valued sample; cancellation-recovered token and runtime measurements are
tagged `observed_lower_bound`. Terminal-complete token/cost summaries remain
separate from `observed_*_lower_bound` summaries, and agent-derived latency is
nullable when no runtime snapshot exists. This is an intentional experimental
schema break from the last shipped/reviewed v3, whose zero-filled missing
snapshots were ambiguous. Intermediate revision numbers used only by
uncommitted development drafts are intentionally not retained.

For multi-agent runs, follow OpenAI's published accounting convention as an
available view: latency comes from the root agent, while token and API-cost
totals include all agents. Also retain per-agent timings so alternative
critical-path and summed-work views remain possible. OpenAI's Ultra results use
four agents by default.

## Plot and drilldown completion gate

The evaluator is ready for tuning loops when the same retained fact table can
produce:

1. score versus estimated cost, latency, and output tokens, with Pareto-frontier
   highlighting;
2. multi-agent score/latency curves by agent count and topology;
3. model/tier/effort/harness comparisons with uncertainty and attempt counts;
4. pass@1, pass@k, per-task `k` distributions, and task-by-task paired deltas;
5. refusal, timeout, verifier failure, safety failure, and infrastructure-error
   breakdowns without silently converting one class into another;
6. cold image/bootstrap, scheduler queue, model, tool, and verifier latency
   decomposition;
7. token and cost composition, including cache reads and writes rather than
   treating all input tokens as one price, with missing, exact zero,
   terminal-complete, and observed-lower-bound samples kept distinct; and
8. a click-through from every aggregate point to its exact attempts,
   trajectories, tool calls, diffs, verifier output, and artifacts.

The allocation invariant is one retained VM per task across a parameter sweep.
Configurations run concurrently in isolated directories inside that VM; a VM
is not allocated per configuration. Every configuration still receives an
identically seeded private workspace and independent process group.

OpenAI notes that several launch-page latency and cost values are offline
estimates based on production behavior rather than direct wall-clock
measurements. Nanocodex must label measured and simulated values separately and
retain the inputs to either calculation.
