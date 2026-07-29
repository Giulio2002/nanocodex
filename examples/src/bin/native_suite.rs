use std::{env, error::Error, path::PathBuf};

use nanocodex::{Nanocodex, OpenAi, Thinking, oai::auth::OpenAiAuth};
use nanocodex_eval::harbor::Harbor;
use nanocodex_eval::{
    EvalAttemptOutcome, EvalEventKind, EvalEventStream, EvalEventStreamError, Evaluator, Task,
};

const K: usize = 5;
const TASKS: [&str; 3] = [
    "tasks/write-greeting",
    "tasks/uppercase-message",
    "tasks/extract-todos",
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let output_directory = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from(".nanocodex/evals"), PathBuf::from);
    let [first, second, third] = TASKS.map(Task::load);
    let (first, second, third) = (first?, second?, third?);
    let agent = Nanocodex::builder(OpenAi::new(auth()?)?).thinking(Thinking::Low);
    let (eval, events) = Evaluator::builder(agent)
        .output_directory(output_directory)
        .max_concurrency(TASKS.len() * K)
        .build()?;
    let harbor = Harbor::new(&eval)?.record(events.subscribe())?;
    let observer = tokio::spawn(observe(events.subscribe(), TASKS.len() * K));

    let (first, second, third) = tokio::try_join!(
        eval.task_n(first, K),
        eval.task_n(second, K),
        eval.task_n(third, K),
    )?;

    let results = first
        .into_iter()
        .chain(second)
        .chain(third)
        .collect::<Vec<_>>();
    let job = harbor.finish(results.clone()).await?;
    observer.await??;
    print_results(results);
    println!("Harbor job: {}", job.directory().display());
    Ok(())
}

fn auth() -> Result<OpenAiAuth, Box<dyn Error>> {
    match env::var("OPENAI_API_KEY") {
        Ok(api_key) if !api_key.trim().is_empty() => Ok(OpenAiAuth::api_key(api_key)),
        Ok(_) | Err(env::VarError::NotPresent) => {
            let auth_file = env::var_os("NANOCODEX_AUTH_FILE")
                .map(PathBuf::from)
                .or_else(|| {
                    env::var_os("CODEX_HOME").map(|path| PathBuf::from(path).join("auth.json"))
                })
                .or_else(|| {
                    env::var_os("HOME").map(|path| PathBuf::from(path).join(".codex/auth.json"))
                })
                .ok_or("set OPENAI_API_KEY or NANOCODEX_AUTH_FILE")?;
            Ok(nanocodex::oai::auth::load_chatgpt_auth(auth_file)?)
        }
        Err(error) => Err(error.into()),
    }
}

fn print_results(results: impl IntoIterator<Item = EvalAttemptOutcome>) {
    for outcome in results {
        match outcome {
            EvalAttemptOutcome::Scored(result) => println!(
                "{}: {:?} in {} ms",
                result.trial_name,
                result.status,
                result
                    .agent
                    .as_ref()
                    .map_or(0, |agent| agent.metadata.duration_ms),
            ),
            EvalAttemptOutcome::Unscored(failure) => println!(
                "{}: {:?} ({})",
                failure.trial_name, failure.exception.outcome, failure.exception.message,
            ),
        }
    }
}

async fn observe(mut events: EvalEventStream, expected: usize) -> Result<(), EvalEventStreamError> {
    let mut completed = 0;
    while completed < expected {
        let Some(event) = events.recv().await? else {
            break;
        };
        match &event.kind {
            EvalEventKind::AttemptStarted { .. } => {
                eprintln!("{}: started", event.trial_name);
            }
            EvalEventKind::Completed(result) => {
                completed += 1;
                eprintln!("{}: {:?}", event.trial_name, result.status);
            }
            EvalEventKind::Failed(failure) => {
                completed += 1;
                eprintln!(
                    "{}: Errored ({:?})",
                    event.trial_name, failure.exception.kind
                );
            }
            EvalEventKind::Agent(_)
            | EvalEventKind::VerifierStarted
            | EvalEventKind::VerifierOutput { .. }
            | EvalEventKind::VerifierCompleted(_) => {}
        }
    }
    Ok(())
}
