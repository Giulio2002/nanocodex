use std::{env, error::Error, path::PathBuf};

use nanocodex::{Nanocodex, OpenAi, oai::auth::OpenAiAuth};
use nanocodex_eval::harbor::Harbor;
use nanocodex_eval::{EvalEventKind, Evaluator, Task};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let task_directory = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("tasks/write-greeting"), PathBuf::from);
    let output_directory = env::args_os()
        .nth(2)
        .map_or_else(|| PathBuf::from(".nanocodex/evals"), PathBuf::from);
    let task = Task::load(task_directory)?;
    let (eval, events) = Evaluator::builder(Nanocodex::builder(OpenAi::new(auth()?)?))
        .output_directory(output_directory)
        .build()?;

    let harbor = Harbor::new(&eval)?.record(events.subscribe())?;
    let mut event_stream = events.subscribe();
    let observer_task = tokio::spawn(async move {
        let mut count = 0_u64;
        while let Some(event) = event_stream.recv().await? {
            count += 1;
            if matches!(
                event.kind,
                EvalEventKind::Completed(_) | EvalEventKind::Failed(_)
            ) {
                break;
            }
        }
        Ok::<_, nanocodex_eval::EvalEventStreamError>(count)
    });

    let result = eval.task(task).await?;
    let job = harbor.finish(vec![result.clone()]).await?;
    let event_count = observer_task.await??;

    println!("{}: {:?}", result.trial_name(), result.outcome());
    println!("Observed {event_count} typed events independently");
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
