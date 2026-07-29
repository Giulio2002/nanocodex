use std::{env, error::Error, path::PathBuf};

use nanocodex::{Nanocodex, OpenAi, Thinking, oai::auth::OpenAiAuth};
use nanocodex_eval::{Evaluator, Sweep, Task};

const K: u16 = 5;
type AnyError = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    let task = Task::load(
        env::args_os()
            .nth(1)
            .map_or_else(|| PathBuf::from("tasks/write-greeting"), PathBuf::from),
    )?;
    let auth = auth()?;
    let nanocodex = Nanocodex::builder(OpenAi::new(auth)?);
    let sweep = Sweep::builder()
        .task(task)
        .trials(K)
        .agent("thinking-low", nanocodex.clone().thinking(Thinking::Low))?
        .agent("thinking-high", nanocodex.clone().thinking(Thinking::High))?
        .build()?;

    println!("planned {} independent attempts", sweep.attempt_count());
    let (eval, _events) = Evaluator::builder(nanocodex)
        .output_directory(".nanocodex/evals/sweeps/thinking")
        .max_concurrency(sweep.attempt_count())
        .build()?;
    let results = eval.sweep(sweep).await?;
    for agent in ["thinking-low", "thinking-high"] {
        let completed = results
            .attempts()
            .iter()
            .filter(|attempt| attempt.agent().as_str() == agent)
            .count();
        println!("{agent}: {completed} completed attempts");
    }
    Ok(())
}

fn auth() -> Result<OpenAiAuth, AnyError> {
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
