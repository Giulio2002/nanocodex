use std::{env, error::Error, path::PathBuf};

use nanocodex::{
    Nanocodex, OpenAi, Thinking, Tools,
    oai::auth::OpenAiAuth,
    tools::mcp::{Mcp, McpServer},
};
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
    let default_tools = Tools::builder().build()?;
    let docs = Mcp::builder()
        .server(
            "docs",
            McpServer::http("https://developers.openai.com/mcp")
                .description("Search OpenAI developer documentation."),
        )
        .build()?;
    let tools_with_mcp = default_tools
        .clone()
        .into_builder()
        .provider(docs)
        .build()?;
    let sweep = Sweep::builder()
        .task(task)
        .trials(K)
        .agent(
            "without-mcp",
            nanocodex
                .clone()
                .thinking(Thinking::Low)
                .tools(default_tools),
        )?
        .agent(
            "with-docs-mcp",
            nanocodex
                .clone()
                .thinking(Thinking::Low)
                .tools(tools_with_mcp),
        )?
        .build()?;

    println!("planned {} independent attempts", sweep.attempt_count());
    let (eval, _events) = Evaluator::builder(nanocodex)
        .output_directory(".nanocodex/evals/sweeps/tools")
        .max_concurrency(sweep.attempt_count())
        .build()?;
    let results = eval.sweep(sweep).await?;
    for agent in ["without-mcp", "with-docs-mcp"] {
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
