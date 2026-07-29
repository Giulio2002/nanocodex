use std::{path::PathBuf, sync::Arc};

use clap::{Args, builder::NonEmptyStringValueParser};
use eyre::{Result, WrapErr, eyre};
use nanocodex::{Nanocodex, NanocodexBuilder, OpenAi, Thinking, Tools, oai::auth::OpenAiAuth};

#[derive(Args)]
pub(crate) struct AgentArgs {
    /// Explicit `OpenAI` API key override. Otherwise `OPENAI_API_KEY` is preferred.
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    api_key: Option<String>,

    /// Explicitly use `ChatGPT` authorization from this credential file.
    #[arg(long, env = "NANOCODEX_AUTH_FILE")]
    auth_file: Option<PathBuf>,

    /// Reasoning effort used by every fresh task agent. Fresh runs default to medium.
    #[arg(long, env = "OPENAI_REASONING_EFFORT")]
    thinking: Option<Thinking>,

    /// Allow the agent to search the public web. Disabled by default for eval integrity.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    web_search: Option<bool>,
}

#[derive(Clone)]
pub(crate) enum SharedAuth {
    ApiKey(Arc<str>),
    AuthFile(PathBuf),
}

impl AgentArgs {
    pub(crate) fn builder(self, thinking: Thinking, web_search: bool) -> Result<NanocodexBuilder> {
        let auth = Self::select_auth(self.api_key, self.auth_file, Self::environment_api_key()?)?;
        Self::builder_with_auth(auth.nanocodex()?, thinking, web_search)
    }

    pub(crate) fn shared_builder(
        self,
        thinking: Thinking,
        web_search: bool,
    ) -> Result<(NanocodexBuilder, SharedAuth)> {
        let auth = Self::select_auth(self.api_key, self.auth_file, Self::environment_api_key()?)?;
        let builder = Self::builder_with_auth(auth.nanocodex()?, thinking, web_search)?;
        Ok((builder, auth))
    }

    fn builder_with_auth(
        auth: OpenAiAuth,
        thinking: Thinking,
        web_search: bool,
    ) -> Result<NanocodexBuilder> {
        let tools = Tools::builder().web_search(web_search).build()?;
        let openai = OpenAi::new(auth)?;
        Ok(Nanocodex::builder(openai).thinking(thinking).tools(tools))
    }

    pub(crate) const fn thinking(&self) -> Option<Thinking> {
        self.thinking
    }

    pub(crate) const fn web_search(&self) -> Option<bool> {
        self.web_search
    }

    fn select_auth(
        explicit_api_key: Option<String>,
        auth_file: Option<PathBuf>,
        environment_api_key: Option<String>,
    ) -> Result<SharedAuth> {
        if let Some(api_key) = explicit_api_key {
            return Ok(SharedAuth::ApiKey(api_key.into()));
        }
        if let Some(auth_file) = auth_file {
            return Ok(SharedAuth::AuthFile(auth_file));
        }
        if let Some(api_key) = environment_api_key {
            return Ok(SharedAuth::ApiKey(api_key.into()));
        }
        Ok(SharedAuth::AuthFile(Self::default_auth_file()?))
    }

    fn environment_api_key() -> Result<Option<String>> {
        match std::env::var("OPENAI_API_KEY") {
            Ok(api_key) if api_key.trim().is_empty() => Ok(None),
            Ok(api_key) => Ok(Some(api_key)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error @ std::env::VarError::NotUnicode(_)) => {
                Err(error).wrap_err("OPENAI_API_KEY is not valid Unicode")
            }
        }
    }

    fn load_subscription_auth(auth_file: &std::path::Path) -> Result<OpenAiAuth> {
        nanocodex::oai::auth::load_chatgpt_auth(auth_file).map_err(|error| {
            eyre!(
                "ChatGPT authorization could not be loaded from {}: {error}. Run `nanocodex auth login`",
                auth_file.display()
            )
        })
    }

    fn default_auth_file() -> Result<PathBuf> {
        if let Some(path) = std::env::var_os("NANOCODEX_AUTH_FILE") {
            return Ok(PathBuf::from(path));
        }
        if let Some(path) = std::env::var_os("CODEX_HOME").filter(|path| !path.is_empty()) {
            return Ok(PathBuf::from(path).join("auth.json"));
        }
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .ok_or_else(|| {
                eyre!("home directory is unavailable; pass --auth-file or NANOCODEX_AUTH_FILE")
            })?;
        Ok(PathBuf::from(home).join(".codex/auth.json"))
    }
}

impl SharedAuth {
    fn nanocodex(&self) -> Result<OpenAiAuth> {
        match self {
            Self::ApiKey(api_key) => Ok(OpenAiAuth::api_key(Arc::clone(api_key))),
            Self::AuthFile(path) => AgentArgs::load_subscription_auth(path),
        }
    }
}
