use std::{collections::BTreeMap, path::PathBuf, time::Duration};

const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_mins(5);

/// One MCP server transport and its lifecycle limits.
#[derive(Clone)]
pub struct McpServer {
    pub(crate) transport: McpTransport,
    pub(crate) description: Option<String>,
    pub(crate) startup_timeout: Duration,
    pub(crate) tool_timeout: Duration,
    pub(crate) enabled_tools: Option<Vec<String>>,
    pub(crate) disabled_tools: Vec<String>,
    pub(crate) unsupported_option: Option<&'static str>,
}

#[derive(Clone)]
pub(crate) enum McpTransport {
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        cwd: Option<PathBuf>,
    },
    LegacySse {
        url: String,
    },
    StreamableHttp {
        url: String,
        bearer: Option<SecretSource>,
        headers: BTreeMap<String, SecretSource>,
    },
}

#[derive(Clone)]
pub(crate) enum SecretSource {
    Value(String),
    Environment(String),
}

impl McpServer {
    /// Creates a local MCP server launched over stdio.
    #[must_use]
    pub fn stdio(command: impl Into<String>) -> Self {
        Self {
            transport: McpTransport::Stdio {
                command: command.into(),
                args: Vec::new(),
                env: BTreeMap::new(),
                cwd: None,
            },
            description: None,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            tool_timeout: DEFAULT_TOOL_TIMEOUT,
            enabled_tools: None,
            disabled_tools: Vec::new(),
            unsupported_option: None,
        }
    }

    /// Creates a remote MCP server using the Streamable HTTP transport.
    #[must_use]
    pub fn http(url: impl Into<String>) -> Self {
        Self {
            transport: McpTransport::StreamableHttp {
                url: url.into(),
                bearer: None,
                headers: BTreeMap::new(),
            },
            description: None,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            tool_timeout: DEFAULT_TOOL_TIMEOUT,
            enabled_tools: None,
            disabled_tools: Vec::new(),
            unsupported_option: None,
        }
    }

    /// Creates a remote MCP server using the legacy HTTP+SSE transport.
    #[must_use]
    pub fn sse(url: impl Into<String>) -> Self {
        Self {
            transport: McpTransport::LegacySse { url: url.into() },
            description: None,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            tool_timeout: DEFAULT_TOOL_TIMEOUT,
            enabled_tools: None,
            disabled_tools: Vec::new(),
            unsupported_option: None,
        }
    }

    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    #[must_use]
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    #[must_use]
    pub fn tool_timeout(mut self, timeout: Duration) -> Self {
        self.tool_timeout = timeout;
        self
    }

    #[must_use]
    pub fn enabled_tools(mut self, tools: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.enabled_tools = Some(tools.into_iter().map(Into::into).collect());
        self
    }

    #[must_use]
    pub fn disabled_tools(mut self, tools: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.disabled_tools = tools.into_iter().map(Into::into).collect();
        self
    }

    /// Adds one argument to a stdio server command.
    #[must_use]
    pub fn arg(mut self, argument: impl Into<String>) -> Self {
        match &mut self.transport {
            McpTransport::Stdio { args, .. } => args.push(argument.into()),
            McpTransport::LegacySse { .. } | McpTransport::StreamableHttp { .. } => {
                self.unsupported_option = Some("arg");
            }
        }
        self
    }

    /// Adds arguments to a stdio server command.
    #[must_use]
    pub fn args(mut self, arguments: impl IntoIterator<Item = impl Into<String>>) -> Self {
        match &mut self.transport {
            McpTransport::Stdio { args, .. } => {
                args.extend(arguments.into_iter().map(Into::into));
            }
            McpTransport::LegacySse { .. } | McpTransport::StreamableHttp { .. } => {
                self.unsupported_option = Some("args");
            }
        }
        self
    }

    /// Adds an explicit environment value to a stdio server process.
    #[must_use]
    pub fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        match &mut self.transport {
            McpTransport::Stdio { env, .. } => {
                env.insert(name.into(), value.into());
            }
            McpTransport::LegacySse { .. } | McpTransport::StreamableHttp { .. } => {
                self.unsupported_option = Some("env");
            }
        }
        self
    }

    /// Sets the working directory for a stdio server process.
    #[must_use]
    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        match &mut self.transport {
            McpTransport::Stdio { cwd: current, .. } => *current = Some(cwd.into()),
            McpTransport::LegacySse { .. } | McpTransport::StreamableHttp { .. } => {
                self.unsupported_option = Some("cwd");
            }
        }
        self
    }

    /// Sets a Streamable HTTP bearer token directly.
    #[must_use]
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        match &mut self.transport {
            McpTransport::StreamableHttp { bearer, .. } => {
                *bearer = Some(SecretSource::Value(token.into()));
            }
            McpTransport::Stdio { .. } | McpTransport::LegacySse { .. } => {
                self.unsupported_option = Some("bearer_token");
            }
        }
        self
    }

    /// Resolves a Streamable HTTP bearer token from an environment variable.
    #[must_use]
    pub fn bearer_token_env(mut self, variable: impl Into<String>) -> Self {
        match &mut self.transport {
            McpTransport::StreamableHttp { bearer, .. } => {
                *bearer = Some(SecretSource::Environment(variable.into()));
            }
            McpTransport::Stdio { .. } | McpTransport::LegacySse { .. } => {
                self.unsupported_option = Some("bearer_token_env");
            }
        }
        self
    }

    /// Adds a fixed Streamable HTTP header.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        match &mut self.transport {
            McpTransport::StreamableHttp { headers, .. } => {
                headers.insert(name.into(), SecretSource::Value(value.into()));
            }
            McpTransport::Stdio { .. } | McpTransport::LegacySse { .. } => {
                self.unsupported_option = Some("header");
            }
        }
        self
    }

    /// Resolves a Streamable HTTP header value from an environment variable.
    #[must_use]
    pub fn header_env(mut self, name: impl Into<String>, variable: impl Into<String>) -> Self {
        match &mut self.transport {
            McpTransport::StreamableHttp { headers, .. } => {
                headers.insert(name.into(), SecretSource::Environment(variable.into()));
            }
            McpTransport::Stdio { .. } | McpTransport::LegacySse { .. } => {
                self.unsupported_option = Some("header_env");
            }
        }
        self
    }

    pub(crate) fn includes_tool(&self, name: &str) -> bool {
        self.enabled_tools
            .as_ref()
            .is_none_or(|enabled| enabled.iter().any(|candidate| candidate == name))
            && !self
                .disabled_tools
                .iter()
                .any(|candidate| candidate == name)
    }
}

impl SecretSource {
    pub(crate) fn resolve(&self) -> Result<String, String> {
        match self {
            Self::Value(value) => Ok(value.clone()),
            Self::Environment(variable) => std::env::var(variable).map_err(|error| {
                format!("environment variable `{variable}` is unavailable: {error}")
            }),
        }
    }
}
