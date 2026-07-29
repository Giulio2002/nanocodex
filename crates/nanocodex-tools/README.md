# Nanocodex Tools

Tool building blocks for `OpenAI` agents.

`nanocodex-tools` is useful without the Nanocodex agent loop. It provides the
caller-defined [`Tool`] contract, `tool` macro, heterogeneous [`Tools`]
registry, Code Mode runtime, standard workspace tools, and native MCP clients.
The shared contract types are defined by `nanocodex-oai-api` and re-exported
here so a tool implementation has one import surface.

## Define and select tools

The definition is the single source of truth for a tool's registry name. The
macro derives its input and output schemas from the function:

```rust
use nanocodex_tools::{Tools, tool};

#[tool(
    name = "deployment_region",
    description = "Return the production region for a named service.",
    parallel = true
)]
async fn deployment_region(service: String) -> Result<String, std::io::Error> {
    Ok(format!("{service}: us-west-2"))
}

# fn build() -> Result<(), nanocodex_tools::ToolsBuildError> {
let tools = Tools::builder()
    .without_defaults()
    .tool(deployment_region)
    .build()?;
# Ok(())
# }
```

Macro tools execute serially unless `parallel = true` explicitly marks their
local effects as safe to overlap. This does not change the provider wire
protocol.

Implement [`Tool`] directly when execution needs [`ToolContext`], freeform
input, multimodal [`ToolOutput`], or a custom definition:

```rust
use nanocodex_tools::{
    Tool, ToolContext, ToolDefinition, ToolInput, ToolOutput, ToolResult,
    contract::async_trait,
};
use serde_json::json;

struct DeploymentRegion;

#[async_trait]
impl Tool for DeploymentRegion {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "deployment_region",
            "Return the production region for a named service.",
            json!({
                "type": "object",
                "properties": { "service": { "type": "string" } },
                "required": ["service"],
                "additionalProperties": false
            }),
        )
    }

    async fn execute(
        &self,
        input: ToolInput,
        _context: ToolContext<'_>,
    ) -> ToolResult {
        let input: serde_json::Value = input.decode_json()?;
        let service = input["service"].as_str().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "service must be a string",
            )
        })?;
        Ok(ToolOutput::text(format!("{service}: us-west-2")))
    }
}
```

## Embed Code Mode in another host

[`hosted`] is the portable boundary for environments that own JavaScript
execution outside Rust. Implement [`hosted::CodeModeHost`] and pass it to
[`hosted::HostedTools`]; the adapter reuses the same execution, nested-call,
notification, observer, and owned-context types as native Code Mode. The
[`hosted`] module documentation includes a complete host implementation.

## MCP is native and always available

MCP is not a feature flag. Native consumers configure stdio or Streamable HTTP
servers and install the provider into the same registry:

```rust
use nanocodex_tools::{
    Tools,
    mcp::{Mcp, McpServer},
};

# fn build() -> Result<(), Box<dyn std::error::Error>> {
let mcp = Mcp::builder()
    .server(
        "company_docs",
        McpServer::stdio("company-docs-mcp").arg("--readonly"),
    )
    .build()?;

let tools = Tools::builder().provider(mcp).build()?;
# Ok(())
# }
```

Handshakes and discovery start with the owning runtime. `mcp::Mcp` exposes only
the provider-native `tool_search` initially. Search results contain loadable MCP
namespaces for direct model calls and also activate the matching definitions for
Code Mode, keeping large catalogs out of the initial tool list.

## Companion workspace runtimes

The default `native` feature remains the complete tools crate: registry, Code
Mode, MCP, web/image tools, macros, and standard workspace tools.

The narrower `workspace-runtime` feature exists only for process companions
such as `nanocodex-vm-guest`. With default features disabled, it exposes the
canonical [`workspace_runtime::WorkspaceToolRuntime`], standard tool
identities, and their shared contracts without linking OpenAI transports, Code
Mode/QuickJS, MCP, or HTTP clients. This is artifact separation, not a second
tool implementation or an alternate mode for normal native applications.

## Going lower level

The crate root intentionally contains only the normal registry path:
[`Tools`], `ToolsBuilder`, `ToolsBuildError`, [`Tool`], `tool`, and the
types required by the `Tool` methods, plus
[`ambient_sensitive_environment`](crate::ambient_sensitive_environment) for
deliberately restoring proxy-safe credential markers to tool subprocesses.

- [`contract`] contains complete model-visible inputs, outputs, errors, and
  retained wire forms.
- [`hosted`] contains the portable application-owned Code Mode boundary.
- [`runtime`] contains the stateful per-agent executor, built-in connection
  configuration, and dynamic-provider contract.
- [`code_mode`] contains cell results, notifications, and nested-tool updates.
- `mcp` contains transport configuration, authentication, discovery, login,
  and runtime control.
- `standard` contains reusable standard-tool identities and the host-owned
  plan implementation.
- [`workspace_runtime`] contains the retained canonical workspace-tool runtime
  used by process companions.
- [`image`] contains prompt-image preparation and tool-output normalization.
