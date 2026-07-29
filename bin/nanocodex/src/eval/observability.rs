use std::path::PathBuf;

use clap::{Args, ValueEnum, builder::NonEmptyStringValueParser};
use nanocodex_observability::{LogFormat, LogOutput, ObservabilityBuilder, ObservabilityGuard};

const DEFAULT_FILTER: &str = "warn,nanocodex_eval=info,nanocodex_vm=info,nanocodex=info,nanocodex_agent=info,nanocodex_oai_api=info,nanocodex_tools=info";

#[derive(Args)]
pub(crate) struct ObservabilityArgs {
    /// Tracing filter directive. Defaults to Evaluator and Nanocodex lifecycle spans at info.
    #[arg(
        long,
        env = "RUST_LOG",
        default_value = DEFAULT_FILTER,
        value_parser = NonEmptyStringValueParser::new()
    )]
    log_filter: String,

    /// Tracing filter applied only to exported OpenTelemetry spans.
    #[arg(
        long,
        env = "OTEL_LEVEL",
        default_value = DEFAULT_FILTER,
        value_parser = NonEmptyStringValueParser::new()
    )]
    otel_filter: String,

    /// Local tracing output format.
    #[arg(long, env = "NANOCODEX_EVAL_LOG_FORMAT", value_enum)]
    log_format: Option<LogFormatArg>,

    // Keep the temporary Nanoeval environment contract readable while callers
    // migrate to the Nanocodex-owned spelling.
    #[arg(
        long = "__legacy-nanoeval-log-format",
        env = "NANOEVAL_LOG_FORMAT",
        hide = true,
        value_enum
    )]
    legacy_log_format: Option<LogFormatArg>,

    /// Append local tracing output to this file instead of stderr.
    #[arg(long, env = "NANOCODEX_EVAL_LOG_FILE")]
    log_file: Option<PathBuf>,

    #[arg(
        long = "__legacy-nanoeval-log-file",
        env = "NANOEVAL_LOG_FILE",
        hide = true
    )]
    legacy_log_file: Option<PathBuf>,

    /// Export spans through OTLP/HTTP protobuf.
    #[arg(
        long,
        env = "OTEL_EXPORTER_OTLP_ENDPOINT",
        value_parser = NonEmptyStringValueParser::new()
    )]
    otel_endpoint: Option<String>,

    /// Deployment environment attached to exported spans.
    #[arg(
        long,
        env = "OTEL_DEPLOYMENT_ENVIRONMENT",
        default_value = "development",
        value_parser = NonEmptyStringValueParser::new()
    )]
    otel_environment: String,
}

#[derive(Clone, Copy, Default, ValueEnum)]
enum LogFormatArg {
    Pretty,
    #[default]
    Compact,
    Json,
}

impl ObservabilityArgs {
    pub(crate) fn install(
        self,
    ) -> Result<ObservabilityGuard, nanocodex_observability::ObservabilityError> {
        let output = self
            .log_file
            .or(self.legacy_log_file)
            .map_or(LogOutput::Stderr, LogOutput::File);
        let format = self
            .log_format
            .or(self.legacy_log_format)
            .unwrap_or_default();
        let mut builder = ObservabilityBuilder::new("nanocodex-eval", env!("CARGO_PKG_VERSION"))
            .filter(self.log_filter)
            .otel_filter(self.otel_filter)
            .format(format.into())
            .output(output)
            .environment(self.otel_environment);
        if let Some(endpoint) = self.otel_endpoint {
            builder = builder.otlp_endpoint(endpoint);
        }
        builder.install()
    }
}

impl From<LogFormatArg> for LogFormat {
    fn from(format: LogFormatArg) -> Self {
        match format {
            LogFormatArg::Pretty => Self::Pretty,
            LogFormatArg::Compact => Self::Compact,
            LogFormatArg::Json => Self::Json,
        }
    }
}
