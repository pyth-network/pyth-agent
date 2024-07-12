use {
    anyhow::{
        anyhow,
        Context,
        Result,
    },
    clap::Parser,
    opentelemetry::KeyValue,
    opentelemetry_otlp::WithExportConfig,
    pyth_agent::agent::{
        config::Config,
        Agent,
    },
    std::{
        io::IsTerminal,
        path::PathBuf,
        time::Duration,
    },
    tracing_subscriber::prelude::*,
};

#[derive(Parser, Debug)]
#[clap(author = "Pyth Data Association", version)]
/// Pyth Agent - publish data to the Pyth Network
struct Arguments {
    #[clap(short, long, default_value = "config/config.toml")]
    /// Path to configuration file
    config: PathBuf,

    #[clap(short = 'L', long)]
    /// Whether to print file:line info for each log statement
    log_locations: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize a Tracing Subscriber
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_file(false)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(true)
        .with_ansi(std::io::stderr().is_terminal());

    // Set up the OpenTelemetry exporter, defaults to 127.0.0.1:4317
    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_timeout(Duration::from_secs(3));

    // Set up the OpenTelemetry tracer
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .with_trace_config(opentelemetry_sdk::trace::config().with_resource(
            opentelemetry_sdk::Resource::new(vec![KeyValue::new("service.name", "pyth-agent")]),
        ))
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .map_err(|e| anyhow::anyhow!("Error initializing open telemetry: {}", e))?;

    // Set up the telemetry layer
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    let registry = tracing_subscriber::registry().with(telemetry);

    // Use the compact formatter if we're in a terminal, otherwise use the JSON formatter.
    if std::io::stderr().is_terminal() {
        registry.with(fmt_layer.compact()).init();
    } else {
        registry.with(fmt_layer.json()).init();
    }

    let args = Arguments::parse();

    if !args.config.as_path().exists() {
        return Err(anyhow!("No config found under {:?}", args.config.to_str()));
    }

    println!("Loading config from {:?}", args.config.display());

    // Parse config early for logging channel capacity
    let config = Config::new(args.config).context("Could not parse config")?;

    // Launch the application. If it fails, print the full backtrace and exit. RUST_BACKTRACE
    // should be set to 1 for this otherwise it will only print the top-level error.
    if let Err(err) = start(config).await {
        eprintln!("{}", err.backtrace());
        err.chain().for_each(|cause| eprintln!("{cause}"));
        return Err(err);
    }

    Ok(())
}

async fn start(config: Config) -> Result<()> {
    Agent::new(config).start().await;
    Ok(())
}
