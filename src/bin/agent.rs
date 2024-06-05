use {
    anyhow::{
        anyhow,
        Context,
        Result,
    },
    clap::Parser,
    pyth_agent::agent::{
        config::Config,
        Agent,
    },
    std::{
        io::IsTerminal,
        path::PathBuf,
    },
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
    let fmt_builder = tracing_subscriber::fmt()
        .with_file(false)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(std::io::stderr().is_terminal());

    // Use the compact formatter if we're in a terminal, otherwise use the JSON formatter.
    if std::io::stderr().is_terminal() {
        tracing::subscriber::set_global_default(fmt_builder.compact().finish())?;
    } else {
        tracing::subscriber::set_global_default(fmt_builder.json().finish())?;
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
