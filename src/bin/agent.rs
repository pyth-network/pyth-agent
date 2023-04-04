use {
    anyhow::{
        Context,
        Result,
    },
    clap::Parser,
    pyth_agent::agent::{
        config::Config,
        Agent,
    },
    slog::{
        error,
        o,
        Drain,
        Logger,
    },
    slog_async::Async,
    slog_envlogger::LogBuilder,
    std::{
        env,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse config early for logging channel capacity
    let config = Config::new(Arguments::parse().config).context("Could not parse config")?;

    // A plain slog drain that sits inside an async drain instance
    let inner_drain = LogBuilder::new(
        slog_term::FullFormat::new(slog_term::TermDecorator::new().stdout().build())
            .build()
            .fuse(), // Yell loud on logger internal errors
    )
    .parse(&env::var("RUST_LOG").unwrap_or("info".to_string()))
    .build();

    // The top level async drain
    let async_drain = Async::new(inner_drain)
        .chan_size(config.channel_capacities.logger_buffer)
        .build()
        .fuse();

    let logger = slog::Logger::root(async_drain, o!());

    if let Err(err) = start(config, logger.clone()).await {
        error!(logger, "{:#}", err; "error" => format!("{:?}", err));
        return Err(err);
    }

    Ok(())
}

async fn start(config: Config, logger: Logger) -> Result<()> {
    Agent::new(config).start(logger).await;
    Ok(())
}
