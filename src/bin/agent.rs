use {
    anyhow::Result,
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
    std::path::PathBuf,
};

#[derive(Parser, Debug)]
#[clap(author = "Pyth Data Association", version)]
/// Pyth Agent - publish data to the Pyth Network
struct Arguments {
    #[clap(default_value = "config/config.toml")]
    /// Path to configuration file
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    let logger = slog::Logger::root(
        slog_async::Async::new(
            slog_term::CompactFormat::new(slog_term::TermDecorator::new().build())
                .build()
                .fuse(),
        )
        .build()
        .fuse(),
        o!(),
    );

    if let Err(err) = start(logger.clone()).await {
        error!(logger, "{:#}", err; "error" => format!("{:?}", err));
    }
}

async fn start(logger: Logger) -> Result<()> {
    Agent::new(Config::new(Arguments::parse().config)?)
        .start(logger)
        .await;
    Ok(())
}
