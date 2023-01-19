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
async fn main() {
    let logger = slog::Logger::root(
        slog_async::Async::default(
            LogBuilder::new(
                slog_term::CompactFormat::new(slog_term::TermDecorator::new().stdout().build())
                    .build()
                    .fuse(),
            )
            .parse(&env::var("RUST_LOG").unwrap_or("info".to_string()))
            .build(),
        )
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
