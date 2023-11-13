use {
    anyhow::{
        anyhow,
        Context,
        Result,
    },
    clap::{
        Parser,
        ValueEnum,
    },
    pyth_agent::agent::{
        config::Config,
        Agent,
    },
    slog::{
        debug,
        error,
        o,
        Drain,
        Logger,
        PushFnValue,
        Record,
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
    config:     PathBuf,
    #[clap(short, long, default_value = "plain", value_enum)]
    /// Log flavor to use
    log_flavor: LogFlavor,

    #[clap(short = 'L', long)]
    /// Whether to print file:line info for each log statement
    log_locations: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum LogFlavor {
    /// Standard human-readable output
    Plain,
    /// Structured JSON output
    Json,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Arguments::parse();

    if !args.config.as_path().exists() {
        return Err(anyhow!("No config found under {:?}", args.config.to_str()));
    }

    println!("Loading config from {:?}", args.config.display());

    // Parse config early for logging channel capacity
    let config = Config::new(args.config).context("Could not parse config")?;

    let log_level = env::var("RUST_LOG").unwrap_or("info".to_string());

    // Build an async drain with a different inner drain depending on
    // log flavor choice in CLI
    let async_drain = match args.log_flavor {
        LogFlavor::Json => {
            // JSON output using slog-bunyan
            let inner_drain = LogBuilder::new(
                slog_bunyan::with_name(env!("CARGO_PKG_NAME"), std::io::stdout())
                    .build()
                    .fuse(),
            )
            .parse(&log_level)
            .build();

            Async::new(inner_drain)
                .chan_size(config.channel_capacities.logger_buffer)
                .build()
                .fuse()
        }
        LogFlavor::Plain => {
            // Plain, colored output usind slog-term
            let inner_drain = LogBuilder::new(
                slog_term::FullFormat::new(slog_term::TermDecorator::new().stdout().build())
                    .build()
                    .fuse(),
            )
            .parse(&log_level)
            .build();

            Async::new(inner_drain)
                .chan_size(config.channel_capacities.logger_buffer)
                .build()
                .fuse()
        }
    };

    let mut logger = slog::Logger::root(async_drain, o!());

    // Add location information to each log statement if enabled
    if args.log_locations {
        logger = logger.new(o!(
            "loc" => PushFnValue(
            move |r: &Record, ser| {
            ser.emit(format!("{}:{}", r.file(), r.line()))
            }
            ),
        ));
    }

    if let Err(err) = start(config, logger.clone()).await {
        error!(logger, "{}", err);
        debug!(logger, "error context"; "context" => format!("{:?}", err));
        return Err(err);
    }

    Ok(())
}

async fn start(config: Config, logger: Logger) -> Result<()> {
    Agent::new(config).start(logger).await;
    Ok(())
}
