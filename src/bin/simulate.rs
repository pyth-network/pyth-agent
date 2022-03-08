use anyhow::Result;
use pyth_agent::simulator::Simulator;
use slog::{o, Drain, Logger};

#[tokio::main]
async fn main() -> Result<()> {
    let logger = Logger::root(
        slog_term::FullFormat::new(slog_term::PlainSyncDecorator::new(std::io::stdout()))
            .build()
            .fuse(),
        o!(),
    );

    let mut simulator = Simulator::new(logger.new(o!()));
    tokio::spawn(async move { simulator.run().await }).await?
}
