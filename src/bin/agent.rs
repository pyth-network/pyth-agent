use std::sync::Arc;

use anyhow::Result;
use pyth_agent::publisher::Publisher;

#[tokio::main]
async fn main() -> Result<()> {
    // TODO: config options to decide which role this user will have

    Ok(())
}

async fn start_publisher() {
    let publisher = Publisher::new();
    publisher.run().await;
    todo!();
}
