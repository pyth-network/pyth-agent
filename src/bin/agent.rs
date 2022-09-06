use pyth_agent::publisher::Publisher;

#[tokio::main]
async fn main() {
    // TODO: run whole agent binary against a test setup, to check that it works
    // TODO: end-to-end integration tests
    Publisher::new().run().await;
}
