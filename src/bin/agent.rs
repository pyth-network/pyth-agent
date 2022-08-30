use pyth_agent::publisher::Publisher;

#[tokio::main]
async fn main() {
    Publisher::new().run().await;
}
