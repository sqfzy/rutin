mod oram;

#[tokio::main]
async fn main() {
    oram::run().await;
}
