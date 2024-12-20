#![feature(let_chains)]

mod proxy;

#[tokio::main]
async fn main() {
    crate::proxy::run().await;
}
