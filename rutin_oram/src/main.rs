#![feature(let_chains)]

mod proxy;
mod oram;

#[tokio::main]
async fn main() {
    crate::proxy::run().await;
}
