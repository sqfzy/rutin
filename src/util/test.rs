use tracing::Level;

pub fn test_init() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();
}

use bytes::BytesMut;
use flume::{Receiver, Sender};
use futures::FutureExt;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

struct Foo {
    a: usize,
}

impl Foo {
    fn foo() {}
}
