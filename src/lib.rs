#![feature(slice_split_once)]
#![feature(lazy_cell)]

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod cli;
pub mod cmd;
pub mod conf;
pub mod connection;
pub mod frame;
// mod frame2;
pub mod init;
pub mod persist;
pub mod shared;
// mod persist;
// pub mod replicaof;
// pub mod script;
mod server;
pub mod util;

pub use init::init;
pub use server::run;

// pub type Bytes = Vec<u8>;
pub type Key = bytes::Bytes;
pub type RawCmd = bytes::Bytes;
pub type Int = i64;
pub type Id = u128;

pub use connection::Connection;

use std::time::SystemTime;
use tokio::time::Instant;
static EPOCH: once_cell::sync::Lazy<Instant> = once_cell::sync::Lazy::new(|| {
    Instant::now()
        - SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
});
