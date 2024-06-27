#![feature(associated_type_defaults)]
#![feature(never_type)]
#![warn(clippy::print_stdout)]

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod cli;
pub mod cmd;
pub mod conf;
pub mod connection;
pub mod frame;
pub mod init;
pub mod persist;
pub mod server;
pub mod shared;
pub mod util;

pub use init::init;
pub use server::run;

// pub type Bytes = Vec<u8>;
pub type Key = bytes::Bytes;
// pub type RawCmd = bytes::Bytes;
pub type Int = i64;
pub type Id = u128;
pub type CmdFlag = u128;
