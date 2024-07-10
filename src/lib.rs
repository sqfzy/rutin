#![feature(associated_type_defaults)]
#![feature(never_type)]
#![feature(try_blocks)]
#![warn(clippy::print_stdout)]

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod cli;
pub mod cmd;
pub mod conf;
pub mod connection;
pub mod error;
pub mod frame;
pub mod init;
pub mod persist;
pub mod server;
pub mod shared;
pub mod util;

pub type Key = bytes::Bytes;
pub type Int = i64;
pub type Id = u64;
pub type CmdFlag = u128;
