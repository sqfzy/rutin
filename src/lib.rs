#![feature(associated_type_defaults)]
#![feature(never_type)]
#![feature(try_blocks)]
#![warn(clippy::print_stdout)]
#![feature(decl_macro)]
#![feature(iter_advance_by)]
#![feature(let_chains)]
#![feature(once_cell_get_mut)]
#![feature(async_closure)]

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod cli;
pub mod cmd;
pub mod conf;
pub mod error;
pub mod frame;
pub mod persist;
pub mod server;
pub mod shared;
pub mod util;

pub type Key = shared::db::Str;
pub type Int = i128;
pub type Id = u64;
