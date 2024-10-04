#![warn(clippy::print_stdout)]
#![allow(internal_features)]
#![feature(associated_type_defaults)]
#![feature(never_type)]
#![feature(try_blocks)]
#![feature(decl_macro)]
#![feature(iter_advance_by)]
#![feature(let_chains)]
#![feature(once_cell_get_mut)]
#![feature(async_closure)]
#![feature(core_intrinsics)]
#![feature(vec_pop_if)]
// #![feature(closure_lifetime_binder)]
// #![feature(specialization)]

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod cli;
pub mod cmd;
pub mod conf;
pub mod error;
pub mod frame;
pub mod persist;
pub mod server;
pub mod shared;
pub mod util;

pub type Int = i128;
pub type Id = u64;
