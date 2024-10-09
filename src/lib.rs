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
#![feature(option_get_or_insert_default)]
// #![feature(closure_lifetime_binder)]
// #![feature(specialization)]

// FIX: 批处理时CmdUnparsed的blobstring内容为乱码
//     1. 执行redis-benchmark -p 6379 -r 1000 -t set -n 100000 -P 1024 -q由概率出错
// 但执行redis-benchmark -p 6379 -r 1000 -t get -n 100000 -P 1024 -q不会出错
// 希望找到Get和Set的某个不同之处(它导致该错误)，硬编码Set命令的执行逻辑后，
// 错误仍然存在，执行lpush命令也有同样的问题。暂不清除错误的原因
//     2. 收集出错时的完整的网络数据包，并将其重新发送给服务，服务可以正确处理，
// 怀疑是redis-benchmark的问题，但是不确定

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
