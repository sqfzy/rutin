pub mod aof;
pub mod rdb;

// TODO:
// 1. 多文件并行save/load
// 2. 支持maxmemory

// #[allow(async_fn_in_trait)]
// pub trait Persist {
//     async fn save(&mut self) -> anyhow::Result<()>;
//     async fn load(&mut self) -> anyhow::Result<()>;
// }
