pub mod aof;
pub mod rdb;

#[allow(async_fn_in_trait)]
pub trait AsyncPersist {
    async fn save_async(&mut self) -> anyhow::Result<()>;
    async fn load_async(&mut self) -> anyhow::Result<()>;
}

pub trait Persist {
    fn save(&mut self) -> anyhow::Result<()>;
    fn load(&mut self) -> anyhow::Result<()>;
}
