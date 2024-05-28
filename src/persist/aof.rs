use crate::{
    conf::Conf,
    persist::rdb::{rdb_load, rdb_save},
    shared::Shared,
    util::FakeStream,
    Connection,
};
use anyhow::Result;
use async_shutdown::ShutdownManager;
use bytes::BytesMut;
use serde::Deserialize;
use std::{os::unix::fs::MetadataExt, sync::Arc, time::Duration};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

pub struct AOF {
    file: File,
    shared: Shared,
    conf: Arc<Conf>,
    buffer: BytesMut,
    shutdown: ShutdownManager<()>,
}

impl AOF {
    pub async fn new(
        shared: Shared,
        conf: Arc<Conf>,
        shutdown: ShutdownManager<()>,
    ) -> Result<Self> {
        Ok(AOF {
            file: tokio::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(conf.aof.file_path.as_str())
                .await?,
            buffer: BytesMut::with_capacity(1024),
            shared,
            conf,
            shutdown,
        })
    }

    // 清空AOF文件
    pub async fn clear(&mut self) -> Result<()> {
        self.file.set_len(0).await?;
        Ok(())
    }

    async fn rewrite(&mut self) -> anyhow::Result<()> {
        let path = self.conf.aof.file_path.clone();
        let temp_path = format!("{}.tmp", path);
        let bak_path = format!("{}.bak", path);
        // 创建临时文件，先使用RDB格式保存数据
        let mut temp_file = tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&temp_path)
            .await?;

        // 将数据保存到临时文件
        rdb_save(&mut temp_file, self.shared.db(), false).await?;

        // 将数据保存到临时文件后，将原来的AOF文件关闭
        self.file = temp_file;

        // 将旧AOF文件备份
        tokio::fs::rename(&path, &bak_path).await?;
        // 将新AOF文件重命名为AOF文件
        tokio::fs::rename(&temp_path, &path).await?;

        Ok(())
    }
}

impl AOF {
    pub async fn save(&mut self) -> anyhow::Result<()> {
        let aof_conf = &self.conf.aof;
        // 为了避免在shutdown的时候，还有数据没有写入到文件中，shutdown时必须等待该函数执行完毕
        let _delay_token = self.shutdown.delay_shutdown_token()?;

        let mut count = 0;
        let max_count = 2 << aof_conf.max_record_exponent;
        let wcmd_receiver = self
            .shared
            .wcmd_propagator()
            .to_aof
            .as_ref()
            .unwrap()
            .1
            .clone();

        match aof_conf.append_fsync {
            AppendFSync::Always => loop {
                tokio::select! {
                    _ = self.shutdown.wait_shutdown_triggered() => break,
                    b = wcmd_receiver.recv_async() => {
                        self.buffer.extend(b?);
                        while let Ok(b) = wcmd_receiver.try_recv() {
                            self.buffer.extend(b);
                        }

                        self.file.write_all_buf(&mut self.buffer).await?;
                        self.file.sync_data().await?;
                    }
                }

                count += 1;
                if count >= max_count {
                    self.rewrite().await?;
                }
            },
            AppendFSync::EverySec => {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tokio::select! {
                        _ = self.shutdown.wait_shutdown_triggered() => {
                            break
                        } ,
                        // 每隔一秒，同步文件
                        _ = interval.tick() => {
                            self.file.sync_data().await?;
                            self.file.write_all_buf(&mut self.buffer).await?;
                            self.file.sync_data().await?;
                        }
                        b = wcmd_receiver.recv_async() => {
                            self.buffer.extend(b?);
                            while let Ok(b) = wcmd_receiver.try_recv() {
                                self.buffer.extend(b);
                            }
                        }
                    }

                    count += 1;
                    if count >= max_count {
                        self.rewrite().await?;
                    }
                }
            }
            AppendFSync::No => loop {
                tokio::select! {
                    _ = self.shutdown.wait_shutdown_triggered() => break,
                    b = wcmd_receiver.recv_async() => {
                        self.buffer.extend(b?);
                        while let Ok(b) = wcmd_receiver.try_recv() {
                            self.buffer.extend(b);
                        }

                        self.file.write_all_buf(&mut self.buffer).await?;
                    }
                }

                count += 1;
                if count >= max_count {
                    self.rewrite().await?;
                }
            },
        }

        while let Ok(b) = wcmd_receiver.try_recv() {
            self.buffer.extend(b);
        }
        self.file.write_all_buf(&mut self.buffer).await?;
        self.file.sync_data().await?;

        Ok(())
    }

    pub async fn load<'a>(&mut self, mut client: Connection<FakeStream<'a>>) -> anyhow::Result<()> {
        // 读取AOF文件内容并发送给AOF server
        let mut buf = BytesMut::with_capacity(self.file.metadata().await?.size() as usize);
        while self.file.read_buf(&mut buf).await? != 0 {}

        // 如果AOF文件以REDIS开头，说明是RDB与AOF混合文件，需要先加载RDB
        if buf.starts_with(b"REDIS") {
            rdb_load(&mut buf, self.shared.db(), false)?;
        }

        client.write_buf(&mut buf).await?;
        client.flush().await?;

        debug_assert!(buf.is_empty());

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename = "append_fsync")]
pub enum AppendFSync {
    Always,
    #[default]
    EverySec,
    No,
}
