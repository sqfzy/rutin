use crate::{
    cmd::dispatch,
    conf::Conf,
    frame::RESP3Decoder,
    persist::rdb::{rdb_load, rdb_save},
    server::Handler,
    shared::Shared,
};
use anyhow::Result;
use bytes::BytesMut;
use serde::Deserialize;
use std::{os::unix::fs::MetadataExt, path::Path, sync::Arc, time::Duration};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tokio_util::codec::Decoder;

pub struct AOF {
    file: File,
    shared: Shared,
    conf: Arc<Conf>,
}

impl AOF {
    pub async fn new(shared: Shared, conf: Arc<Conf>, file_path: impl AsRef<Path>) -> Result<Self> {
        Ok(AOF {
            file: tokio::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(file_path)
                .await?,
            shared,
            conf,
        })
    }

    // 清空AOF文件
    pub async fn clear(&mut self) -> Result<()> {
        self.file.set_len(0).await?;
        Ok(())
    }

    async fn rewrite(&mut self) -> anyhow::Result<()> {
        let path = self.conf.aof.as_ref().unwrap().file_path.clone();
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
        rdb_save(&mut temp_file, self.shared.db(), true).await?;

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
        let aof_conf = self.conf.aof.as_ref().unwrap();

        // 为了避免在shutdown的时候，还有数据没有写入到文件中，shutdown时必须等待该函数执行完毕
        let shutdown = self.shared.shutdown().clone();
        let _delay_token = shutdown.delay_shutdown_token()?;

        let mut curr_aof_size = 0_u128; // 单位为byte
        let auto_aof_rewrite_min_size = (aof_conf.auto_aof_rewrite_min_size as u128) << 20;
        let wcmd_receiver = self
            .shared
            .wcmd_propagator()
            .to_aof
            .as_ref()
            .unwrap()
            .1
            .clone();

        // PERF: 使用io_uring
        match aof_conf.append_fsync {
            AppendFSync::Always => loop {
                tokio::select! {
                    _ = shutdown.wait_shutdown_triggered() => break,
                    wcmd = wcmd_receiver.recv() => {
                        let mut wcmd = wcmd?;

                        while let Some(w) = wcmd_receiver.try_recv()? {
                            wcmd.unsplit(w);
                        }

                        curr_aof_size += wcmd.len() as u128;
                        if curr_aof_size >= auto_aof_rewrite_min_size {
                            self.rewrite().await?;
                            curr_aof_size = 0;
                        }

                        self.file.write_all_buf(&mut wcmd).await?;
                        self.file.sync_data().await?;
                    }
                }
            },
            AppendFSync::EverySec => {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                let mut buffer = BytesMut::with_capacity(1024);

                loop {
                    tokio::select! {
                        _ = shutdown.wait_shutdown_triggered() => {
                            break
                        } ,
                        // 每隔一秒，同步文件
                        // PERF: 同步文件时会造成性能波动，也许应该新开一个线程来处理这个任务
                        _ = interval.tick() => {
                            self.file.write_all_buf(&mut buffer).await?;
                            self.file.sync_data().await?;
                        }
                        wcmd = wcmd_receiver.recv() => {
                            let mut wcmd = wcmd?;

                            while let Some(w) = wcmd_receiver.try_recv()? {
                                wcmd.unsplit(w);
                            }

                            curr_aof_size += wcmd.len() as u128;
                            if curr_aof_size >= auto_aof_rewrite_min_size {
                                self.rewrite().await?;
                                curr_aof_size = 0;
                            }

                            buffer.unsplit(wcmd);
                        }
                    }
                }
            }
            AppendFSync::No => loop {
                tokio::select! {
                    _ = shutdown.wait_shutdown_triggered() => break,
                    wcmd = wcmd_receiver.recv() => {
                        let mut wcmd = wcmd?;

                        while let Some(w) = wcmd_receiver.try_recv()? {
                            wcmd.unsplit(w);
                        }

                        curr_aof_size += wcmd.len() as u128;
                        if curr_aof_size >= auto_aof_rewrite_min_size {
                            self.rewrite().await?;
                            curr_aof_size = 0;
                        }

                        self.file.write_all_buf(&mut wcmd).await?;
                    }
                }
            },
        }

        while let Ok(Some(mut wcmd)) = wcmd_receiver.try_recv() {
            self.file.write_all_buf(&mut wcmd).await?;
        }

        self.file.sync_data().await?;
        self.rewrite().await?; // 最后再重写一次，确保数据完整
        tracing::info!("AOF file rewrited.");
        Ok(())
    }

    pub async fn load(&mut self) -> anyhow::Result<()> {
        let mut buf = BytesMut::with_capacity(self.file.metadata().await?.size() as usize);
        while self.file.read_buf(&mut buf).await? != 0 {}

        // 如果AOF文件以REDIS开头，说明是RDB与AOF混合文件，需要先加载RDB
        if buf.starts_with(b"REDIS") {
            rdb_load(&mut buf, self.shared.db(), false).await?;
        }

        let (mut handler, _) = Handler::new_fake_with(self.shared.clone(), None, None);
        let mut decoder = RESP3Decoder::default();
        while let Some(cmd_frame) = decoder.decode(&mut buf)? {
            dispatch(cmd_frame, &mut handler).await?;
        }

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
