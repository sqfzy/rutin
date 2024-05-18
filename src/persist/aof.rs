use std::{
    io::{Read, Write},
    sync::Arc,
    time::Duration,
};

use crate::{
    conf::Conf,
    persist::{
        rdb::{rdb_load, rdb_save},
        Persist,
    },
    shared::Shared,
};
use anyhow::Result;
use async_shutdown::ShutdownManager;
use backon::{BlockingRetryable, ExponentialBuilder};
use bytes::BytesMut;
use serde::Deserialize;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

pub struct AOF {
    file: File,
    path: String,
    append_fsync: AppendFSync,
    max_record_exponent: usize,

    shared: Shared,
    buffer: BytesMut,
    socket: String,
    shutdown: ShutdownManager<()>,
}

impl AOF {
    pub async fn new(
        append_fsync: AppendFSync,
        shared: Shared,
        conf: &Arc<Conf>,
        shutdown: ShutdownManager<()>,
    ) -> Result<Self> {
        let path = conf.aof.file_path.clone();
        let server_addr = format!("{}:{}", conf.server.addr, conf.server.port);

        Ok(AOF {
            file: tokio::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path.as_str())
                .await?,
            path,
            append_fsync,
            max_record_exponent: conf.aof.max_record_exponent,

            buffer: BytesMut::with_capacity(1024),
            socket: server_addr,
            shared,
            shutdown,
        })
    }

    async fn rewrite(&mut self) -> anyhow::Result<()> {
        // 创建临时文件，先使用RDB格式保存数据
        let mut temp_file = tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(format!("{}.tmp", self.path))
            .await?;
        rdb_save(&mut temp_file, self.shared.db(), false).await?;

        // 使用新文件作为AOF文件
        self.file = temp_file;

        // 将旧AOF文件备份
        tokio::fs::rename(&self.path, format!("{}.bak", self.path)).await?;
        // 将新AOF文件重命名为AOF文件
        tokio::fs::rename(format!("{}.tmp", self.path), &self.path).await?;

        Ok(())
    }
}

impl Persist for AOF {
    async fn save(&mut self) -> anyhow::Result<()> {
        // 为了避免在shutdown的时候，还有数据没有写入到文件中，shutdown时必须等待该函数执行完毕
        let _delay_token = self.shutdown.delay_shutdown_token()?;

        let mut count = 0;
        let max_count = 2 << self.max_record_exponent;
        let wcmd_receiver = self
            .shared
            .wcmd_propagator()
            .to_aof
            .as_ref()
            .unwrap()
            .1
            .clone();

        // PERF: 存在性能问题
        match self.append_fsync {
            AppendFSync::Always => loop {
                tokio::select! {
                    _ = self.shutdown.wait_shutdown_triggered() => break,
                    b = wcmd_receiver.recv_async() => {
                        self.buffer.extend(b?.to_raw());
                        while let Ok(b) = wcmd_receiver.try_recv() {
                            self.buffer.extend(b.to_raw());
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
                            self.buffer.extend(b?.to_raw());
                            while let Ok(b) = wcmd_receiver.try_recv() {
                                self.buffer.extend(b.to_raw());
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
                        self.buffer.extend(b?.to_raw());
                        while let Ok(b) = wcmd_receiver.try_recv() {
                            self.buffer.extend(b.to_raw());
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
            self.buffer.extend(b.to_raw());
        }
        self.file.write_all_buf(&mut self.buffer).await?;
        self.file.sync_data().await?;

        Ok(())
    }

    async fn load(&mut self) -> anyhow::Result<()> {
        // AOF load: 2. 连接AOF server
        let mut client = (|| std::net::TcpStream::connect(&self.socket))
            .retry(&ExponentialBuilder::default())
            .call()?;

        // AOF load: 3. 读取AOF文件内容并发送给AOF server
        let mut buf = BytesMut::with_capacity(1024 * 8);
        while self.file.read_buf(&mut buf).await? != 0 {}

        // 如果AOF文件以REDIS开头，说明是RDB与AOF混合文件，需要先加载RDB
        if buf.starts_with(b"REDIS") {
            rdb_load(&mut buf, self.shared.db(), false)?;
        }

        client.write_all(&buf)?;
        client.flush()?;

        // AOF load: 4. 关闭写端(发送FIN)，通知对方数据已经发送完毕
        client.shutdown(std::net::Shutdown::Write)?;

        // AOF load: 6. 接收并丢弃AOF server的响应
        loop {
            let mut buf = [0; 1024 * 8];
            let n = client.read(&mut buf)?;
            if n == 0 {
                // AOF load: 9. 收到FIN，结束读取AOF server的响应
                break;
            }
        }

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
