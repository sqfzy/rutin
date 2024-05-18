use std::{
    io::{Read, Write},
    time::Duration,
};

use crate::{
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
    append_fsync: AppendFSync,
    shared: Shared,
    buffer: BytesMut,
    socket: String,
    shutdown: ShutdownManager<()>,
}

impl AOF {
    pub async fn new(
        path: &str,
        append_fsync: AppendFSync,
        server_addr: String,
        shared: Shared,
        shutdown: ShutdownManager<()>,
    ) -> Result<Self> {
        Ok(AOF {
            file: tokio::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(path)
                .await?,
            append_fsync,
            buffer: BytesMut::with_capacity(1024),
            socket: server_addr,
            shared,
            shutdown,
        })
    }

    // fn rewrite(&mut self) -> anyhow::Result<()> {
    //    let buf =  rdb_save(self.shared.db(), false);
    //
    // }
}

impl Persist for AOF {
    async fn save(&mut self) -> anyhow::Result<()> {
        // 为了避免在shutdown的时候，还有数据没有写入到文件中，shutdown时必须等待该函数执行完毕
        let _delay_token = self.shutdown.delay_shutdown_token()?;

        let mut count = 0;
        let wcmd_receiver = self.shared.wcmd_propagator().new_receiver();
        match self.append_fsync {
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
            },
        }

        while let Ok(b) = wcmd_receiver.try_recv() {
            self.buffer.extend(b);
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
        // TEST:
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
