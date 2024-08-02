use crate::{
    cmd::dispatch,
    frame::Resp3Decoder,
    persist::rdb::{rdb_load, rdb_save},
    server::Handler,
    shared::{Message, Shared, SignalManager},
};
use anyhow::Result;
use bytes::BytesMut;
use flume::Receiver;
use serde::Deserialize;
use std::{os::unix::fs::MetadataExt, path::Path, time::Duration};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::codec::Decoder;

pub struct Aof {
    file: File,
    shared: Shared,
}

impl Aof {
    pub async fn new(shared: Shared, file_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Aof {
            file: tokio::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(file_path)
                .await?,
            shared,
        })
    }

    // 清空AOF文件
    pub async fn clear(&mut self) -> Result<()> {
        self.file.set_len(0).await?;
        Ok(())
    }

    async fn rewrite(&mut self) -> anyhow::Result<()> {
        let path = self.shared.conf().aof.as_ref().unwrap().file_path.clone();
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

impl Aof {
    pub async fn save_and_propagate_wcmd_to_replicas(
        &mut self,
        wcmd_receiver: Receiver<Message>,
    ) -> anyhow::Result<()> {
        let aof_conf = self.shared.conf().aof.as_ref().unwrap();

        // 为了避免在shutdown的时候，还有数据没有写入到文件中，shutdown时必须等待该函数执行完毕
        let signal_manager = self.shared.signal_manager().clone();
        let _delay_token = signal_manager.delay_shutdown_token()?;

        let mut curr_aof_size = 0_u128; // 单位为byte
        let auto_aof_rewrite_min_size = (aof_conf.auto_aof_rewrite_min_size as u128) << 20;

        let mut replica_handlers: Vec<Handler<TcpStream>> = Vec::new();

        match aof_conf.append_fsync {
            AppendFSync::Always => loop {
                tokio::select! {
                    _ = signal_manager.wait_special_signal() => break,
                    msg = wcmd_receiver.recv_async() => match msg.expect("wcmd_propagator should never close") {
                        Message::Wcmd(wcmd) => {
                            curr_aof_size += wcmd.len() as u128;

                            self.file.write_all(&wcmd).await?;
                            self.file.sync_data().await?;

                            if !replica_handlers.is_empty() {
                                for handler in replica_handlers.iter_mut() {
                                    handler.conn.write_all(&wcmd).await?;
                                }
                            }

                            if curr_aof_size >= auto_aof_rewrite_min_size {
                                self.rewrite().await?;
                                curr_aof_size = 0;
                            }

                        }
                        Message::AddReplica(handler) => {
                            replica_handlers.push(handler);
                        }
                    }
                }
            },
            AppendFSync::EverySec => {
                let mut interval = tokio::time::interval(Duration::from_secs(1));

                loop {
                    tokio::select! {
                        _ = signal_manager.wait_special_signal() => {
                            break
                        },
                        // 每隔一秒，同步文件
                        _ = interval.tick() => {
                            self.file.sync_data().await?;
                        }
                        msg = wcmd_receiver.recv_async() =>match msg.expect("wcmd_propagator should never close") {
                            Message::Wcmd(wcmd) => {
                                curr_aof_size += wcmd.len() as u128;

                                self.file.write_all(& wcmd).await?;

                                if !replica_handlers.is_empty() {
                                    for handler in replica_handlers.iter_mut() {
                                        handler.conn.write_all(&wcmd).await?;
                                    }
                                }

                                if curr_aof_size >= auto_aof_rewrite_min_size {
                                    self.rewrite().await?;
                                    curr_aof_size = 0;
                                }

                            }
                            Message::AddReplica(handler) => {
                                replica_handlers.push(handler);
                            }
                        }
                    }
                }
            }
            AppendFSync::No => loop {
                tokio::select! {
                    _ = signal_manager.wait_special_signal() => break,
                    msg = wcmd_receiver.recv_async() => match msg.expect("wcmd_propagator should never close") {
                        Message::Wcmd(wcmd) => {
                            curr_aof_size += wcmd.len() as u128;

                            self.file.write_all(&wcmd).await?;

                            if !replica_handlers.is_empty() {
                                for handler in replica_handlers.iter_mut() {
                                    handler.conn.write_all(&wcmd).await?;
                                }
                            }

                            if curr_aof_size >= auto_aof_rewrite_min_size {
                                self.rewrite().await?;
                                curr_aof_size = 0;
                            }
                        }
                        Message::AddReplica(handler) => {
                            replica_handlers.push(handler);
                        }
                    }
                }
            },
        }

        while let Ok(Message::Wcmd(wcmd)) = wcmd_receiver.try_recv() {
            self.file.write_all(&wcmd).await?;
        }

        self.file.sync_data().await?;
        self.rewrite().await?; // 最后再重写一次
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
        let mut decoder = Resp3Decoder::default();
        while let Some(cmd_frame) = decoder.decode(&mut buf)? {
            dispatch(cmd_frame, &mut handler).await?;
        }

        debug_assert!(buf.is_empty());

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase", rename = "append_fsync")]
pub enum AppendFSync {
    Always,
    #[default]
    EverySec,
    No,
}

#[tokio::test]
async fn aof_test() {
    use crate::{
        cmd::dispatch,
        frame::Resp3,
        server::{Handler, SHUTDOWN_SIGNAL},
        util::{get_test_shared, test_init},
    };
    use std::io::Write;

    test_init();

    const INIT_CONTENT: &[u8; 315] = b"*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000015\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000042\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000003\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000025\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000010\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000015\r\n$3\r\nVXK\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000004\r\n$3\r\nVXK\r\n";

    // 1. 测试写传播以及AOF save
    // 2. 测试AOF load

    let test_file_path = "tests/appendonly/test.aof";

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(test_file_path)
        .unwrap_or_else(|e| {
            eprintln!("Failed to open file: {}", e);
            std::process::exit(1);
        });
    file.write_all(INIT_CONTENT).unwrap();
    drop(file);

    let shared = get_test_shared();

    let mut aof = Aof::new(shared.clone(), test_file_path).await.unwrap();

    aof.load().await.unwrap();

    let wcmd_receiver = shared.wcmd_rx().cloned().unwrap();
    tokio::spawn(async move {
        aof.save_and_propagate_wcmd_to_replicas(wcmd_receiver)
            .await
            .unwrap();
    });

    let db = shared.db();
    // 断言AOF文件中的内容已经加载到内存中
    assert_eq!(
        db.get(&"key:000000000015".into())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .unwrap()
            .to_vec(),
        b"VXK"
    );
    assert_eq!(
        db.get(&"key:000000000003".into())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .unwrap()
            .to_vec(),
        b"VXK"
    );
    assert_eq!(
        db.get(&"key:000000000025".into())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .unwrap()
            .to_vec(),
        b"VXK"
    );

    let (mut handler, _) = Handler::new_fake_with(shared.clone(), None, None);

    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(test_file_path)
        .await
        .unwrap();
    file.set_len(0).await.unwrap(); // 清空AOF文件
    drop(file);

    let frames = vec![
        Resp3::new_array(vec![
            Resp3::new_blob_string("SET".into()),
            Resp3::new_blob_string("key:000000000015".into()),
            Resp3::new_blob_string("VXK".into()),
        ]),
        Resp3::new_array(vec![
            Resp3::new_blob_string("SET".into()),
            Resp3::new_blob_string("key:000000000003".into()),
            Resp3::new_blob_string("VXK".into()),
        ]),
        Resp3::new_array(vec![
            Resp3::new_blob_string("SET".into()),
            Resp3::new_blob_string("key:000000000025".into()),
            Resp3::new_blob_string("VXK".into()),
        ]),
    ];

    // 执行SET命令, handler会将命令写入AOF文件
    for f in frames {
        dispatch(f, &mut handler).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    shared
        .signal_manager()
        .trigger_shutdown(SHUTDOWN_SIGNAL)
        .unwrap();
}
