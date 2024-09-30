use crate::{
    cmd::dispatch,
    frame::Resp3Decoder,
    persist::rdb::{rdb_load, rdb_save},
    server::Handler,
    shared::{Letter, Shared, AOF_ID, SET_MASTER_ID},
};
use bytes::BytesMut;
use event_listener::listener;
use serde::Deserialize;
use std::{os::unix::fs::MetadataExt, path::Path, time::Duration};
use tokio::{
    fs::File,
    io::{self, AsyncReadExt, AsyncWriteExt},
};
use tokio_util::codec::Decoder;
use tracing::info;

pub struct Aof {
    pub file: File,
    pub shared: Shared,
}

impl Aof {
    pub async fn new(shared: Shared, file_path: impl AsRef<Path>) -> io::Result<Self> {
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
    pub async fn clear(&mut self) -> io::Result<()> {
        self.file.set_len(0).await?;
        Ok(())
    }

    pub async fn rewrite(&mut self) -> anyhow::Result<()> {
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
        let rdb_data = rdb_save(self.shared.db(), true).await?;

        temp_file.write_all(&rdb_data).await?;

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
    pub async fn save(&mut self) -> anyhow::Result<()> {
        let aof_conf = self.shared.conf().aof.as_ref().unwrap();
        let post_office = self.shared.post_office();

        let (aof_outbox, aof_inbox) = post_office.new_mailbox_with_special_id(AOF_ID);

        post_office.set_aof_outbox(Some(aof_outbox)).await;

        let mut curr_aof_size = 0_u128; // 单位为byte
        let auto_aof_rewrite_min_size = aof_conf.auto_aof_rewrite_min_size;

        let set_master_outbox = post_office.get_outbox(SET_MASTER_ID);

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        match aof_conf.append_fsync {
            AppendFSync::Always => loop {
                match aof_inbox.recv_async().await {
                    Letter::ShutdownServer | Letter::Reset => {
                        break;
                    }
                    Letter::BlockAll { unblock_event } => {
                        listener!(unblock_event  => listener);
                        listener.await;
                    }
                    Letter::Wcmd(wcmd) => {
                        curr_aof_size += wcmd.len() as u128;

                        self.file.write_all(&wcmd).await?;

                        if let Some(set_master_outbox) = &set_master_outbox {
                            set_master_outbox.send_async(Letter::Wcmd(wcmd)).await.ok();
                        }

                        self.file.sync_data().await?;

                        if curr_aof_size >= auto_aof_rewrite_min_size {
                            self.rewrite().await?;
                            curr_aof_size = 0;
                        }
                    }
                    Letter::Resp3(_) | Letter::Psync { .. } => {}
                }
            },

            AppendFSync::EverySec => loop {
                tokio::select! {
                    biased;

                    letter = aof_inbox.recv_async() => match letter {
                        Letter::ShutdownServer | Letter::Reset => {
                            break;
                        }
                        Letter::BlockAll { unblock_event } => {
                            listener!(unblock_event  => listener);
                            listener.await;
                        }
                        Letter::Wcmd(wcmd) => {
                            curr_aof_size += wcmd.len() as u128;

                            self.file.write_all(&wcmd).await?;

                            if let Some(set_master_outbox) = &set_master_outbox {
                                set_master_outbox.send_async(Letter::Wcmd(wcmd)).await.ok();
                            }

                            if curr_aof_size >= auto_aof_rewrite_min_size {
                                self.rewrite().await?;
                                curr_aof_size = 0;
                            }
                        }
                        Letter::Resp3(_) | Letter::Psync { .. } => {}
                    },
                    // 每隔一秒，同步文件
                    _ = interval.tick() => {
                        self.file.sync_data().await?;
                    }
                }
            },

            AppendFSync::No => loop {
                match aof_inbox.recv_async().await {
                    Letter::ShutdownServer | Letter::Reset => {
                        break;
                    }
                    Letter::BlockAll { unblock_event } => {
                        listener!(unblock_event  => listener);
                        listener.await;
                    }
                    Letter::Wcmd(wcmd) => {
                        curr_aof_size += wcmd.len() as u128;

                        self.file.write_all(&wcmd).await?;

                        if let Some(set_master_outbox) = &set_master_outbox {
                            set_master_outbox.send_async(Letter::Wcmd(wcmd)).await.ok();
                        }

                        if curr_aof_size >= auto_aof_rewrite_min_size {
                            self.rewrite().await?;
                            curr_aof_size = 0;
                        }
                    }
                    Letter::Resp3(_) | Letter::Psync { .. } => {}
                }
            },
        }

        while let Ok(Letter::Wcmd(wcmd)) = aof_inbox.inner.try_recv() {
            self.file.write_all(&wcmd).await?;
        }

        self.file.sync_data().await?;
        self.rewrite().await?; // 最后再重写一次
        info!("AOF file rewrited.");
        Ok(())
    }

    pub async fn load(&mut self) -> anyhow::Result<()> {
        info!("Loading AOF file...");

        let mut buf = BytesMut::with_capacity(self.file.metadata().await?.size() as usize);
        while self.file.read_buf(&mut buf).await? != 0 {}

        // 如果AOF文件以REDIS开头，说明是RDB与AOF混合文件，需要先加载RDB
        if buf.starts_with(b"REDIS") {
            rdb_load(&mut buf, self.shared.db(), false).await?;
        }

        let (mut handler, _) = Handler::new_fake_with(self.shared, None, None);
        let mut decoder = Resp3Decoder::default();
        while let Some(mut cmd_frame) = decoder.decode(&mut buf)? {
            dispatch(&mut cmd_frame, &mut handler).await?;
        }

        debug_assert!(buf.is_empty());

        info!("AOF file loaded.");

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
        frame::StaticResp3,
        server::Handler,
        util::{gen_test_shared, test_init},
    };
    use std::io::Write;
    use std::time::Duration;

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

    let shared = gen_test_shared();

    let mut aof = Aof::new(shared, test_file_path).await.unwrap();

    aof.load().await.unwrap();

    shared.pool().spawn_pinned(move || async move {
        aof.save().await.unwrap();
    });

    let db = shared.db();
    // 断言AOF文件中的内容已经加载到内存中
    assert_eq!(
        db.get_object("key:000000000015".as_bytes())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .to_vec(),
        b"VXK"
    );
    assert_eq!(
        db.get_object("key:000000000003".as_bytes())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .to_vec(),
        b"VXK"
    );
    assert_eq!(
        db.get_object("key:000000000025".as_bytes())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .to_vec(),
        b"VXK"
    );

    let (mut handler, _) = Handler::new_fake_with(shared, None, None);

    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(test_file_path)
        .await
        .unwrap();
    file.set_len(0).await.unwrap(); // 清空AOF文件
    drop(file);

    let mut frames = vec![
        StaticResp3::new_array(vec![
            StaticResp3::new_blob_string("SET".as_bytes().into()),
            StaticResp3::new_blob_string("key:000000000015".as_bytes().into()),
            StaticResp3::new_blob_string("VXK".as_bytes().into()),
        ]),
        StaticResp3::new_array(vec![
            StaticResp3::new_blob_string("SET".as_bytes().into()),
            StaticResp3::new_blob_string("key:000000000003".as_bytes().into()),
            StaticResp3::new_blob_string("VXK".as_bytes().into()),
        ]),
        StaticResp3::new_array(vec![
            StaticResp3::new_blob_string("SET".as_bytes().into()),
            StaticResp3::new_blob_string("key:000000000025".as_bytes().into()),
            StaticResp3::new_blob_string("VXK".as_bytes().into()),
        ]),
    ];

    // 执行SET命令, handler会将命令写入AOF文件
    for f in &mut frames {
        dispatch(f, &mut handler).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    shared.post_office().send_shutdown_server().await;
}
