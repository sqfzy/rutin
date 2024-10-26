use crate::{
    cmd::dispatch,
    conf::{AofConf, AppendFSync},
    persist::rdb::{decode_rdb, encode_rdb},
    server::Handler,
    shared::{Letter, Shared, AOF_ID, SET_MASTER_ID},
    util::{StaticBytes, StaticStr},
};
use bytes::BytesMut;
use event_listener::{listener, IntoNotification};
use rutin_resp3::codec::decode::Resp3Decoder;
use serde::Deserialize;
use std::{os::unix::fs::MetadataExt, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
    time::Instant,
};
use tokio_util::codec::Decoder;
use tracing::{info, instrument};

pub fn spawn_save_aof(shared: Shared, aof_conf: Arc<AofConf>) {
    #[instrument(level = "info", skip(shared), err)]
    async fn rewrite_aof(shared: Shared, aof_conf: &AofConf) -> anyhow::Result<()> {
        let now = Instant::now();

        let path = &aof_conf.file_path;
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
        let rdb_data = encode_rdb(shared.db(), true).await;

        temp_file.write_all(&rdb_data).await?;

        // 将旧AOF文件备份
        if tokio::fs::try_exists(&path).await? {
            tokio::fs::rename(&path, &bak_path).await?;
        }
        // 将新AOF文件重命名为AOF文件
        tokio::fs::rename(&temp_path, &path).await?;

        info!("rewrite aof elapsed={:?}", now.elapsed());

        Ok(())
    }

    #[inline]
    #[instrument(level = "info", skip(shared), err)]
    pub async fn _save_aof(shared: Shared, aof_conf: Arc<AofConf>) -> anyhow::Result<()> {
        let post_office = shared.post_office();
        let mut aof_file = tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&aof_conf.file_path)
            .await
            .unwrap();

        let mailbox = post_office.register_mailbox(AOF_ID);

        let mut curr_aof_size = 0_u128; // 单位为byte
        let auto_aof_rewrite_min_size = aof_conf.auto_aof_rewrite_min_size << 20;

        let set_master_outbox = post_office.get_outbox(SET_MASTER_ID);

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        match aof_conf.append_fsync {
            AppendFSync::Always => loop {
                match mailbox.recv_async().await {
                    Letter::Shutdown => {
                        break;
                    }
                    Letter::Block { unblock_event } => {
                        unblock_event.notify(1.additional());
                        listener!(unblock_event  => listener);
                        listener.await;
                    }
                    Letter::Wcmd(wcmd) => {
                        curr_aof_size += wcmd.len() as u128;

                        aof_file.write_all(&wcmd).await?;

                        if let Some(set_master_outbox) = &set_master_outbox {
                            set_master_outbox.send_async(Letter::Wcmd(wcmd)).await.ok();
                        }

                        aof_file.sync_data().await?;

                        if curr_aof_size >= auto_aof_rewrite_min_size {
                            rewrite_aof(shared, &aof_conf).await?;
                            curr_aof_size = 0;
                        }
                    }
                    Letter::Resp3(_) | Letter::Psync { .. } => {}
                }
            },

            AppendFSync::EverySec => loop {
                tokio::select! {
                    biased;

                    letter = mailbox.recv_async() => match letter {
                        Letter::Shutdown => {
                            break;
                        }
                        Letter::Block { unblock_event } => {
                            unblock_event.notify(1.additional());
                            listener!(unblock_event  => listener);
                            listener.await;
                        }
                        Letter::Wcmd(wcmd) => {
                            curr_aof_size += wcmd.len() as u128;

                            aof_file.write_all(&wcmd).await?;

                            if let Some(set_master_outbox) = &set_master_outbox {
                                set_master_outbox.send_async(Letter::Wcmd(wcmd)).await.ok();
                            }

                            if curr_aof_size >= auto_aof_rewrite_min_size {
                                rewrite_aof(shared, &aof_conf).await?;
                                curr_aof_size = 0;
                            }
                        }
                        Letter::Resp3(_) | Letter::Psync { .. } => {}
                    },
                    // 每隔一秒，同步文件
                    _ = interval.tick() => {
                        aof_file.sync_data().await?;
                    }
                }
            },

            AppendFSync::No => loop {
                match mailbox.recv_async().await {
                    Letter::Shutdown => {
                        break;
                    }
                    Letter::Block { unblock_event } => {
                        unblock_event.notify(1.additional());
                        listener!(unblock_event  => listener);
                        listener.await;
                    }
                    Letter::Wcmd(wcmd) => {
                        curr_aof_size += wcmd.len() as u128;

                        aof_file.write_all(&wcmd).await?;

                        if let Some(set_master_outbox) = &set_master_outbox {
                            set_master_outbox.send_async(Letter::Wcmd(wcmd)).await.ok();
                        }

                        if curr_aof_size >= auto_aof_rewrite_min_size {
                            rewrite_aof(shared, &aof_conf).await?;
                            curr_aof_size = 0;
                        }
                    }
                    Letter::Resp3(_) | Letter::Psync { .. } => {}
                }
            },
        }

        while let Ok(Letter::Wcmd(wcmd)) = mailbox.inbox.try_recv() {
            aof_file.write_all(&wcmd).await?;
        }

        aof_file.sync_data().await?;
        rewrite_aof(shared, &aof_conf).await?; // 最后再重写一次

        Ok(())
    }

    shared.pool().spawn_pinned(move || async move {
        _save_aof(shared, aof_conf).await.ok();
    });
}

#[instrument(level = "info", skip(shared), err)]
pub async fn load_aof(shared: Shared, aof_conf: &AofConf) -> anyhow::Result<()> {
    // 暂停AOF写入
    let _guard = shared.post_office().send_block(AOF_ID).await;

    let now = Instant::now();

    let mut aof_file = tokio::fs::OpenOptions::new()
        .read(true)
        .open(&aof_conf.file_path)
        .await?;

    let mut buf = BytesMut::with_capacity(aof_file.metadata().await?.size() as usize);
    while aof_file.read_buf(&mut buf).await? != 0 {}

    // 如果AOF文件以REDIS开头，说明是RDB与AOF混合文件，需要先加载RDB
    if buf.starts_with(b"REDIS") {
        decode_rdb(&mut buf, shared.db(), false).await?;
    }

    let (mut handler, _) = Handler::new_fake_with(shared, None, None);
    let mut decoder = Resp3Decoder::<StaticBytes, StaticStr>::default();
    while let Some(cmd_frame) = decoder.decode(&mut buf)? {
        dispatch(cmd_frame, &mut handler).await?;
    }

    info!("load aof elapsed={:?}", now.elapsed());
    Ok(())
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
    let aof_conf = AofConf {
        use_rdb_preamble: false,
        file_path: test_file_path.to_string(),
        append_fsync: AppendFSync::EverySec,
        auto_aof_rewrite_min_size: 1024,
    };

    load_aof(shared, &aof_conf).await.unwrap();

    let db = shared.db();
    // 断言AOF文件中的内容已经加载到内存中
    assert_eq!(
        db.get_object("key:000000000015".as_bytes())
            .await
            .unwrap()
            .value
            .on_str()
            .unwrap()
            .to_vec(),
        b"VXK"
    );
    assert_eq!(
        db.get_object("key:000000000003".as_bytes())
            .await
            .unwrap()
            .value
            .on_str()
            .unwrap()
            .to_vec(),
        b"VXK"
    );
    assert_eq!(
        db.get_object("key:000000000025".as_bytes())
            .await
            .unwrap()
            .value
            .on_str()
            .unwrap()
            .to_vec(),
        b"VXK"
    );

    spawn_save_aof(shared, Arc::new(aof_conf));

    let (mut handler, _) = Handler::new_fake_with(shared, None, None);

    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(test_file_path)
        .await
        .unwrap();
    file.set_len(0).await.unwrap(); // 清空AOF文件
    drop(file);

    let frames = vec![
        StaticResp3::new_array(vec![
            StaticResp3::new_blob_string("SET".as_bytes()),
            StaticResp3::new_blob_string("key:000000000015".as_bytes()),
            StaticResp3::new_blob_string("VXK".as_bytes()),
        ]),
        StaticResp3::new_array(vec![
            StaticResp3::new_blob_string("SET".as_bytes()),
            StaticResp3::new_blob_string("key:000000000003".as_bytes()),
            StaticResp3::new_blob_string("VXK".as_bytes()),
        ]),
        StaticResp3::new_array(vec![
            StaticResp3::new_blob_string("SET".as_bytes()),
            StaticResp3::new_blob_string("key:000000000025".as_bytes()),
            StaticResp3::new_blob_string("VXK".as_bytes()),
        ]),
    ];

    // 执行SET命令, handler会将命令写入AOF文件
    for f in frames {
        dispatch(f, &mut handler).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    shared.post_office().send_shutdown_server();
}
