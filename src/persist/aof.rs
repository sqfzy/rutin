use crate::{
    cmd::dispatch,
    frame::Resp3Decoder,
    persist::rdb::{rdb_load, rdb_save},
    server::Handler,
    shared::{Inbox, Shared},
    util::save_and_propagate_wcmd_to_replicas,
};
use anyhow::Result;
use bytes::BytesMut;
use serde::Deserialize;
use std::{os::unix::fs::MetadataExt, path::Path};
use tokio::{fs::File, io::AsyncReadExt};
use tokio_util::codec::Decoder;

pub struct Aof {
    pub file: File,
    pub shared: Shared,
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
        wcmd_inbox: Inbox,
    ) -> anyhow::Result<()> {
        save_and_propagate_wcmd_to_replicas(self, wcmd_inbox).await
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
        server::Handler,
        shared::{Letter, WCMD_PROPAGATE_ID},
        util::{get_test_shared, test_init},
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

    let shared = get_test_shared();

    let mut aof = Aof::new(shared.clone(), test_file_path).await.unwrap();

    aof.load().await.unwrap();

    let wcmd_receiver = shared.post_office().get_inbox(WCMD_PROPAGATE_ID).unwrap();
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
    shared.post_office().send_all(Letter::ShutdownServer).await;
}
