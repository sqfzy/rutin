use crate::{
    cmd::commands::{Flag, EXISTS_CMD_FLAG},
    conf::{
        AccessControl, Acl, AofConf, Conf, MemoryConf, RdbConf, ReplicaConf, SecurityConf,
        ServerConf,
    },
    persist::aof::AppendFSync,
    shared::{
        db::{Db, NEVER_EXPIRE},
        Shared,
    },
};
use arc_swap::{ArcSwap, ArcSwapOption};
use bytestring::ByteString;
use rand::Rng;
use std::sync::{Mutex, Once};
use tracing::Level;

pub const TEST_AC_USERNAME: &str = "test_ac";
pub const TEST_AC_PASSWORD: &str = "test_pwd";
pub const TEST_AC_CMDS_FLAG: Flag = EXISTS_CMD_FLAG;

pub fn test_init() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        NEVER_EXPIRE.init();

        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .init();
    });
}

pub fn get_test_config() -> Conf {
    let run_id: ByteString = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(40)
        .map(char::from)
        .collect::<String>()
        .into();

    let acl = Acl::new();
    acl.insert(
        TEST_AC_USERNAME.into(),
        AccessControl {
            password: TEST_AC_PASSWORD.into(),
            cmds_flag: TEST_AC_CMDS_FLAG,
            ..Default::default()
        },
    );

    Conf {
        server: ServerConf {
            addr: "127.0.0.1".into(),
            port: 6379,
            run_id,
            expire_check_interval_secs: 1,
            log_level: "info".into(),
            max_connections: 1024,
            max_batch: 1024,
        },
        security: SecurityConf {
            requirepass: None,
            rename_commands: vec![],
            default_ac: ArcSwap::from_pointee(AccessControl::new_loose()),
            acl: Some(acl),
        },
        replica: ReplicaConf {
            master_info: Mutex::new(None),
            max_replica: 6,
            master_auth: None,
        },
        rdb: Some(RdbConf {
            file_path: "dump.rdb".to_string(),
            save: None,
            version: 9,
            enable_checksum: true,
        }),
        aof: Some(AofConf {
            use_rdb_preamble: true,
            file_path: "tests/appendonly/test.aof".to_string(),
            append_fsync: AppendFSync::EverySec,
            auto_aof_rewrite_min_size: 128,
        }),
        memory: Some(MemoryConf {
            maxmemory: u64::MAX,
            maxmemory_policy: Default::default(),
            maxmemory_samples: 5,
        }),
        tls: None,
    }
}

pub fn get_test_db() -> Db {
    Db::new(Box::leak(Box::new(get_test_config())))
}

pub fn get_test_shared() -> Shared {
    Shared::new(Box::leak(Box::new(get_test_config())), Default::default())
}
