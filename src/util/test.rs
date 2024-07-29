use std::sync::{Arc, Once};

use crate::{
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
use crossbeam::atomic::AtomicCell;
use rand::Rng;
use tracing::Level;

pub const TEST_AC_USERNAME: &str = "test_ac";
pub const TEST_AC_PASSWORD: &str = "test_pwd";
pub const TEST_AC_CMD_FLAG: u128 = 0x010;

pub fn test_init() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        NEVER_EXPIRE.init();

        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .init();
    });
}

pub fn get_test_config() -> Arc<Conf> {
    let run_id: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(40)
        .map(char::from)
        .collect();

    let acl = Acl::new();
    acl.insert(
        TEST_AC_USERNAME.into(),
        AccessControl {
            password: TEST_AC_PASSWORD.into(),
            cmd_flag: TEST_AC_CMD_FLAG,
            ..Default::default()
        },
    );

    let conf = Conf {
        server: ServerConf {
            addr: "127.0.0.1".to_string(),
            port: 6379,
            run_id,
            expire_check_interval_secs: 1,
            log_level: "info".to_string(),
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
            master_addr: ArcSwapOption::new(None),
            max_replica: 6,
            offset: AtomicCell::new(0),
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
    };

    Arc::new(conf)
}

pub fn get_test_db() -> Arc<Db> {
    Arc::new(Db::new(get_test_config()))
}

pub fn get_test_shared() -> Shared {
    Shared::new(get_test_db(), get_test_config(), Default::default())
}
