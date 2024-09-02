use crate::{
    cmd::commands::{CmdFlag, EXISTS_CMD_FLAG},
    conf::{
        gen_run_id, AccessControl, Acl, AofConf, Conf, MasterConf, MemoryConf, RdbConf,
        ReplicaConf, SecurityConf, ServerConf,
    },
    persist::aof::AppendFSync,
    server::preface,
    shared::{db::Db, Shared},
};
use arc_swap::ArcSwap;
use std::sync::Once;
use tokio::sync::Mutex;
use tracing::Level;

pub const TEST_AC_USERNAME: &str = "test_ac";
pub const TEST_AC_PASSWORD: &str = "test_pwd";
pub const TEST_AC_CMDS_FLAG: CmdFlag = EXISTS_CMD_FLAG;

pub fn test_init() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        preface();

        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .init();
    });
}

pub fn get_test_config() -> Conf {
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
            host: "127.0.0.1".into(),
            port: 6379,
            run_id: gen_run_id(),
            expire_check_interval_secs: 1,
            log_level: "info".into(),
            max_connections: 1024,
            max_batch: 1024,
            standalone: false,
        },
        security: SecurityConf {
            requirepass: None,
            default_ac: ArcSwap::from_pointee(AccessControl::new_loose()),
            acl: Some(acl),
        },
        master: Some(MasterConf {
            max_replica: 6,
            backlog_size: 1 << 10,
            ping_replica_period: 10,
            timeout: 60,
        }),
        replica: ReplicaConf {
            master_info: Mutex::new(None),
            master_auth: None,
            read_only: true,
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
    Db::new(get_test_config().memory.clone())
}

pub fn get_test_shared() -> Shared {
    Shared::with_conf(get_test_config())
}
