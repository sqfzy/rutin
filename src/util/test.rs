#![allow(unreachable_code)]

use crate::{
    cmd::commands::{CmdFlag, EXISTS_CMD_FLAG},
    conf::{
        gen_run_id, AccessControl, Acl, AofConf, Conf, ExpirationEvict, MasterConf, MemoryConf,
        OomConf, RdbConf, ReplicaConf, SecurityConf, ServerConf,
    },
    persist::aof::AppendFSync,
    server::{preface, FakeHandler},
    shared::Shared,
};
use arc_swap::ArcSwap;
use std::sync::Mutex;
use std::sync::Once;
use tracing::Level;

pub fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap()
}

pub const TEST_AC_USERNAME: &str = "test_ac";
pub const TEST_AC_PASSWORD: &str = "test_pwd";
pub const TEST_AC_CMDS_FLAG: CmdFlag = EXISTS_CMD_FLAG;

pub fn test_init() {
    #[cfg(not(feature = "test_util"))]
    panic!("need to enable test_util feature");

    static INIT: Once = Once::new();

    INIT.call_once(|| {
        preface();

        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .init();
    });
}

pub fn gen_test_config() -> Conf {
    #[cfg(not(feature = "test_util"))]
    panic!("need to enable test_util feature");

    let acl = Acl::new();
    acl.insert(
        TEST_AC_USERNAME,
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
            log_level: "info".into(),
            max_connections: 1024,
            max_batch: 1024,
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
            enable_checksum: true,
        }),
        aof: Some(AofConf {
            use_rdb_preamble: true,
            file_path: "tests/appendonly/test.aof".to_string(),
            append_fsync: AppendFSync::EverySec,
            auto_aof_rewrite_min_size: 128,
        }),
        memory: MemoryConf {
            oom: Some(OomConf {
                maxmemory: u64::MAX,
                maxmemory_policy: Default::default(),
                maxmemory_samples_count: 5,
            }),
            expiration_evict: ExpirationEvict { samples_count: 800 },
        },
        tls: None,
    }
}

pub fn gen_test_handler() -> FakeHandler {
    #[cfg(not(feature = "test_util"))]
    panic!("need to enable test_util feature");

    FakeHandler::new_fake_with(Shared::with_conf(gen_test_config()), None, None).0
}

pub fn gen_test_shared() -> Shared {
    #[cfg(not(feature = "test_util"))]
    panic!("need to enable test_util feature");

    Shared::with_conf(gen_test_config())
}
