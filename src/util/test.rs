#![allow(unreachable_code)]

use crate::{
    cmd::commands::{CmdFlag, EXISTS_CMD_FLAG},
    conf::*,
    server::{preface, FakeHandler},
    shared::Shared,
};
use arc_swap::{ArcSwap, ArcSwapOption};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Once};
use tracing::Level;

pub fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap()
}

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

pub fn gen_test_config() -> Conf {
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
        server: ArcSwap::from_pointee(ServerConf {
            host: "127.0.0.1".into(),
            port: 6379,
            run_id: gen_run_id(),
            log_level: "info".into(),
            max_connections: 1024,
            max_batch: 1024,
        }),
        security: ArcSwap::from_pointee(SecurityConf {
            requirepass: None,
            default_ac: Arc::new(AccessControl::new_loose()),
            acl: Some(acl),
        }),
        master: ArcSwapOption::from_pointee(Some(MasterConf {
            max_replica: 6,
            backlog_size: 1 << 10,
            ping_replica_period_ms: 10,
            timeout_ms: 60,
        })),
        replica: ArcSwapOption::from_pointee(ReplicaConf {
            master_auth: None,
            read_only: true,
            master_host: "127.0.0.1".into(),
            master_port: 6400,
            offset: AtomicU64::new(0),
            master_run_id: "test".into(),
        }),
        rdb: ArcSwapOption::from_pointee(Some(RdbConf {
            file_path: "dump.rdb".to_string(),
            save: None,
            enable_checksum: true,
        })),
        aof: ArcSwapOption::from_pointee(Some(AofConf {
            use_rdb_preamble: true,
            file_path: "tests/appendonly/test.aof".to_string(),
            append_fsync: AppendFSync::EverySec,
            auto_aof_rewrite_min_size: 128,
        })),
        memory: ArcSwap::from_pointee(MemoryConf {
            oom: Some(OomConf {
                maxmemory: u64::MAX,
                maxmemory_policy: Default::default(),
                maxmemory_samples_count: 5,
            }),
            expiration_evict: ExpirationEvict { samples_count: 800 },
        }),
        tls: ArcSwapOption::from_pointee(None),
    }
}

pub fn gen_test_handler() -> FakeHandler {
    FakeHandler::new_fake_with(Shared::with_conf(gen_test_config()), None, None).0
}

pub fn gen_test_shared() -> Shared {
    Shared::with_conf(gen_test_config())
}
