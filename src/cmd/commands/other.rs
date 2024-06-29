use super::*;
use crate::{
    cmd::{
        error::{CmdError, Err},
        CmdExecutor, CmdType, CmdUnparsed,
    },
    conf::AccessControl,
    connection::AsyncStream,
    frame::Resp3,
    persist::rdb::Rdb,
    server::Handler,
    util, CmdFlag, Id,
};
use bytes::Bytes;
use tracing::instrument;

/// # Reply:
///
/// **Array reply**: a nested list of command details. The order of the commands in the array is random.
// #[derive(Debug)]
// pub struct _Command;
//
// impl CmdExecutor for _Command {
//     const CMD_TYPE: CmdType = CmdType::Other;
//
//     // TODO: 返回命令字典，以便支持客户端的命令补全
// async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError>{
//
//         Ok(None)
//     }
//
//     fn parse(cmd_frame: RESP3) -> Result<Self, CmdError> {
//
//         Ok(_Command)
//     }
// }

/// # Reply:
///
/// **Simple string reply**: PONG when no argument is provided.
/// **Bulk string reply**: the provided argument.
#[derive(Debug)]
pub struct Ping {
    msg: Option<Bytes>,
}

impl CmdExecutor for Ping {
    const NAME: &'static str = "PING";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = PING_FLAG;

    #[instrument(level = "debug", skip(_handler), ret, err)]
    async fn execute(
        self,
        _handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let res = match self.msg {
            Some(msg) => Resp3::new_simple_string(
                msg.try_into()
                    .map_err(|_| CmdError::from("message is not valid utf8"))?,
            ),
            None => Resp3::new_simple_string("PONG".into()),
        };

        Ok(Some(res))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if !args.is_empty() && args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Ping { msg: args.next() })
    }
}

/// # Reply:
///
/// **Bulk string reply**: the given string.
#[derive(Debug)]
pub struct Echo {
    msg: Bytes,
}

impl CmdExecutor for Echo {
    const NAME: &'static str = "ECHO";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = ECHO_FLAG;

    #[instrument(level = "debug", skip(_handler), ret, err)]
    async fn execute(
        self,
        _handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        Ok(Some(Resp3::new_blob_string(self.msg)))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Echo {
            msg: args.next().unwrap(),
        })
    }
}
//
// 该命令用于获取Redis服务器的各种信息和统计数值
// *1\r\n$4\r\ninfo\r\n
// *2\r\n$4\r\ninfo\r\n$11\r\nreplication\r\n
// pub struct Info {
//     pub sections: Section,
// }
//
// #[allow(dead_code)]
// pub enum Section {
//     Array(Vec<Section>),
//     // all: Return all sections (excluding module generated ones)
//     All,
//     // default: Return only the default set of sections
//     Default,
//     // everything: Includes all and modules
//     Everything,
//     // server: General information about the Redis server
//     Server,
//     // clients: Client connections section
//     Clients,
//     // memory: Memory consumption related information
//     Memory,
//     // persistence: RDB and AOF related information
//     Persistence,
//     // stats: General statistics
//     Stats,
//     // replication: Master/replica replication information
//     Replication,
//     // cpu: CPU consumption statistics
//     Cpu,
//     // commandstats: Redis command statistics
//     CommandStats,
//     // latencystats: Redis command latency percentile distribution statistics
//     LatencyStats,
//     // sentinel: Redis Sentinel section (only applicable to Sentinel instances)
//     Sentinel,
//     // cluster: Redis Cluster section
//     Cluster,
//     // modules: Modules section
//     Modules,
//     // keyspace: Database related statistics
//     Keyspace,
//     // errorstats: Redis error statistics
//     ErrorStats,
// }
// impl TryFrom<Bytes> for Section {
//     type Error = Error;
//
//     fn try_from(bulk: Bytes) -> Result<Self, Self::Error> {
//         let value = bulk.to_ascii_uppercase();
//         match value.as_slice() {
//             b"REPLICATION" => Ok(Section::Replication),
//             // TODO:
//             _ => Err(anyhow!("Incomplete")),
//         }
//     }
// }
// impl TryFrom<Vec<Bytes>> for Section {
//     type Error = Error;
//
//     fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
//         let mut sections = Vec::with_capacity(cmd_frame.array_len()?);
//         for section in bulks {
//             sections.push(section.try_into()?);
//         }
//         Ok(Section::Array(sections))
//     }
// }

// impl Info {
// #[instrument(level = "debug", skip(handler), ret, err)]
//     pub async fn execute(&self, _db: &Db) -> ResultCmd
//         debug!("executing command 'INFO'");
//
//         match self.sections {
//             Section::Replication => {
//                 let res = if CONFIG.replication.replicaof.is_none() {
//                     format!(
//                         "role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
//                         CONFIG.server.run_id,
//                         OFFSET.load(std::sync::atomic::Ordering::SeqCst)
//                     )
//                 } else {
//                     format!(
//                         "role:slave\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
//                         CONFIG.server.run_id,
//                         OFFSET.load(std::sync::atomic::Ordering::SeqCst)
//                     )
//                 };
//                 Ok(Some(RESP3::Bulk(res.into())))
//             }
//             // TODO:
//             _ => Err(anyhow!("Incomplete")),
//         }
//     }
// }

// 该命令用于在后台异步保存当前数据库的数据到磁盘
/// # Reply:
///
/// **Simple string reply:** Background saving started.
/// **Simple string reply:** Background saving scheduled.
#[derive(Debug)]
pub struct BgSave;

impl CmdExecutor for BgSave {
    const NAME: &'static str = "BGSAVE";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = BGSAVE_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let rdb_conf = &handler.shared.conf().rdb;
        let shared = &handler.shared;

        let mut rdb = if let Some(rdb) = rdb_conf {
            Rdb::new(shared, rdb.file_path.clone(), rdb.enable_checksum)
        } else {
            Rdb::new(shared, "./dump.rdb".into(), false)
        };
        // let mut rdb = RDB::new(shared, rdb_conf.unwrap_or("").file_path.clone(), rdb_conf.enable_checksum);
        tokio::spawn(async move {
            if let Err(e) = rdb.save().await {
                tracing::error!("save rdb error: {:?}", e);
            } else {
                tracing::info!("save rdb success");
            }
        });

        Ok(Some(Resp3::new_simple_string(
            "Background saving started".into(),
        )))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if !args.is_empty() {
            return Err(Err::WrongArgNum.into());
        }

        Ok(BgSave)
    }
}

// pub struct BgRewriteAof;

#[derive(Debug)]
pub struct Auth {
    pub username: Bytes,
    pub password: Bytes,
}

impl CmdExecutor for Auth {
    const NAME: &'static str = "AUTH";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = AUTH_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        if let Some(acl) = handler.shared.conf().security.acl.as_ref() {
            if let Some(ac) = acl.get(&self.username) {
                if !ac.is_pwd_correct(&self.password) {
                    Err("ERR invalid password".into())
                } else {
                    // 设置客户端的权限
                    handler.context.ac = std::sync::Arc::new(ac.clone());
                    Ok(Some(Resp3::new_simple_string("OK".into())))
                }
            } else {
                Err("ERR invalid username".into())
            }
        } else {
            // 没有设置ACL
            Ok(Some(Resp3::new_simple_string("OK".into())))
        }
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 && args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Auth {
            username: args.next().unwrap(),
            password: args.next().unwrap_or_default(),
        })
    }
}

/// # Desc:
///
/// 执行该命令后，会开启客户端缓存追踪。每次执行读命令时，客户端都可能缓存该键
/// 值对， 因此需要对客户端读过的键值对加入 监听事件ObjectEvent::Write，当该键
/// 值对被修改时服务端需要发送invalidation messages给客户端。取消追踪并不意味
/// 着客户端一定不会收到invalidation messages，它仅仅保证对于新的读命令，不再监
/// 听其键值对
///
/// # Reply:
///
/// **Simple string reply:** OK if the connection was successfully put in tracking
/// mode or if the tracking mode was successfully disabled. Otherwise, an error is
/// returned.
#[derive(Debug)]
pub struct ClientTracking {
    switch_on: bool,
    // 将该客户端缓存失效的消息重定向
    redirect: Option<Id>,
}

impl CmdExecutor for ClientTracking {
    const NAME: &'static str = "TRACKING";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = CLIENT_TRACKING_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        if !self.switch_on {
            // 关闭追踪后并不意味着之前的追踪事件会被删除，只是不再添加新的追踪事件
            handler.context.client_track = None;
            return Ok(Some(Resp3::new_simple_string("OK".into())));
        }

        if let Some(redirect) = self.redirect {
            let redirect_bg_sender = handler
                .shared
                .db()
                .get_client_bg_sender(redirect)
                .ok_or("ERR The client ID you want redirect to does not exist")?;
            handler.context.client_track = Some(redirect_bg_sender);
        } else {
            handler.context.client_track = Some(handler.bg_task_channel.new_sender());
        }

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() > 2 {
            return Err(Err::WrongArgNum.into());
        }

        let mut switch = [0; 3];
        let bulk = args.next().unwrap();
        let len = bulk.len();
        switch[..len].clone_from_slice(bulk.as_ref());
        switch[..len].make_ascii_uppercase();

        let switch = match &switch[..len] {
            b"ON" => true,
            b"OFF" => false,
            _ => return Err("ERR invalid switch is given")?,
        };

        let redirect = if let Some(redirect) = args.next() {
            Some(util::atoi::<Id>(&redirect)?)
        } else {
            None
        };

        Ok(ClientTracking {
            switch_on: switch,
            redirect,
        })
    }
}

#[cfg(test)]
mod cmd_other_tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        conf::{AccessControl, Acl, Conf},
        shared::Shared,
        util::test_init,
    };

    #[tokio::test]
    async fn auth_test() {
        test_init();

        let username = "admin";
        let password = "123456";
        let cmd_flag = 0x010;
        let acl = Acl::new();
        acl.insert(
            Bytes::from(username),
            AccessControl {
                password: Bytes::from(password),
                cmd_flag,
                ..Default::default()
            },
        );

        let conf = Conf {
            security: crate::conf::SecurityConf {
                acl: Some(acl),
                ..Default::default()
            },
            ..Default::default()
        };

        let shared = Shared::new(Default::default(), Arc::new(conf), Default::default());
        let (mut handler, _) = Handler::new_fake_with(shared, None, None);

        let auth = Auth::parse(
            &mut CmdUnparsed::from([username, "1234567"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = auth.execute(&mut handler).await;
        assert_eq!(res.unwrap_err().to_string(), "ERR invalid password");

        let auth = Auth::parse(
            &mut CmdUnparsed::from(["admin1", password].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = auth.execute(&mut handler).await;
        assert_eq!(res.unwrap_err().to_string(), "ERR invalid username");

        let auth = Auth::parse(
            &mut CmdUnparsed::from([username, password].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        auth.execute(&mut handler).await.unwrap();
        assert_eq!(handler.context.ac.cmd_flag(), cmd_flag);
    }

    #[tokio::test]
    async fn client_tracking_test() {
        test_init();

        let (mut handler, _) = Handler::new_fake();

        let tracking = ClientTracking::parse(
            &mut CmdUnparsed::from(["ON"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        tracking.execute(&mut handler).await.unwrap();
        assert!(handler.context.client_track.is_some());

        let tracking = ClientTracking::parse(
            &mut CmdUnparsed::from(["OFF"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        tracking.execute(&mut handler).await.unwrap();
        assert!(handler.context.client_track.is_none());
    }
}
