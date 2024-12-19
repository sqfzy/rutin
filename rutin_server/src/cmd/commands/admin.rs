use super::*;
use crate::{
    cmd::CmdExecutor,
    conf::{AccessControl, AccessControlIntermedium, DEFAULT_USER, ReplicaConf},
    error::{RutinError, RutinResult},
    frame::Resp3,
    persist::rdb::save_rdb,
    server::{AsyncStream, Handler, HandlerContext},
    shared::{Letter, NULL_ID, SET_MASTER_ID},
    util,
};
use bytes::BytesMut;
use bytestring::ByteString;
use itertools::Itertools;
use std::{any::Any, fmt::Debug, sync::Arc};
use tokio::net::TcpStream;
use tracing::instrument;

/// # Reply:
///
/// Array reply: an array of Bulk string reply elements representing ACL categories or commands in a given category.
/// Simple error reply: the command returns an error if an invalid category name is given.
#[derive(Debug)]
pub struct AclCat<A> {
    pub category: Option<A>,
}

impl<A> CmdExecutor<A> for AclCat<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(_handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        _handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let res = if let Some(cat) = self.category {
            let cat = cat_name_to_cmds_flag(cat)?;

            cmds_flag_to_names(cat)
                .iter_mut()
                .map(|s| Resp3::new_blob_string(s.as_bytes()))
                .collect_vec()
        } else {
            cmds_flag_to_names(ALL_CMDS_FLAG)
                .iter_mut()
                .map(|s| Resp3::new_blob_string(s.as_bytes()))
                .collect_vec()
        };

        Ok(Some(Resp3::new_array(res)))
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 && args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        Ok(AclCat {
            category: args.next(),
        })
    }
}

/// # Reply:
///
/// Integer reply: the number of users that were deleted. This number will not always match the number of arguments since certain users may not exist.
///
/// # Tips:
///
/// default_ac是默认的ac，不可删除
#[derive(Debug)]
pub struct AclDelUser<A> {
    pub users: Vec<A>,
}

impl<A> CmdExecutor<A> for AclDelUser<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut count = 0;

        if let Some(acl) = &handler.shared.conf().security_conf().acl {
            for user in self.users {
                if acl.remove(user.as_ref()).is_some() {
                    count += 1;
                }
            }
        }

        Ok(Some(Resp3::new_integer(count)))
    }

    fn parse(args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        Ok(AclDelUser {
            users: args.collect(),
        })
    }

}

/// # Reply:
///
/// Simple string reply: OK. If the rules contain errors, the error is returned.
///
/// # Usage:
///
/// RESET代表清空
///
/// ```
/// ACL SETUSER <name> [enable | disable]  [PWD <password>] [ALLOWCMD <cmd>,...]
/// [DENYCMD <cmd>,...] [ALLOWCAT <category>,...] [DENYCAT <category>,...] [DENYRKEY <readable key>,...]
/// [DENYWKEY <writeable key>,...] [DENYCHANNEL <channel>,...]
/// ```
#[derive(Debug)]
pub struct AclSetUser<A> {
    pub name: A,
    pub aci: AccessControlIntermedium,
}

impl<A> CmdExecutor<A> for AclSetUser<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let shared = handler.shared;
        let conf = shared.conf();
        let security_conf = conf.security_conf();

        if self.name.as_ref() == DEFAULT_USER {
            // 如果是default_ac则clone后合并，再放回
            let default_ac = security_conf.default_ac.clone();
            let mut default_ac = AccessControl::clone(&default_ac);

            default_ac.merge(self.aci)?;

            conf.update_security_conf(
                &mut |mut security_conf| {
                    security_conf.default_ac = Arc::new(default_ac.clone());
                    security_conf
                },
                shared,
            );
        } else if let Some(acl) = &security_conf.acl {
            if let Some(mut ac) = acl.get_mut(self.name.as_ref()) {
                // 如果存在则合并
                ac.merge(self.aci)?;
            } else {
                // 不存在则插入
                acl.insert(self.name, self.aci.try_into()?);
            }
        }

        Ok(Some(Resp3::new_simple_string("OK")))
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.is_empty()
            && args.len() != 2
            && args.len() != 4
            && args.len() != 6
            && args.len() != 8
            && args.len() != 10
            && args.len() != 12
        {
            return Err(RutinError::WrongArgNum);
        }

        let name = args.next().unwrap();

        let mut aci = AccessControlIntermedium::default();

        // FIX: 键名无法包含','
        while let Some(b) = args.next_uppercase::<16>() {
            match b.as_ref() {
                b"ENABLE" => aci.enable = Some(true),
                b"DISABLE" => aci.enable = Some(false),
                b"PWD" => aci.password = Some(args.next().unwrap().into_bytes()),
                // collect 从ALLOWCMD开始直到某个cmd的末尾不带','则结束
                b"ALLOWCMD" => {
                    let mut allow_commands = Vec::new();
                    for b in args.by_ref() {
                        let b = b.as_ref();
                        if b.last().is_some_and(|b| *b == b',') {
                            // allow_commands.push(b.split_off(b.len() - 1));
                            allow_commands.push(BytesMut::from(&b[..b.len() - 1]));
                        } else {
                            allow_commands.push(b.into());
                            break;
                        }
                    }
                    aci.allow_commands = Some(allow_commands);
                }
                b"DENYCMD" => {
                    let mut deny_commands = Vec::with_capacity(10);
                    for b in args.by_ref() {
                        let b = b.as_ref();
                        if b.last().is_some_and(|b| *b == b',') {
                            deny_commands.push(BytesMut::from(&b[..b.len() - 1]));
                        } else {
                            deny_commands.push(b.into());
                            break;
                        }
                    }
                    aci.deny_commands = Some(deny_commands);
                }
                b"ALLOWCAT" => {
                    let mut allow_categories = Vec::with_capacity(10);
                    for b in args.by_ref() {
                        let b = b.as_ref();
                        if b.last().is_some_and(|b| *b == b',') {
                            allow_categories.push(BytesMut::from(&b[..b.len() - 1]));
                        } else {
                            allow_categories.push(b.into());
                            break;
                        }
                    }
                    aci.allow_categories = Some(allow_categories);
                }
                b"DENYCAT" => {
                    let mut deny_categories = Vec::with_capacity(10);
                    for b in args.by_ref() {
                        let b = b.as_ref();
                        if b.last().is_some_and(|b| *b == b',') {
                            deny_categories.push(BytesMut::from(&b[..b.len() - 1]));
                        } else {
                            deny_categories.push(b.into());
                            break;
                        }
                    }
                    aci.deny_categories = Some(deny_categories);
                }
                b"DENYRKEY" => {
                    let mut deny_read_key_patterns = Vec::with_capacity(10);
                    for b in args.by_ref() {
                        let b = b.as_ref();
                        if b.last().is_some_and(|b| *b == b',') {
                            deny_read_key_patterns
                                .push(String::from_utf8(b[..b.len() - 1].to_vec())?);
                        } else {
                            deny_read_key_patterns.push(String::from_utf8(b.to_vec())?);
                            break;
                        }
                    }
                    aci.deny_read_key_patterns = Some(deny_read_key_patterns);
                }
                b"DENYWKEY" => {
                    let mut deny_write_key_patterns = Vec::with_capacity(10);
                    for b in args.by_ref() {
                        let b = b.as_ref();
                        if b.last().is_some_and(|b| *b == b',') {
                            deny_write_key_patterns
                                .push(String::from_utf8(b[..b.len() - 1].to_vec())?);
                        } else {
                            deny_write_key_patterns.push(String::from_utf8(b.to_vec())?);
                            break;
                        }
                    }
                    aci.deny_write_key_patterns = Some(deny_write_key_patterns);
                }
                b"DENYCHANNEL" => {
                    let mut deny_channel_patterns = Vec::with_capacity(10);
                    for b in args.by_ref() {
                        let b = b.as_ref();
                        if b.last().is_some_and(|b| *b == b',') {
                            deny_channel_patterns
                                .push(String::from_utf8(b[..b.len() - 1].to_vec())?);
                        } else {
                            deny_channel_patterns.push(String::from_utf8(b.to_vec())?);
                            break;
                        }
                    }
                    aci.deny_channel_patterns = Some(deny_channel_patterns);
                }
                _ => return Err(RutinError::Syntax),
            }
        }

        Ok(AclSetUser { name, aci })
    }

}

/// # Reply:
///
/// Array reply: list of existing ACL users.
#[derive(Debug)]
pub struct AclUsers;

impl<A> CmdExecutor<A> for AclUsers
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut users = vec![Resp3::new_blob_string(DEFAULT_USER)];
        if let Some(acl) = &handler.shared.conf().security_conf().acl {
            users.extend(acl.iter().map(|e| Resp3::new_blob_string(e.key().clone())));
        }

        Ok(Some(Resp3::new_array(users)))
    }

    fn parse(_args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        Ok(AclUsers)
    }

}

/// # Reply:
///
/// Bulk string reply: the username of the current connection.
#[derive(Debug)]
pub struct AclWhoAmI;

impl<A> CmdExecutor<A> for AclWhoAmI
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        Ok(Some(Resp3::new_blob_string(handler.context.user.clone())))
    }

    fn parse(_args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        Ok(AclWhoAmI)
    }

}

// /// # Reply:
// /// **Simple string reply**: OK.
// #[derive(Debug)]
// pub struct AppendOnly;
//
// impl<A, > CmdExecutor<A, Bytes,ByteString> for AppendOnly
// where
//     A: IntoUppercase + Equivalent<Key> + Hash+Debug,
// A::Mut: AsMut<[u8]>,
// {

//     #[instrument(
//         level = "debug",
//         skip(handler),
//         ret(level = "debug"),
//         err(level = "debug")
//     )]
//     async fn execute(
//         self,
//         handler: &mut Handler<impl AsyncStream>,
//     ) -> RutinResult<Option<Resp3<B,S>>> {
//         let shared = handler.shared;
//         let conf = shared.conf();
//
//         if conf.aof.is_some() {
//             return Ok(Some(Resp3::new_simple_string("OK")));
//         }
//
//         let f = |shared: &mut SharedInner| {
//             shared.conf.aof = Some(Default::default());
//         };
//
//         shared.post_office().send_reset_server(Box::new(f)).await;
//
//         Ok(Some(Resp3::new_simple_string("OK")))
//     }
//
//     fn parse(args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
//         if !args.is_empty() {
//             return Err(RutinError::WrongArgNum);
//         }
//
//         Ok(AppendOnly)
//     }
// }

// 该命令用于在后台异步保存当前数据库的数据到磁盘
/// # Reply:
///
/// **Simple string reply:** Background saving started.
/// **Simple string reply:** Background saving scheduled.
#[derive(Debug)]
pub struct BgSave;

impl<A> CmdExecutor<A> for BgSave
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let shared = handler.shared;

        handler.shared.pool().spawn_pinned(move || async move {
            save_rdb(
                shared,
                &shared.conf().rdb_conf().clone().unwrap_or_default(),
            )
            .await
            .ok();
        });

        Ok(Some(Resp3::new_simple_string("Background saving started")))
    }

    fn parse(args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if !args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        Ok(BgSave)
    }

}

/// # Reply:
///
/// **Simple string reply**: OK.
#[derive(Debug)]
pub struct PSync<A> {
    pub repl_id: A,
    pub repl_offset: u64,
}

impl<A> CmdExecutor<A> for PSync<A>
where
    A: CmdArg,
{
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let post_office = handler.shared.post_office();

        if let Some(handler) = (handler as &mut dyn Any).downcast_mut::<Handler<TcpStream>>()
            && let Some(outbox) = post_office.get_outbox(SET_MASTER_ID)
        {
            let conf = handler.shared.conf();

            let whatever_handler = {
                let stream =
                    TcpStream::connect((conf.server_conf().host.as_ref(), conf.server_conf().port))
                        .await
                        .map_err(|e| RutinError::from(e.to_string()))?;

                let mailbox = post_office.register_mailbox(NULL_ID);
                // 用于通知关闭whatever_handler
                outbox.send(Letter::Shutdown).ok();

                let context = HandlerContext::new(handler.shared, NULL_ID, mailbox);

                Handler::new(handler.shared, stream, context)
            };

            // 换出handler
            let handle_replica = std::mem::replace(handler, whatever_handler);

            // 因为BackLog在SetMaster任务中，因此需要由SetMaster任务处理Psync命令
            outbox
                .send_async(Letter::Psync {
                    handle_replica,
                    repl_id: self
                        .repl_id
                        .as_ref()
                        .to_vec()
                        .try_into()
                        .map_err(|_| RutinError::from("ERR invalid replication id"))?,
                    repl_offset: self.repl_offset,
                })
                .await
                .ok();

            Ok(None)
        } else {
            Err(RutinError::from(
                "ERR server cann't be a master without `master` config",
            ))
        }
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        Ok(PSync {
            repl_id: args.next().unwrap(),
            repl_offset: util::atoi(args.next().unwrap().as_ref())?,
        })
    }

}

/// # Reply:
///
/// **Simple string reply**: OK.
#[derive(Debug)]
pub struct ReplConf {
    pub sub_cmd: ReplConfSubCmd,
}

impl<A> CmdExecutor<A> for ReplConf
where
    A: CmdArg,
{
    #[allow(unused_variables)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        // TODO:
        match self.sub_cmd {
            ReplConfSubCmd::ListeningPort { port } => {
                // handler.shared.conf().replica.listening_port.store(port);
            }
            ReplConfSubCmd::Ack { offset } => {
                // handler.shared.conf().replica.ack.store(offset);
            }
        }

        Ok(Some(Resp3::new_simple_string("OK")))
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 {
            return Err(RutinError::WrongArgNum);
        }

        let sub_cmd = args.next_uppercase::<16>().unwrap();

        let sub_cmd = match sub_cmd.as_ref() {
            b"LISTENING-PORT" => ReplConfSubCmd::ListeningPort {
                port: util::atoi(args.next().unwrap().as_ref())?,
            },
            b"ACK" => ReplConfSubCmd::Ack {
                offset: util::atoi(args.next().unwrap().as_ref())?,
            },
            _ => return Err(RutinError::Syntax),
        };

        Ok(ReplConf { sub_cmd })
    }

}

// REPLCONF listening-port <port>
// 用于从节点通知主节点其当前监听的端口号。这有助于主节点在需要时与从节点建立新的连接。
//
// REPLCONF ip-address <ip>
// 用于从节点通知主节点其当前的 IP 地址。这在从节点的 IP 地址发生变化时尤为重要，以确保主节点能正确识别并与从节点通信。
//
// REPLCONF capa <capability>
// 用于从节点向主节点声明其支持的功能（capability）。常见的功能声明包括：
// capa eof: 表示从节点支持 PSYNC 协议中的 EOF 标记，用于在主节点完成 RDB 文件传输时告知从节点。
// capa psync2: 表示从节点支持 PSYNC2 协议（Redis 4.0+ 中引入的改进版 PSYNC 协议）。
//
// REPLCONF ack <offset>
// 用于从节点定期向主节点报告其已复制的数据的偏移量（offset）。主节点通过这个信息来判断从节点的同步进度，并决定哪些数据可以安全地从内存中清除。
//
// REPLCONF getack
// 这个命令由主节点发送给从节点，要求从节点立即发送一个 REPLCONF ack 消息。这通常在主节点需要立即确认从节点的同步状态时使用。
//
// REPLCONF client-id <id>
// 用于从节点通知主节点其客户端 ID，这在 Redis 集群中可以用来跟踪特定从节点。
//
// REPLCONF current-flush-position <position>
// 用于通知主节点从节点的当前写入位置，这有助于管理主从之间的数据同步流量，特别是在高并发环境下。
//
// REPLCONF rdb-only <yes|no>
// 从节点可以使用这个命令通知主节点它是否仅希望接收 RDB 文件（通常在初始同步过程中），而不接收后续的增量数据。这在某些只需要初始数据快照的场景下使用。
#[derive(Debug)]
pub enum ReplConfSubCmd {
    Ack { offset: u64 },
    // Capa,
    // Get,
    ListeningPort { port: u16 },
    // NoOne,
    // Offset,
    // Set,
}

/// # Reply:
///
/// WARN: 官网上说响应OK但实际上没有返回响应
#[derive(Debug)]
pub struct ReplicaOf {
    replica_no_one: bool,
    master_host: ByteString,
    master_port: u16,
}

impl<A> CmdExecutor<A> for ReplicaOf
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let shared = handler.shared;
        let conf = shared.conf();

        if self.replica_no_one {
            conf.update_replica_conf(&mut |_| None, shared);
        } else {
            conf.update_replica_conf(
                &mut |mut replica_conf| {
                    if let Some(r) = &mut replica_conf {
                        r.master_host = self.master_host.clone();
                        r.master_port = self.master_port;
                    } else {
                        replica_conf =
                            Some(ReplicaConf::new(self.master_host.clone(), self.master_port));
                    }
                    replica_conf
                },
                shared,
            );
        }

        Ok(None)
    }

    fn parse(mut args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        };

        let arg1 = args.next().unwrap();
        let arg2 = args.next().unwrap();

        if arg1.as_ref() == b"NO" && arg2.as_ref() == b"ONE" {
            return Ok(ReplicaOf {
                replica_no_one: true,
                master_host: Default::default(),
                master_port: 0,
            });
        }

        Ok(ReplicaOf {
            replica_no_one: false,
            master_host: arg1
                .as_ref()
                .try_into()
                .map_err(|_| RutinError::from("ERR value is not a valid hostname or ip address"))?,
            master_port: util::atoi(arg2.as_ref())?,
        })
    }

}

/// # Reply:
/// **Simple string reply**: OK.
#[derive(Debug)]
pub struct Save;

impl<A> CmdExecutor<A> for Save
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let shared = handler.shared;
        if save_rdb(
            shared,
            &shared.conf().rdb_conf().clone().unwrap_or_default(),
        )
        .await
        .is_ok()
        {
            Ok(Some(Resp3::new_simple_string("OK")))
        } else {
            Err(RutinError::from("ERR save rdb failed"))
        }
    }

    fn parse(args: CmdUnparsed<A>, _ac: &AccessControl) -> RutinResult<Self> {
        if !args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        Ok(Save)
    }

}

#[tokio::test]
async fn cmd_acl_tests() {
    use crate::util::TEST_AC_USERNAME;
    crate::util::test_init();

    let mut handler = Handler::new_fake().0;

    let res = AclSetUser::test(
        [
            "default_ac",
            "enable",
            "PWD",
            "password",
            "ALLOWCMD",
            "get",
            "DENYCMD",
            "set",
            "ALLOWCAT",
            "string",
            "DENYCAT",
            "hash",
            "DENYRKEY",
            r"foo\d+",
            "DENYWKEY",
            r"bar\d+",
            "DENYCHANNEL",
            "channel*",
        ]
        .as_ref(),
        &mut handler,
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(res.into_simple_string_unchecked(), "OK");

    let res = AclSetUser::test(
        [
            "user",
            "enable",
            "PWD",
            "password",
            "ALLOWCMD",
            "get",
            "DENYCMD",
            "set",
            "ALLOWCAT",
            "string",
            "DENYCAT",
            "hash",
            "DENYRKEY",
            r"foo\d+",
            "DENYWKEY",
            r"bar\d+",
            "DENYCHANNEL",
            "channel*",
        ]
        .as_ref(),
        &mut handler,
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(res.into_simple_string_unchecked(), "OK");

    {
        let default_ac = handler.shared.conf().security_conf().default_ac.clone();

        let security_conf = handler.shared.conf().security_conf();
        let acl = security_conf.acl.as_ref().unwrap();
        let user = acl.get("user".as_bytes()).unwrap();
        let user_ac = user.value();

        assert_eq!(default_ac.enable, user_ac.enable);
        assert_eq!(default_ac.password, user_ac.password);
        assert_eq!(
            default_ac
                .deny_read_key_patterns
                .as_ref()
                .unwrap()
                .patterns(),
            user_ac.deny_read_key_patterns.as_ref().unwrap().patterns()
        );
        assert_eq!(
            default_ac
                .deny_write_key_patterns
                .as_ref()
                .unwrap()
                .patterns(),
            user_ac.deny_write_key_patterns.as_ref().unwrap().patterns()
        );
        assert_eq!(
            default_ac
                .deny_channel_patterns
                .as_ref()
                .unwrap()
                .patterns(),
            user_ac.deny_channel_patterns.as_ref().unwrap().patterns()
        );
    }

    {
        let security_conf = handler.shared.conf().security_conf();
        let acl = security_conf.acl.as_ref().unwrap();
        let user = acl.get("user".as_bytes()).unwrap();
        let user_ac = user.value();

        assert!(user_ac.enable);
        assert_eq!(user_ac.password.as_ref(), b"password");

        assert!(!user_ac.is_forbidden_cmd(GET_CMD_FLAG));
        assert!(user_ac.is_forbidden_cmd(SET_CMD_FLAG));
        assert!(user_ac.is_forbidden_cmd(HDEL_CMD_FLAG));
        assert!(!user_ac.is_forbidden_cmd(MSET_CMD_FLAG));

        assert!(user_ac.deny_reading_or_writing_key(b"foo1", READ_CAT_FLAG));
        assert!(!user_ac.deny_reading_or_writing_key(b"foo", READ_CAT_FLAG));
        assert!(user_ac.deny_reading_or_writing_key(b"bar1", WRITE_CAT_FLAG));
        assert!(!user_ac.deny_reading_or_writing_key(b"bar", WRITE_CAT_FLAG));

        assert!(user_ac.is_forbidden_channel(b"channel"));
        assert!(!user_ac.is_forbidden_channel(b"chan"));
    }

    let res = AclUsers::test(Default::default(), &mut handler)
        .await
        .unwrap()
        .unwrap();

    let res = res.into_array_unchecked();
    assert!(res.contains(&Resp3::new_blob_string("default_ac")));
    assert!(res.contains(&Resp3::new_blob_string("user")));
    assert!(res.contains(&Resp3::new_blob_string(TEST_AC_USERNAME)));

    let res = AclWhoAmI::test(Default::default(), &mut handler)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res.into_blob_string_unchecked(), b"default_ac".as_ref());

    let res = AclDelUser::test(["user"].as_ref(), &mut handler)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(res.into_integer_unchecked(), 1);

    let res = AclUsers::test(Default::default(), &mut handler)
        .await
        .unwrap()
        .unwrap();
    let res = res.into_array_unchecked();
    assert!(res.contains(&Resp3::new_blob_string("default_ac")));
    assert!(res.contains(&Resp3::new_blob_string(TEST_AC_USERNAME)));
}
