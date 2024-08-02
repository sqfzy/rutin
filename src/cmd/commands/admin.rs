use std::sync::Arc;

use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::{AccessControl, AccessControlIntermedium, DEFAULT_USER},
    connection::AsyncStream,
    frame::Resp3,
    server::Handler,
    util::{set_server_to_master, set_server_to_replica},
};
use crate::{
    error::{RutinError, RutinResult},
    persist::rdb::Rdb,
};
use backon::Retryable;
use bytes::Bytes;
use bytestring::ByteString;
use itertools::Itertools;
use tokio::net::TcpStream;
use tracing::{error, instrument};

/// # Reply:
///
/// Array reply: an array of Bulk string reply elements representing ACL categories or commands in a given category.
/// Simple error reply: the command returns an error if an invalid category name is given.
#[derive(Debug)]
pub struct AclCat {
    pub category: Option<Bytes>,
}

impl CmdExecutor for AclCat {
    const NAME: &'static str = "ACLCAT";
    const CATS_FLAG: Flag = ACLCAT_CATS_FLAG;
    const CMD_FLAG: Flag = ACLCAT_CMD_FLAG;

    #[instrument(level = "debug", skip(_handler), ret, err)]
    async fn execute(self, _handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let res: Vec<Resp3<Bytes, ByteString>> = if let Some(cat) = self.category {
            let cat = cat_name_to_flag(cat.as_ref())?;

            cmds_flag_to_names(cat)
                .iter_mut()
                .map(|s| Resp3::new_blob_string(s.as_bytes().into()))
                .collect_vec()
        } else {
            cmds_flag_to_names(ALL_CMD_FLAG)
                .iter_mut()
                .map(|s| Resp3::new_blob_string(s.as_bytes().into()))
                .collect_vec()
        };

        Ok(Some(Resp3::new_array(res)))
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
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
pub struct AclDelUser {
    pub users: Vec<Bytes>,
}

impl CmdExecutor for AclDelUser {
    const NAME: &'static str = "ACLDELUSER";
    const CATS_FLAG: Flag = ACLDELUSER_CATS_FLAG;
    const CMD_FLAG: Flag = ACLDELUSER_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut count = 0;

        if let Some(acl) = &handler.shared.conf().security.acl {
            for user in self.users {
                if acl.remove(&user).is_some() {
                    count += 1;
                }
            }
        }

        Ok(Some(Resp3::new_integer(count)))
    }

    fn parse(args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
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
pub struct AclSetUser {
    pub name: Bytes,
    pub aci: AccessControlIntermedium,
}

impl CmdExecutor for AclSetUser {
    const NAME: &'static str = "ACLSETUSER";
    const CATS_FLAG: Flag = ACLSETUSER_CATS_FLAG;
    const CMD_FLAG: Flag = ACLSETUSER_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let security = &handler.shared.conf().security;
        if self.name == DEFAULT_USER {
            // 如果是default_ac则clone后合并，再放回
            let default_ac = security.default_ac.load();
            let mut default_ac = AccessControl::clone(&default_ac);

            default_ac.merge(self.aci)?;

            security.default_ac.store(std::sync::Arc::new(default_ac));
        } else if let Some(acl) = &handler.shared.conf().security.acl {
            if let Some(mut ac) = acl.get_mut(&self.name) {
                // 如果存在则合并
                ac.merge(self.aci)?;
            } else {
                // 不存在则插入
                acl.insert(self.name, self.aci.try_into()?);
            }
        }

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
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
        while let Some(b) = args.next() {
            match b.as_ref() {
                b"enable" => aci.enable = Some(true),
                b"disable" => aci.enable = Some(false),
                b"PWD" => aci.password = Some(args.next().unwrap()),
                // collect 从ALLOWCMD开始直到某个cmd的末尾不带','则结束
                b"ALLOWCMD" => {
                    let mut allow_commands = Vec::with_capacity(10);
                    for mut b in args.by_ref() {
                        if b.last().is_some_and(|b| *b == b',') {
                            allow_commands.push(b.split_off(b.len() - 1));
                        } else {
                            allow_commands.push(b);
                            break;
                        }
                    }
                    aci.allow_commands = Some(allow_commands.clone());
                }
                b"DENYCMD" => {
                    let mut deny_commands = Vec::with_capacity(10);
                    for mut b in args.by_ref() {
                        if b.last().is_some_and(|b| *b == b',') {
                            deny_commands.push(b.split_off(b.len() - 1));
                        } else {
                            deny_commands.push(b);
                            break;
                        }
                    }
                    aci.deny_commands = Some(deny_commands.clone());
                }
                b"ALLOWCAT" => {
                    let mut allow_categories = Vec::with_capacity(10);
                    for mut b in args.by_ref() {
                        if b.last().is_some_and(|b| *b == b',') {
                            allow_categories.push(b.split_off(b.len() - 1));
                        } else {
                            allow_categories.push(b);
                            break;
                        }
                    }
                    aci.allow_categories = Some(allow_categories.clone());
                }
                b"DENYCAT" => {
                    let mut deny_categories = Vec::with_capacity(10);
                    for mut b in args.by_ref() {
                        if b.last().is_some_and(|b| *b == b',') {
                            deny_categories.push(b.split_off(b.len() - 1));
                        } else {
                            deny_categories.push(b);
                            break;
                        }
                    }
                    aci.deny_categories = Some(deny_categories.clone());
                }
                b"DENYRKEY" => {
                    let mut deny_read_key_patterns = Vec::with_capacity(10);
                    for b in args.by_ref() {
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

impl CmdExecutor for AclUsers {
    const NAME: &'static str = "ACLUSERS";
    const CATS_FLAG: Flag = ACLUSERS_CATS_FLAG;
    const CMD_FLAG: Flag = ACLUSERS_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut users = vec![Resp3::new_blob_string(DEFAULT_USER)];
        if let Some(acl) = &handler.shared.conf().security.acl {
            users.extend(acl.iter().map(|e| Resp3::new_blob_string(e.key().clone())));
        }

        Ok(Some(Resp3::new_array(users)))
    }

    fn parse(_args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        Ok(AclUsers)
    }
}

/// # Reply:
///
/// Bulk string reply: the username of the current connection.
#[derive(Debug)]
pub struct AclWhoAmI;

impl CmdExecutor for AclWhoAmI {
    const NAME: &'static str = "ACLWHOAMI";
    const CATS_FLAG: Flag = ACLWHOAMI_CATS_FLAG;
    const CMD_FLAG: Flag = ACLWHOAMI_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        Ok(Some(Resp3::new_blob_string(handler.context.user.clone())))
    }

    fn parse(_args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        Ok(AclWhoAmI)
    }
}

// 该命令用于在后台异步保存当前数据库的数据到磁盘
/// # Reply:
///
/// **Simple string reply:** Background saving started.
/// **Simple string reply:** Background saving scheduled.
#[derive(Debug)]
pub struct BgSave;

impl CmdExecutor for BgSave {
    const NAME: &'static str = "BGSAVE";
    const CATS_FLAG: Flag = BGSAVE_CATS_FLAG;
    const CMD_FLAG: Flag = BGSAVE_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
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

    fn parse(args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
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
pub struct PSync {
    pub replication_id: Bytes,
    pub offset: u64,
}

impl CmdExecutor for PSync {
    const NAME: &'static str = "PSYNC";
    const CATS_FLAG: Flag = PSYNC_CATS_FLAG;
    const CMD_FLAG: Flag = PSYNC_CMD_FLAG;

    async fn execute(self, _handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        todo!()
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        Ok(PSync {
            replication_id: args.next().unwrap(),
            offset: util::atoi(&args.next().unwrap())?,
        })
    }
}

/// # Reply:
///
/// **Simple string reply**: OK.
#[derive(Debug)]
pub struct ReplicaOf {
    should_set_to_master: bool,
    master_host: ByteString,
    master_port: u16,
}

impl CmdExecutor for ReplicaOf {
    const NAME: &'static str = "REPLICAOF";
    const CATS_FLAG: Flag = REPLICAOF_CATS_FLAG;
    const CMD_FLAG: Flag = REPLICAOF_CMD_FLAG;

    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        if self.should_set_to_master {
            set_server_to_master(&handler.shared);

            return Ok(Some(Resp3::new_simple_string("OK".into())));
        }

        set_server_to_replica(&handler.shared, self.master_host, self.master_port).await?;

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(mut args: CmdUnparsed, _ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        };

        let arg1 = args.next().unwrap();
        let arg2 = args.next().unwrap();

        if arg1 == "NO" && arg2 == "ONE" {
            return Ok(ReplicaOf {
                should_set_to_master: true,
                master_host: Default::default(),
                master_port: 0,
            });
        }

        Ok(ReplicaOf {
            should_set_to_master: false,
            master_host: args
                .next()
                .unwrap()
                .try_into()
                .map_err(|_| RutinError::from("ERR value is not a valid hostname or ip address"))?,
            master_port: util::atoi(&args.next().unwrap())?,
        })
    }
}

#[tokio::test]
async fn cmd_acl_tests() {
    use crate::util::TEST_AC_USERNAME;
    crate::util::test_init();

    let mut handler = Handler::new_fake().0;

    let acl_set_user = AclSetUser::parse(
        CmdUnparsed::from(
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
        ),
        &AccessControl::new_loose(),
    )
    .unwrap();

    let resp = acl_set_user.execute(&mut handler).await.unwrap().unwrap();
    assert_eq!(resp.as_simple_string_uncheckd(), "OK");

    let acl_set_user = AclSetUser::parse(
        CmdUnparsed::from(
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
        ),
        &AccessControl::new_loose(),
    )
    .unwrap();

    let resp = acl_set_user.execute(&mut handler).await.unwrap().unwrap();
    assert_eq!(resp.as_simple_string_uncheckd(), "OK");

    {
        let default_ac = handler.shared.conf().security.default_ac.load();
        let user_ac = handler
            .shared
            .conf()
            .security
            .acl
            .as_ref()
            .unwrap()
            .get(&"user".into())
            .unwrap();

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
        let user = handler
            .shared
            .conf()
            .security
            .acl
            .as_ref()
            .unwrap()
            .get(&"user".into())
            .unwrap();
        let ac = user.value();

        assert!(ac.enable);
        assert_eq!(ac.password.as_ref(), b"password");

        assert!(!ac.is_forbidden_cmd(Get::CMD_FLAG));
        assert!(ac.is_forbidden_cmd(Set::CMD_FLAG));
        assert!(ac.is_forbidden_cmd(HDel::CMD_FLAG));
        assert!(!ac.is_forbidden_cmd(MSet::CMD_FLAG));

        assert!(ac.is_forbidden_key(b"foo1", READ_CAT_FLAG));
        assert!(!ac.is_forbidden_key(b"foo", READ_CAT_FLAG));
        assert!(ac.is_forbidden_key(b"bar1", WRITE_CAT_FLAG));
        assert!(!ac.is_forbidden_key(b"bar", WRITE_CAT_FLAG));

        assert!(ac.is_forbidden_channel(b"channel"));
        assert!(!ac.is_forbidden_channel(b"chan"));
    }

    let acl_users = AclUsers::parse(CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();

    let resp = acl_users.execute(&mut handler).await.unwrap().unwrap();
    let res = resp.as_array_uncheckd();
    assert!(res.contains(&Resp3::new_blob_string("default_ac".into())));
    assert!(res.contains(&Resp3::new_blob_string("user".into())));
    assert!(res.contains(&Resp3::new_blob_string(TEST_AC_USERNAME.into())));

    let acl_whoami = AclWhoAmI::parse(CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();
    AclWhoAmI::parse(CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();

    let resp = acl_whoami.execute(&mut handler).await.unwrap().unwrap();
    assert_eq!(resp.as_blob_string_uncheckd(), "default_ac");

    let acl_deluser = AclDelUser::parse(
        CmdUnparsed::from(["user"].as_ref()),
        &AccessControl::new_loose(),
    )
    .unwrap();

    let resp = acl_deluser.execute(&mut handler).await.unwrap().unwrap();
    assert_eq!(resp.as_integer_uncheckd(), 1);

    let acl_users = AclUsers::parse(CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();

    let resp = acl_users.execute(&mut handler).await.unwrap().unwrap();
    let res = resp.as_array_uncheckd();
    assert!(res.contains(&Resp3::new_blob_string("default_ac".into())));
    assert!(res.contains(&Resp3::new_blob_string(TEST_AC_USERNAME.into())));
}
