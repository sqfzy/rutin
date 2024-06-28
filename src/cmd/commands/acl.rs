use super::*;
use crate::{
    cmd::{flag_to_cmd_names, CmdError, CmdExecutor, CmdType, CmdUnparsed, Err},
    conf::{AccessControl, AccessControlIntermedium, ACL_CATEGORIES, DEFAULT_USER},
    connection::AsyncStream,
    frame::Resp3,
    server::Handler,
    CmdFlag,
};
use bytes::Bytes;
use bytestring::ByteString;
use tracing::instrument;

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
    const TYPE: CmdType = CmdType::Read;
    const FLAG: CmdFlag = ACLCAT_FLAG;

    #[instrument(level = "debug", skip(_handler), ret, err)]
    async fn execute(
        self,
        _handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let res: Vec<Resp3<Bytes, ByteString>> = if let Some(cat) = self.category {
            let cat = ACL_CATEGORIES
                .iter()
                .find(|&c| c.name.as_bytes() == cat.as_ref())
                .ok_or_else(|| CmdError::from("ERR unknow category"))?;

            flag_to_cmd_names(cat.flag)?
                .into_iter()
                .map(|s| Resp3::new_blob_string(s.into()))
                .collect()
        } else {
            ACL_CATEGORIES
                .iter()
                .map(|cat| Resp3::new_blob_string(cat.name.into()))
                .collect()
        };

        Ok(Some(Resp3::new_array(res)))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 && args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(AclCat {
            category: args.next(),
        })
    }
}

/// # Reply:
///
/// Integer reply: the number of users that were deleted. This number will not always match the number of arguments since certain users may not exist.
#[derive(Debug)]
pub struct AclDelUser {
    pub users: Vec<Bytes>,
}

impl CmdExecutor for AclDelUser {
    const NAME: &'static str = "ACLDELUSER";
    const TYPE: CmdType = CmdType::Write;
    const FLAG: CmdFlag = ACLDELUSER_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut count = 0;

        if let Some(acl) = &handler.shared.conf().security.acl {
            for user in self.users {
                if acl.remove(&user).is_some() {
                    count += 1;
                }
            }
        }

        Ok(Some(Resp3::new_integer(count as i64)))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if args.is_empty() {
            return Err(Err::WrongArgNum.into());
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
/// RESET代表清空，不支持修改default_ac
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
    const TYPE: CmdType = CmdType::Write;
    const FLAG: CmdFlag = ACLSETUSER_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        if let Some(acl) = &handler.shared.conf().security.acl {
            if let Some(mut ac) = acl.get_mut(&self.name) {
                // 如果存在则合并
                ac.merge(self.aci).map_err(CmdError::from)?;
            } else {
                // 不存在则插入
                acl.insert(self.name, self.aci.try_into().map_err(CmdError::from)?);
            }
        }

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        if args.is_empty()
            && args.len() != 2
            && args.len() != 4
            && args.len() != 6
            && args.len() != 8
            && args.len() != 10
            && args.len() != 12
        {
            return Err(Err::WrongArgNum.into());
        }

        let name = args.next().unwrap();
        // 不支持修改default_ac
        if name.as_ref() == b"default_ac" {
            return Err("ERR default_ac is read-only".into());
        }

        let mut aci = AccessControlIntermedium::default();

        let args = args.into_iter();

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
                            deny_read_key_patterns.push(
                                String::from_utf8(b[..b.len() - 1].to_vec())
                                    .map_err(|_| Err::Syntax)?,
                            );
                        } else {
                            deny_read_key_patterns
                                .push(String::from_utf8(b.to_vec()).map_err(|_| Err::Syntax)?);
                            break;
                        }
                    }
                    aci.deny_read_key_patterns = Some(deny_read_key_patterns);
                }
                b"DENYWKEY" => {
                    let mut deny_write_key_patterns = Vec::with_capacity(10);
                    for b in args.by_ref() {
                        if b.last().is_some_and(|b| *b == b',') {
                            deny_write_key_patterns.push(
                                String::from_utf8(b[..b.len() - 1].to_vec())
                                    .map_err(|_| Err::Syntax)?,
                            );
                        } else {
                            deny_write_key_patterns
                                .push(String::from_utf8(b.to_vec()).map_err(|_| Err::Syntax)?);
                            break;
                        }
                    }
                    aci.deny_write_key_patterns = Some(deny_write_key_patterns);
                }
                b"DENYCHANNEL" => {
                    let mut deny_channel_patterns = Vec::with_capacity(10);
                    for b in args.by_ref() {
                        if b.last().is_some_and(|b| *b == b',') {
                            deny_channel_patterns.push(
                                String::from_utf8(b[..b.len() - 1].to_vec())
                                    .map_err(|_| Err::Syntax)?,
                            );
                        } else {
                            deny_channel_patterns
                                .push(String::from_utf8(b.to_vec()).map_err(|_| Err::Syntax)?);
                            break;
                        }
                    }
                    aci.deny_channel_patterns = Some(deny_channel_patterns);
                }
                _ => return Err(Err::Syntax.into()),
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
    const TYPE: CmdType = CmdType::Read;
    const FLAG: CmdFlag = ACLUSERS_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut users = vec![Resp3::new_blob_string(DEFAULT_USER)];
        if let Some(acl) = &handler.shared.conf().security.acl {
            users.extend(acl.iter().map(|e| Resp3::new_blob_string(e.key().clone())));
        }

        Ok(Some(Resp3::new_array(users)))
    }

    fn parse(_args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
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
    const TYPE: CmdType = CmdType::Read;
    const FLAG: CmdFlag = ACLWHOAMI_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        Ok(Some(Resp3::new_blob_string(handler.context.user.clone())))
    }

    fn parse(_args: &mut CmdUnparsed, _ac: &AccessControl) -> Result<Self, CmdError> {
        Ok(AclWhoAmI)
    }
}

#[tokio::test]
async fn cmd_acl_tests() {
    crate::util::test_init();

    let mut handler = Handler::new_fake().0;

    let acl_set_user = AclSetUser::parse(
        &mut CmdUnparsed::from(
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

        assert!(!ac.is_forbidden_cmd(Get::FLAG));
        assert!(ac.is_forbidden_cmd(Set::FLAG));
        assert!(ac.is_forbidden_cmd(HDel::FLAG));
        assert!(!ac.is_forbidden_cmd(MSet::FLAG));

        assert!(ac.is_forbidden_key(b"foo1", CmdType::Read));
        assert!(!ac.is_forbidden_key(b"foo", CmdType::Read));
        assert!(ac.is_forbidden_key(b"bar1", CmdType::Write));
        assert!(!ac.is_forbidden_key(b"bar", CmdType::Write));

        assert!(ac.is_forbidden_channel(b"channel"));
        assert!(!ac.is_forbidden_channel(b"chan"));
    }

    let acl_users =
        AclUsers::parse(&mut CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();

    let resp = acl_users.execute(&mut handler).await.unwrap().unwrap();
    assert_eq!(
        resp.as_array_uncheckd(),
        &vec![
            Resp3::new_blob_string("default_ac".into()),
            Resp3::new_blob_string("user".into()),
        ]
    );

    let acl_whoami =
        AclWhoAmI::parse(&mut CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();
    AclWhoAmI::parse(&mut CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();

    let resp = acl_whoami.execute(&mut handler).await.unwrap().unwrap();
    assert_eq!(resp.as_blob_string_uncheckd(), "default_ac");

    let acl_deluser = AclDelUser::parse(
        &mut CmdUnparsed::from(["user"].as_ref()),
        &AccessControl::new_loose(),
    )
    .unwrap();

    let resp = acl_deluser.execute(&mut handler).await.unwrap().unwrap();
    assert_eq!(resp.as_integer_uncheckd(), 1);

    let acl_users =
        AclUsers::parse(&mut CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();

    let resp = acl_users.execute(&mut handler).await.unwrap().unwrap();
    assert_eq!(
        resp.as_array_uncheckd(),
        &vec![Resp3::new_blob_string("default_ac".into()),]
    );
}
