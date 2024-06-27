use crate::{
    cmd::{cmd_name_to_flag, commands::*, CmdExecutor, CmdType},
    CmdFlag,
};
use bytes::{Bytes, BytesMut};
use dashmap::mapref::one::{Ref, RefMut};
use dashmap::DashMap;
use regex::bytes::RegexSet;
use serde::Deserialize;

const ACL_CAT_ADMIN_FLAG: CmdFlag = BgSave::FLAG;

const ACL_CAT_READ_FLAG: CmdFlag = Get::FLAG
    | GetRange::FLAG
    | MGet::FLAG
    | LLen::FLAG
    | LPos::FLAG
    | HGet::FLAG
    | HDel::FLAG
    | Exists::FLAG
    | Keys::FLAG
    | NBKeys::FLAG
    | Pttl::FLAG
    | Ttl::FLAG
    | Type::FLAG;

const ACL_CAT_WRITE_FLAG: CmdFlag = Set::FLAG
    | SetEx::FLAG
    | SetNx::FLAG
    | Append::FLAG
    | Incr::FLAG
    | IncrBy::FLAG
    | Decr::FLAG
    | DecrBy::FLAG
    | LPush::FLAG
    | LPop::FLAG
    | BLPop::FLAG
    | HSet::FLAG
    | HExists::FLAG
    | Expire::FLAG
    | ExpireAt::FLAG
    | ExpireTime::FLAG
    | Persist::FLAG
    | Publish::FLAG;

const ACL_CAT_CONNECTION_FLAG: CmdFlag =
    BgSave::FLAG | Ping::FLAG | Echo::FLAG | Auth::FLAG | ClientTracking::FLAG;

const ACL_CAT_KEYSPACE_FLAG: CmdFlag = Del::FLAG
    | Dump::FLAG
    | Exists::FLAG
    | Expire::FLAG
    | ExpireAt::FLAG
    | ExpireTime::FLAG
    | Keys::FLAG
    | NBKeys::FLAG
    | Persist::FLAG
    | Pttl::FLAG
    | Ttl::FLAG
    | Type::FLAG;

const ACL_CAT_STRING_FLAG: CmdFlag = Append::FLAG
    | Decr::FLAG
    | DecrBy::FLAG
    | Get::FLAG
    | GetRange::FLAG
    | GetSet::FLAG
    | Incr::FLAG
    | IncrBy::FLAG
    | MGet::FLAG
    | MSet::FLAG
    | MSetNx::FLAG
    | Set::FLAG
    | SetEx::FLAG
    | SetNx::FLAG
    | StrLen::FLAG;

const ACL_CAT_LIST_FLAG: CmdFlag =
    LLen::FLAG | LPush::FLAG | LPop::FLAG | BLPop::FLAG | LPos::FLAG | NBLPop::FLAG | BLMove::FLAG;

const ACL_CAT_HASH_FLAG: CmdFlag = HDel::FLAG | HExists::FLAG | HGet::FLAG | HSet::FLAG;

const ACL_CAT_PUBSUB_FLAG: CmdFlag = Publish::FLAG | Subscribe::FLAG | Unsubscribe::FLAG;

const ACL_CAT_SCRIPTING_FLAG: CmdFlag = Eval::FLAG | EvalName::FLAG | ScriptExists::FLAG;

#[derive(Debug, Deserialize, Default)]
#[serde(rename = "security")]
pub struct SecurityConf {
    pub requirepass: Option<String>, // 访问密码
    // TODO:
    #[serde(skip)]
    pub forbaiden_commands: Vec<bool>,
    // TODO:
    #[serde(skip)]
    pub rename_commands: Vec<Option<String>>,
    // TODO:
    pub default_ac: Option<AccessControl>,
    pub acl: Option<Acl>,
}

#[repr(transparent)]
#[derive(Debug, Deserialize, Default)]
pub struct Acl(DashMap<Bytes, AccessControl>);

impl Acl {
    pub fn new() -> Self {
        Self(DashMap::new())
    }

    pub fn get(&self, key: &Bytes) -> Option<Ref<'_, Bytes, AccessControl>> {
        if let Some(ac) = self.0.get(key) {
            if ac.enable {
                return Some(ac);
            }
        }

        None
    }

    pub fn get_mut(&self, key: &Bytes) -> Option<RefMut<'_, Bytes, AccessControl>> {
        if let Some(ac) = self.0.get_mut(key) {
            if ac.enable {
                return Some(ac);
            }
        }

        None
    }

    pub fn insert(&self, key: Bytes, value: AccessControl) {
        self.0.insert(key, value);
    }

    pub fn remove(&self, key: &Bytes) {
        self.0.remove(key);
    }

    pub fn disable(&self, key: &Bytes) {
        if let Some(mut ac) = self.0.get_mut(key) {
            ac.enable = false;
        }
    }

    pub fn enable(&self, key: &Bytes) {
        if let Some(mut ac) = self.0.get_mut(key) {
            ac.enable = true;
        }
    }

    pub fn is_enable(&self, key: &Bytes) -> bool {
        if let Some(ac) = self.0.get(key) {
            ac.enable
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct AccessControl {
    pub enable: bool,
    password: Bytes, // 空表示不需要密码
    // 用于记录客户端的命令权限，置0的位表示禁止的命令
    cmd_flag: CmdFlag,
    // 读取key的限制模式
    deny_read_key_patterns: Option<RegexSet>,
    // 写入key的限制模式
    deny_write_key_patterns: Option<RegexSet>,
}

impl AccessControl {
    pub const fn new(
        enable: bool,
        password: Bytes,
        cmd_flag: CmdFlag,
        deny_read_key_patterns: Option<RegexSet>,
        deny_write_key_patterns: Option<RegexSet>,
    ) -> Self {
        Self {
            enable,
            password,
            cmd_flag,
            deny_read_key_patterns,
            deny_write_key_patterns,
        }
    }

    pub const fn new_strict() -> Self {
        Self {
            enable: true,
            password: Bytes::new(),
            cmd_flag: NO_CMD_FLAG,
            deny_read_key_patterns: None,
            deny_write_key_patterns: None,
        }
    }

    pub const fn new_loose() -> Self {
        Self {
            enable: true,
            password: Bytes::new(),
            cmd_flag: ALL_CMD_FLAG,
            deny_read_key_patterns: None,
            deny_write_key_patterns: None,
        }
    }

    pub const fn cmd_flag(&self) -> CmdFlag {
        self.cmd_flag
    }

    // 密码是否正确
    #[inline]
    pub fn is_pwd_correct(&self, pwd: &Bytes) -> bool {
        self.password.is_empty() || self.password == *pwd
    }

    // 是否是禁用的命令
    pub const fn is_forbidden_cmd(&self, check: CmdFlag) -> bool {
        self.cmd_flag & check == 0
    }

    #[inline]
    pub fn is_forbidden_key(&self, key: &[u8], cmd_type: CmdType) -> bool {
        match cmd_type {
            CmdType::Read => {
                if let Some(patterns) = &self.deny_read_key_patterns {
                    patterns.is_match(key)
                } else {
                    false
                }
            }
            CmdType::Write => {
                if let Some(patterns) = &self.deny_write_key_patterns {
                    patterns.is_match(key)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    #[inline]
    pub fn is_forbidden_keys(&self, keys: &[impl AsRef<[u8]>], cmd_type: CmdType) -> bool {
        match cmd_type {
            CmdType::Read => {
                if let Some(patterns) = &self.deny_read_key_patterns {
                    return keys.iter().any(|key| patterns.is_match(key.as_ref()));
                }
            }
            CmdType::Write => {
                if let Some(patterns) = &self.deny_write_key_patterns {
                    return keys.iter().any(|key| patterns.is_match(key.as_ref()));
                }
            }
            _ => {}
        }

        false
    }
}

impl<'de> Deserialize<'de> for AccessControl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(default)]
        struct AccessControlIntermedium {
            enable: bool,
            password: Bytes,
            allow_commands: Option<Vec<BytesMut>>,
            deny_commands: Option<Vec<BytesMut>>,
            allow_categories: Option<Vec<BytesMut>>,
            deny_categories: Option<Vec<BytesMut>>,
            deny_read_key_patterns: Option<Vec<String>>,
            deny_write_key_patterns: Option<Vec<String>>,
        }

        impl Default for AccessControlIntermedium {
            fn default() -> Self {
                Self {
                    enable: true,
                    password: Default::default(),
                    allow_commands: None,
                    deny_commands: None,
                    allow_categories: None,
                    deny_categories: None,
                    deny_read_key_patterns: None,
                    deny_write_key_patterns: None,
                }
            }
        }

        let ac = AccessControlIntermedium::deserialize(deserializer)?;

        let cat_name_to_cat_flag = |cat_name: &mut BytesMut| -> Result<CmdFlag, D::Error> {
            cat_name.make_ascii_uppercase();

            match cat_name.as_ref() {
                b"CONNECTION" => Ok(ACL_CAT_CONNECTION_FLAG),
                b"ADMIN" => Ok(ACL_CAT_ADMIN_FLAG),
                b"READ" => Ok(ACL_CAT_READ_FLAG),
                b"WRITE" => Ok(ACL_CAT_WRITE_FLAG),
                b"KEYSPACE" => Ok(ACL_CAT_KEYSPACE_FLAG),
                b"STRING" => Ok(ACL_CAT_STRING_FLAG),
                b"LIST" => Ok(ACL_CAT_LIST_FLAG),
                b"HASH" => Ok(ACL_CAT_HASH_FLAG),
                b"PUBSUB" => Ok(ACL_CAT_PUBSUB_FLAG),
                b"SCRIPTING" => Ok(ACL_CAT_SCRIPTING_FLAG),
                _ => Err(serde::de::Error::custom("unknown category")),
            }
        };

        let mut cmd_flag = NO_CMD_FLAG;

        if let Some(mut allow_categories) = ac.allow_categories {
            for category_name in &mut allow_categories {
                let flag = cat_name_to_cat_flag(category_name).map_err(serde::de::Error::custom)?;

                cmd_flag |= flag; // 允许某类命令执行
            }
        }

        if let Some(mut allow_cmds) = ac.allow_commands {
            for cmd_name in &mut allow_cmds {
                if cmd_name.as_ref().eq_ignore_ascii_case(b"ALL") {
                    cmd_flag = ALL_CMD_FLAG; // 允许所有命令执行，后面的命令无效
                    break;
                }
                let flag = cmd_name_to_flag(cmd_name).map_err(serde::de::Error::custom)?;
                cmd_flag |= flag; // 允许命令执行
            }
        }

        // 禁止的优先级高于允许的

        if let Some(mut deny_categories) = ac.deny_categories {
            for category_name in &mut deny_categories {
                let flag = cat_name_to_cat_flag(category_name).map_err(serde::de::Error::custom)?;

                cmd_flag &= !flag; // 禁止某类命令执行
            }
        }

        if let Some(mut deny_cmds) = ac.deny_commands {
            for cmd_name in &mut deny_cmds {
                if cmd_name.as_ref().eq_ignore_ascii_case(b"ALL") {
                    cmd_flag = NO_CMD_FLAG; // 禁止所有命令执行，后面的命令无效
                    break;
                }

                let flag = cmd_name_to_flag(cmd_name).map_err(serde::de::Error::custom)?;
                cmd_flag &= !flag; // 禁止命令执行
            }
        }

        let deny_read_key_patterns = if let Some(patterns) = ac.deny_read_key_patterns {
            Some(RegexSet::new(patterns).map_err(serde::de::Error::custom)?)
        } else {
            None
        };

        let deny_write_key_patterns = if let Some(patterns) = ac.deny_write_key_patterns {
            Some(RegexSet::new(patterns).map_err(serde::de::Error::custom)?)
        } else {
            None
        };

        Ok(AccessControl {
            enable: ac.enable,
            password: ac.password,
            cmd_flag,
            deny_read_key_patterns,
            deny_write_key_patterns,
        })
    }
}
