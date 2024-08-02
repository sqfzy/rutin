use crate::{
    cmd::commands::*,
    error::{RutinError, RutinResult},
};
use arc_swap::ArcSwap;
use bytes::Bytes;
use dashmap::{
    iter::Iter,
    mapref::one::{Ref, RefMut},
};
use dashmap::{iter::IterMut, DashMap};
use derive_builder::Builder;
use regex::bytes::RegexSet;
use serde::Deserialize;

pub const DEFAULT_USER: Bytes = Bytes::from_static(b"default_ac");

#[derive(Debug, Deserialize)]
pub struct SecurityConf {
    pub requirepass: Option<String>, // 访问密码
    // TODO:
    #[serde(skip)]
    pub rename_commands: Vec<Option<String>>,
    pub default_ac: ArcSwap<AccessControl>,
    pub acl: Option<Acl>, // None代表禁用ACL
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

    pub fn remove(&self, key: &Bytes) -> Option<(Bytes, AccessControl)> {
        self.0.remove(key)
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

    pub fn iter(&self) -> Iter<'_, Bytes, AccessControl> {
        self.0.iter()
    }

    pub fn iter_mut(&self) -> IterMut<'_, Bytes, AccessControl> {
        self.0.iter_mut()
    }

    pub fn clear(&self) {
        self.0.clear();
    }
}

#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct AccessControl {
    pub enable: bool,
    pub password: Bytes, // 空表示不需要密码
    // 用于记录客户端的命令权限，置0的位表示禁止的命令
    pub cmds_flag: Flag,
    // 读取key的限制模式
    pub deny_read_key_patterns: Option<RegexSet>,
    // 写入key的限制模式
    pub deny_write_key_patterns: Option<RegexSet>,
    // pubsub的限制模式
    pub deny_channel_patterns: Option<RegexSet>,
}

impl AccessControl {
    pub const fn new_strict() -> Self {
        Self {
            enable: true,
            password: Bytes::new(),
            cmds_flag: NO_CMD_FLAG,
            deny_read_key_patterns: None,
            deny_write_key_patterns: None,
            deny_channel_patterns: None,
        }
    }

    pub const fn new_loose() -> Self {
        Self {
            enable: true,
            password: Bytes::new(),
            cmds_flag: ALL_CMD_FLAG,
            deny_read_key_patterns: None,
            deny_write_key_patterns: None,
            deny_channel_patterns: None,
        }
    }

    pub fn set_only_read(&mut self) {
        self.cmds_flag = READ_CAT_FLAG;
    }

    pub fn set_only_write(&mut self) {
        self.cmds_flag = WRITE_CAT_FLAG;
    }

    pub fn merge(&mut self, mut other: AccessControlIntermedium) -> RutinResult<()> {
        if let Some(enable) = other.enable {
            self.enable = enable;
        }
        if let Some(password) = other.password.take() {
            if password.eq_ignore_ascii_case(b"RESET") {
                self.password.clear();
            } else {
                self.password = password;
            }
        }

        if let Some(allow_categories) = other.allow_categories {
            for category_name in &allow_categories {
                let flag = cat_name_to_flag(category_name)?;

                self.cmds_flag |= flag; // 允许某类命令执行
            }
        }

        if let Some(allow_cmds) = other.allow_commands {
            for cmd_name in &allow_cmds {
                if cmd_name.eq_ignore_ascii_case(b"ALL") {
                    self.cmds_flag = ALL_CMD_FLAG; // 允许所有命令执行，后面的命令无效
                    break;
                }
                let flag = cmd_name_to_flag(cmd_name)?;
                self.cmds_flag |= flag; // 允许命令执行
            }
        }

        // 禁止的优先级高于允许的

        if let Some(deny_categories) = other.deny_categories {
            for category_name in &deny_categories {
                let flag = cat_name_to_flag(category_name)?;

                self.cmds_flag &= !flag; // 禁止某类命令执行
            }
        }

        if let Some(deny_cmds) = other.deny_commands {
            for cmd_name in &deny_cmds {
                if cmd_name.eq_ignore_ascii_case(b"ALL") {
                    self.cmds_flag = NO_CMD_FLAG; // 禁止所有命令执行，后面的命令无效
                    break;
                }

                let flag = cmd_name_to_flag(cmd_name)?;
                self.cmds_flag &= !flag; // 禁止命令执行
            }
        }

        // 合并deny_read_key_patterns
        if let (Some(patterns), Some(other_patterns)) = (
            &self.deny_read_key_patterns,
            other.deny_read_key_patterns.as_mut(),
        ) {
            other_patterns.extend_from_slice(patterns.patterns());

            if other_patterns
                .iter()
                .any(|p| p.eq_ignore_ascii_case("RESET"))
            {
                // 重置
                self.deny_read_key_patterns = None;
            } else {
                self.deny_read_key_patterns = Some(RegexSet::new(other_patterns)?);
            }
        } else if let Some(patterns) = other.deny_read_key_patterns {
            self.deny_read_key_patterns = Some(RegexSet::new(patterns)?);
        }

        // 合并deny_write_key_patterns
        if let (Some(patterns), Some(other_patterns)) = (
            &self.deny_write_key_patterns,
            other.deny_write_key_patterns.as_mut(),
        ) {
            other_patterns.extend_from_slice(patterns.patterns());

            if other_patterns
                .iter()
                .any(|p| p.eq_ignore_ascii_case("RESET"))
            {
                // 重置
                self.deny_write_key_patterns = None;
            } else {
                self.deny_write_key_patterns = Some(RegexSet::new(other_patterns)?);
            }
        } else if let Some(patterns) = other.deny_write_key_patterns {
            self.deny_write_key_patterns = Some(RegexSet::new(patterns)?);
        }

        // 合并deny_channel_patterns
        if let (Some(patterns), Some(other_patterns)) = (
            &self.deny_channel_patterns,
            other.deny_channel_patterns.as_mut(),
        ) {
            other_patterns.extend_from_slice(patterns.patterns());

            if other_patterns
                .iter()
                .any(|p| p.eq_ignore_ascii_case("RESET"))
            {
                // 重置
                self.deny_channel_patterns = None;
            } else {
                self.deny_channel_patterns = Some(RegexSet::new(other_patterns)?);
            }
        } else if let Some(patterns) = other.deny_channel_patterns {
            self.deny_channel_patterns = Some(RegexSet::new(patterns)?);
        }

        Ok(())
    }

    pub const fn cmd_flag(&self) -> Flag {
        self.cmds_flag
    }

    // 密码是否正确
    pub fn is_pwd_correct(&self, pwd: &Bytes) -> bool {
        if !self.enable {
            return false;
        }
        self.password.is_empty() || self.password == *pwd
    }

    // 是否是禁用的命令
    pub const fn is_forbidden_cmd(&self, check: Flag) -> bool {
        if !self.enable {
            return true;
        }
        self.cmds_flag & check == 0
    }

    #[inline]
    pub fn is_forbidden_key(&self, key: &dyn AsRef<[u8]>, cats_flag: Flag) -> bool {
        if !self.enable {
            return true;
        }

        if cmds_contains_cmds(cats_flag, READ_CAT_FLAG) {
            if let Some(patterns) = &self.deny_read_key_patterns {
                patterns.is_match(key.as_ref())
            } else {
                false
            }
        } else if cmds_contains_cmds(cats_flag, WRITE_CAT_FLAG) {
            if let Some(patterns) = &self.deny_write_key_patterns {
                patterns.is_match(key.as_ref())
            } else {
                false
            }
        } else {
            unreachable!()
        }
    }

    pub fn is_forbidden_channel(&self, channel: &dyn AsRef<[u8]>) -> bool {
        if !self.enable {
            return true;
        }

        if let Some(patterns) = &self.deny_channel_patterns {
            patterns.is_match(channel.as_ref())
        } else {
            false
        }
    }
}

impl<'de> Deserialize<'de> for AccessControl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ac = AccessControlIntermedium::deserialize(deserializer)?;

        ac.try_into().map_err(serde::de::Error::custom)
    }
}

impl Default for AccessControl {
    fn default() -> Self {
        Self::new_strict()
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
pub struct AccessControlIntermedium {
    pub enable: Option<bool>,
    pub password: Option<Bytes>,
    pub allow_commands: Option<Vec<Bytes>>,
    pub deny_commands: Option<Vec<Bytes>>,
    pub allow_categories: Option<Vec<Bytes>>,
    pub deny_categories: Option<Vec<Bytes>>,
    pub deny_read_key_patterns: Option<Vec<String>>,
    pub deny_write_key_patterns: Option<Vec<String>>,
    pub deny_channel_patterns: Option<Vec<String>>,
}

impl TryFrom<AccessControlIntermedium> for AccessControl {
    type Error = RutinError;

    fn try_from(aci: AccessControlIntermedium) -> Result<Self, Self::Error> {
        let mut ac = AccessControl::new_strict();
        ac.merge(aci)?;

        Ok(ac)
    }
}
