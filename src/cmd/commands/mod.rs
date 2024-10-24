//! <cat_name>_CAT_FLAG代表单个分类标志
//! <cmd_name>_CATS_FLAG代表命令所属的分类标志，或者只是一个普通的分类标志组，每一位代表一个分类
//!
//! <cmd_name>_CMD_FLAG代表单个命令标志
//! <cat_name>_CMDS_FLAG代表分类标志下的所有命令标志，或者只是一个普通的命令组，每一位代表一个命令

mod admin;
mod connection;
mod hash;
mod key;
mod list;
mod pub_sub;
mod script;
// mod set;
mod str;
// mod zset;

pub use admin::*;
pub use connection::*;
pub use hash::*;
pub use key::*;
pub use list::*;
pub use pub_sub::*;
pub use script::*;
pub use str::*;

use super::*;
use crate::error::{RutinError, RutinResult};
use bytes::Bytes;
use rutin_proc::gen_flag;

pub type CmdFlag = u128;
pub type CatFlag = u32;

gen_flag!(
    /*********/
    /* admin */
    /*********/
    AclCat<A>(admin, dangerous),
    AclDelUser<A>(admin, dangerous),
    AclSetUser<A>(admin, dangerous),
    AclUsers(admin, dangerous),
    AclWhoAmI(admin, dangerous),
    // AppendOnly(admin, dangerous),
    BgSave(admin, dangerous),
    PSync<A>(admin, dangerous),
    ReplConf(admin, dangerous),
    ReplicaOf(admin, dangerous),
    Save(admin, dangerous),
    /**************/
    /* connection */
    /**************/
    Auth<A>(connection),
    ClientTracking(connection),
    Echo<A>(connection),
    Ping<A>(connection),
    /************/
    /* keyspace */
    /************/
    Del<A>(keyspace, write),
    Dump<A>(keyspace, read),
    Exists<A>(keyspace, read),
    Expire<A>(keyspace, write),
    ExpireAt<A>(keyspace, write),
    ExpireTime<A>(keyspace, read),
    Keys<A>(keyspace, read),
    // NBKeys(keyspace, read),
    Persist<A>(keyspace, write),
    Pttl<A>(keyspace, read),
    Ttl<A>(keyspace, read),
    Type<A>(keyspace, read),
    /**********/
    /* string */
    /**********/
    Append<A>(string, write),
    Decr<A>(string, write),
    DecrBy<A>(string, write),
    Get<A>(string, read),
    GetRange<A>(string, read),
    GetSet<A>(string, write),
    Incr<A>(string, write),
    IncrBy<A>(string, write),
    MGet<A>(string, read),
    MSet<A>(string, write),
    MSetNx<A>(string, write),
    Set<A>(string, write),
    SetEx<A>(string, write),
    SetNx<A>(string, write),
    StrLen<A>(string, read),
    /********/
    /* list */
    /********/
    BLMove<A>(list, write),
    BLPop<A>(list, write),
    LLen<A>(list, read),
    LPop<A>(list, write),
    LPos<A>(list, read),
    LPush<A>(list, write),
    // NBLPop(list, write),
    /********/
    /* hash */
    /********/
    HDel<A>(hash, write),
    HExists<A>(hash, read),
    HGet<A>(hash, read),
    HSet<A>(hash, write),
    /**********/
    /* pubsub */
    /**********/
    Publish(pubsub),
    Subscribe(pubsub),
    Unsubscribe(pubsub),
    /*************/
    /* scripting */
    /*************/
    Eval<A>(scripting),
    EvalName<A>(scripting),
    ScriptExists<A>(scripting),
    ScriptFlush(scripting),
    ScriptRegister<A>(scripting)
);

/// 调用前应将name转为大写
pub fn cmd_name_to_flag<U>(name: U) -> RutinResult<CmdFlag>
where
    U: IntoUppercase,
{
    let name: Uppercase<32, U::Mut> = name.into_uppercase();
    _cmd_name_to_flag(name.as_ref()).ok_or(RutinError::UnknownCmd)
}

pub fn cmd_flag_to_name(flag: CmdFlag) -> Option<&'static str> {
    _cmd_flag_to_name(flag)
}

pub fn cat_names() -> [&'static str; 11] {
    [
        "admin",
        "read",
        "write",
        "connection",
        "keyspace",
        "string",
        "list",
        "hash",
        "pubsub",
        "scripting",
        "dangerous",
    ]
}

/// 调用前应将cat_name转为大写
pub fn cat_name_to_cmds_flag<U>(cat_name: U) -> RutinResult<CmdFlag>
where
    U: IntoUppercase,
{
    let cat_name = cat_name.into_uppercase::<16>();
    let cat_name = cat_name.as_ref();
    Ok(match cat_name {
        b"ADMIN" => ADMIN_CMDS_FLAG,
        b"READ" => READ_CMDS_FLAG,
        b"WRITE" => WRITE_CMDS_FLAG,
        b"CONNECTION" => CONNECTION_CMDS_FLAG,
        b"KEYSPACE" => KEYSPACE_CMDS_FLAG,
        b"STRING" => STRING_CMDS_FLAG,
        b"LIST" => LIST_CMDS_FLAG,
        b"HASH" => HASH_CMDS_FLAG,
        b"PUBSUB" => PUBSUB_CMDS_FLAG,
        b"SCRIPTING" => SCRIPTING_CMDS_FLAG,
        b"DANGEROUS" => DANGEROUS_CMDS_FLAG,
        _ => {
            return Err(RutinError::UnknownCmdCategory {
                category: Bytes::copy_from_slice(cat_name),
            })
        }
    })
}

// #[allow(unreachable_patterns)]
// pub fn cmds_flag_to_cat_name(cat_flag: CmdFlag) -> &'static str {
//     match cat_flag {
//         ADMIN_CMDS_FLAG => "admin",
//         READ_CMDS_FLAG => "read",
//         WRITE_CMDS_FLAG => "write",
//         CONNECTION_CMDS_FLAG => "connection",
//         KEYSPACE_CMDS_FLAG => "keyspace",
//         STRING_CMDS_FLAG => "string",
//         LIST_CMDS_FLAG => "list",
//         HASH_CMDS_FLAG => "hash",
//         A => "pubsub",
//         SCRIPTING_CMDS_FLAG => "scripting",
//         DANGEROUS_CMDS_FLAG => "dangerous",
//         _ => panic!("Unknown ACL category flag: {}", cat_flag),
//     }
// }

pub fn cmds_flag_to_names(cmds_flag: CmdFlag) -> Vec<&'static str> {
    let mut names = Vec::new();
    let mut cursor = 1;

    while cursor <= cmds_flag {
        if cmds_flag & cursor != 0 {
            if let Some(name) = cmd_flag_to_name(cursor) {
                names.push(name);
            }
        }
        cursor <<= 1;
    }

    names
}

pub fn cat_flag_to_cmds_flag(cat_flag: CatFlag) -> CmdFlag {
    match cat_flag {
        ADMIN_CAT_FLAG => ADMIN_CMDS_FLAG,
        READ_CAT_FLAG => READ_CMDS_FLAG,
        WRITE_CAT_FLAG => WRITE_CMDS_FLAG,
        CONNECTION_CAT_FLAG => CONNECTION_CMDS_FLAG,
        KEYSPACE_CAT_FLAG => KEYSPACE_CMDS_FLAG,
        STRING_CAT_FLAG => STRING_CMDS_FLAG,
        LIST_CAT_FLAG => LIST_CMDS_FLAG,
        HASH_CAT_FLAG => HASH_CMDS_FLAG,
        PUBSUB_CAT_FLAG => PUBSUB_CMDS_FLAG,
        SCRIPTING_CAT_FLAG => SCRIPTING_CMDS_FLAG,
        DANGEROUS_CAT_FLAG => DANGEROUS_CMDS_FLAG,
        _ => panic!("Unknown ACL category flag: {}", cat_flag),
    }
}

#[inline]
pub const fn cmds_contains_cmd(cmds_flag: CmdFlag, cmd_flag: CmdFlag) -> bool {
    cmds_flag & cmd_flag != 0
}

#[inline]
pub const fn cats_contains_cat(cats_flag: CatFlag, cat_flag: CatFlag) -> bool {
    cats_flag & cat_flag != 0
}
