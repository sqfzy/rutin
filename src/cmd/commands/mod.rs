mod admin;
mod connection;
mod hash;
mod key;
mod list;
mod pub_sub;
mod script;
mod set;
mod str;
mod zset;

pub use admin::*;
pub use connection::*;
pub use hash::*;
pub use key::*;
pub use list::*;
pub use pub_sub::*;
pub use script::*;
pub use str::*;

use super::*;
use crate::{
    error::{RutinError, RutinResult},
    util::get_uppercase,
};
use bytes::Bytes;
use paste::paste;

pub type Flag = u128;

pub const ADMIN_CAT_FLAG: Flag = ACLCAT_CMD_FLAG
    | ACLDELUSER_CMD_FLAG
    | ACLSETUSER_CMD_FLAG
    | ACLWHOAMI_CMD_FLAG
    | ACLUSERS_CMD_FLAG
    | BGSAVE_CMD_FLAG
    | PSYNC_CMD_FLAG
    | REPLCONF_CMD_FLAG
    | REPLICAOF_CMD_FLAG;

pub const CONNECTION_CAT_FLAG: Flag =
    AUTH_CMD_FLAG | CLIENTTRACKING_CMD_FLAG | ECHO_CMD_FLAG | PING_CMD_FLAG;

pub const READ_CAT_FLAG: Flag = DUMP_CMD_FLAG
    | EXISTS_CMD_FLAG
    | EXPIRETIME_CMD_FLAG
    | KEYS_CMD_FLAG
    | NBKEYS_CMD_FLAG
    | PTTL_CMD_FLAG
    | TTL_CMD_FLAG
    | TYPE_CMD_FLAG
    | GET_CMD_FLAG
    | GETRANGE_CMD_FLAG
    | MGET_CMD_FLAG
    | STRLEN_CMD_FLAG
    | LLEN_CMD_FLAG
    | LPOS_CMD_FLAG
    | HEXISTS_CMD_FLAG
    | HGET_CMD_FLAG;

pub const WRITE_CAT_FLAG: Flag = DEL_CMD_FLAG
    | DUMP_CMD_FLAG
    | EXPIRE_CMD_FLAG
    | EXPIREAT_CMD_FLAG
    | PERSIST_CMD_FLAG
    | APPEND_CMD_FLAG
    | DECR_CMD_FLAG
    | DECRBY_CMD_FLAG
    | GETSET_CMD_FLAG
    | INCR_CMD_FLAG
    | INCRBY_CMD_FLAG
    | MSET_CMD_FLAG
    | MSETNX_CMD_FLAG
    | SET_CMD_FLAG
    | SETEX_CMD_FLAG
    | SETNX_CMD_FLAG
    | BLMOVE_CMD_FLAG
    | BLPOP_CMD_FLAG
    | LPOP_CMD_FLAG
    | LPUSH_CMD_FLAG
    | NBLPOP_CMD_FLAG
    | HDEL_CMD_FLAG
    | HSET_CMD_FLAG;

pub const KEYSPACE_CAT_FLAG: Flag = DEL_CMD_FLAG
    | DUMP_CMD_FLAG
    | EXISTS_CMD_FLAG
    | EXPIRE_CMD_FLAG
    | EXPIREAT_CMD_FLAG
    | EXPIRETIME_CMD_FLAG
    | KEYS_CMD_FLAG
    | NBKEYS_CMD_FLAG
    | PERSIST_CMD_FLAG
    | PTTL_CMD_FLAG
    | TTL_CMD_FLAG
    | TYPE_CMD_FLAG;

pub const STRING_CAT_FLAG: Flag = APPEND_CMD_FLAG
    | DECR_CMD_FLAG
    | DECRBY_CMD_FLAG
    | GET_CMD_FLAG
    | GETRANGE_CMD_FLAG
    | GETSET_CMD_FLAG
    | INCR_CMD_FLAG
    | INCRBY_CMD_FLAG
    | MGET_CMD_FLAG
    | MSET_CMD_FLAG
    | MSETNX_CMD_FLAG
    | SET_CMD_FLAG
    | SETEX_CMD_FLAG
    | SETNX_CMD_FLAG
    | STRLEN_CMD_FLAG;

pub const LIST_CAT_FLAG: Flag = BLMOVE_CMD_FLAG
    | BLPOP_CMD_FLAG
    | LLEN_CMD_FLAG
    | LPOP_CMD_FLAG
    | LPOS_CMD_FLAG
    | LPUSH_CMD_FLAG
    | NBLPOP_CMD_FLAG;

pub const HASH_CAT_FLAG: Flag = HDEL_CMD_FLAG | HEXISTS_CMD_FLAG | HGET_CMD_FLAG | HSET_CMD_FLAG;

pub const PUBSUB_CAT_FLAG: Flag = PUBLISH_CMD_FLAG | SUBSCRIBE_CMD_FLAG | UNSUBSCRIBE_CMD_FLAG;

pub const SCRIPTING_CAT_FLAG: Flag = EVAL_CMD_FLAG
    | EVALNAME_CMD_FLAG
    | SCRIPTEXISTS_CMD_FLAG
    | SCRIPTFLUSH_CMD_FLAG
    | SCRIPTREGISTER_CMD_FLAG;

pub const DANGEROUS_CAT_FLAG: Flag = ACLCAT_CMD_FLAG
    | ACLDELUSER_CMD_FLAG
    | ACLSETUSER_CMD_FLAG
    | ACLWHOAMI_CMD_FLAG
    | ACLUSERS_CMD_FLAG
    | BGSAVE_CMD_FLAG
    | PSYNC_CMD_FLAG
    | REPLICAOF_CMD_FLAG;

macro_rules! gen_cmd_and_cats_flag {
    // Recursion end condition
    (@internal $shift:expr,) => {
        pub const ALL_CMD_FLAG: Flag = (1 << $shift) - 1;
    };

    // Recursive expansion of the macro, defining each command flag and its shift amount
    (@internal $shift:expr, $cmd:ident ($($cat:ident),*) $(, $rest:ident ($($rest_cat:ident),*))* ) => {
        paste! {
            pub const [<$cmd:upper _CMD_FLAG>]: Flag = 1 << $shift;
            pub const [<$cmd:upper _CATS_FLAG>]: Flag = $([<$cat:upper _CAT_FLAG>])|*;
        }
        gen_cmd_and_cats_flag !(@internal $shift + 1, $($rest ($($rest_cat),*)),*);
    };

    ($($name:ident ($($cat:ident),*)),+) => {
        // Start the internal recursion
        gen_cmd_and_cats_flag !(@internal 0, $($name ($($cat),*)),+);

        pub fn cmd_name_to_flag(name: &[u8]) -> RutinResult<Flag> {
            let mut buf = [0; 32];
            let name = get_uppercase(name, &mut buf)?;
            let name = std::str::from_utf8(&name).map_err(|_| RutinError::UnknownCmd)?;

            Ok(match name {
                $(
                    paste! {stringify!([<$name:upper>])} => paste! {[<$name:upper _CMD_FLAG>]},
                )*
                _ => return Err(RutinError::UnknownCmd)
            })
        }

        pub fn cmd_flag_to_name(flag: Flag) -> Option<&'static str> {
            Some(match flag {
                $(
                    paste! {[<$name:upper _CMD_FLAG>]} => $name::NAME,

                )*
                _ => return None,
            })
        }

        pub fn cmds_flag_to_names(cmds_flag: Flag) -> Vec<&'static str> {
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
    };
}

pub const NO_CMD_FLAG: Flag = Flag::MIN | AUTH_CMD_FLAG; // 允许AUTH命令

gen_cmd_and_cats_flag!(
    /*********/
    /* admin */
    /*********/
    AclCat(admin, dangerous),
    AclDelUser(admin, dangerous),
    AclSetUser(admin, dangerous),
    AclUsers(admin, dangerous),
    AclWhoAmI(admin, dangerous),
    BgSave(admin, dangerous),
    PSync(admin, dangerous),
    ReplConf(admin, dangerous),
    ReplicaOf(admin, dangerous),
    /**************/
    /* connection */
    /**************/
    Auth(connection),
    ClientTracking(connection),
    Echo(connection),
    Ping(connection),
    /************/
    /* keyspace */
    /************/
    Del(keyspace, write),
    Dump(keyspace, read),
    Exists(keyspace, read),
    Expire(keyspace, write),
    ExpireAt(keyspace, write),
    ExpireTime(keyspace, read),
    Keys(keyspace, read),
    NBKeys(keyspace, read),
    Persist(keyspace, write),
    Pttl(keyspace, read),
    Ttl(keyspace, read),
    Type(keyspace, read),
    /**********/
    /* string */
    /**********/
    Append(string, write),
    Decr(string, write),
    DecrBy(string, write),
    Get(string, read),
    GetRange(string, read),
    GetSet(string, write),
    Incr(string, write),
    IncrBy(string, write),
    MGet(string, read),
    MSet(string, write),
    MSetNx(string, write),
    Set(string, write),
    SetEx(string, write),
    SetNx(string, write),
    StrLen(string, read),
    /********/
    /* list */
    /********/
    BLMove(list, write),
    BLPop(list, write),
    LLen(list, read),
    LPop(list, write),
    LPos(list, read),
    LPush(list, write),
    NBLPop(list, write),
    /********/
    /* hash */
    /********/
    HDel(hash, write),
    HExists(hash, read),
    HGet(hash, read),
    HSet(hash, write),
    /**********/
    /* pubsub */
    /**********/
    Publish(pubsub),
    Subscribe(pubsub),
    Unsubscribe(pubsub),
    /*************/
    /* scripting */
    /*************/
    Eval(scripting),
    EvalName(scripting),
    ScriptExists(scripting),
    ScriptFlush(scripting),
    ScriptRegister(scripting)
);

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

pub fn cat_name_to_flag(cat_name: &[u8]) -> RutinResult<Flag> {
    let mut buf = [0; 32];
    let cat_name = get_uppercase(cat_name, &mut buf)?;

    Ok(match cat_name {
        b"ADMIN" => ADMIN_CAT_FLAG,
        b"READ" => READ_CAT_FLAG,
        b"WRITE" => WRITE_CAT_FLAG,
        b"CONNECTION" => CONNECTION_CAT_FLAG,
        b"KEYSPACE" => KEYSPACE_CAT_FLAG,
        b"STRING" => STRING_CAT_FLAG,
        b"LIST" => LIST_CAT_FLAG,
        b"HASH" => HASH_CAT_FLAG,
        b"PUBSUB" => PUBSUB_CAT_FLAG,
        b"SCRIPTING" => SCRIPTING_CAT_FLAG,
        b"DANGEROUS" => DANGEROUS_CAT_FLAG,
        _ => {
            return Err(RutinError::UnknownCmdCategory {
                category: Bytes::copy_from_slice(cat_name),
            })
        }
    })
}

#[allow(unreachable_patterns)]
pub fn cat_flag_to_name(cat_flag: Flag) -> &'static str {
    match cat_flag {
        ADMIN_CAT_FLAG => "admin",
        READ_CAT_FLAG => "read",
        WRITE_CAT_FLAG => "write",
        CONNECTION_CAT_FLAG => "connection",
        KEYSPACE_CAT_FLAG => "keyspace",
        STRING_CAT_FLAG => "string",
        LIST_CAT_FLAG => "list",
        HASH_CAT_FLAG => "hash",
        PUBSUB_CAT_FLAG => "pubsub",
        SCRIPTING_CAT_FLAG => "scripting",
        DANGEROUS_CAT_FLAG => "dangerous",
        _ => panic!("Unknown ACL category flag: {}", cat_flag),
    }
}

#[inline]
pub fn cmds_contains_cmd(cmds_flag: Flag, cmd_flag: Flag) -> bool {
    cmds_flag & cmd_flag != 0
}

pub fn cmds_contains_cmds(cmds_flag: Flag, check: Flag) -> bool {
    cmds_flag & check == check
}
