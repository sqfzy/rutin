use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::Resp3,
    persist::rdb::{
        encode_hash_value, encode_list_value, encode_set_value, encode_str_value, encode_zset_value,
    },
    server::{AsyncStream, Handler, NEVER_EXPIRE, UNIX_EPOCH},
    shared::{db::ObjectValueType, Letter},
    util::atoi,
    Int,
};
use bytes::BytesMut;
use itertools::Itertools;
use std::time::Duration;
use tokio::time::Instant;
use tracing::instrument;

#[derive(Debug)]
enum Opt {
    NX, // 要求键无过期时间
    XX, // 要求键有过期时间
    GT, // 要求new_expire > 键的过期时间
    LT, // 要求new_expire < 键的过期时间
}

impl TryFrom<&[u8]> for Opt {
    type Error = RutinError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match value {
            b"NX" => Ok(Opt::NX),
            b"XX" => Ok(Opt::XX),
            b"GT" => Ok(Opt::GT),
            b"LT" => Ok(Opt::LT),
            _ => Err("ERR invalid option is given".into()),
        }
    }
}

/// 该命令用于在 key 存在时删除 key。
/// # Reply:
///
/// **Integer reply:** the number of keys that were removed.
#[derive(Debug)]
pub struct Del {
    pub keys: Vec<StaticBytes>,
}

impl CmdExecutor for Del {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let db = handler.shared.db();
        let mut count = 0;

        for key in self.keys {
            if db.remove_object(&key).await.is_some() {
                count += 1;
            }
        }

        Ok(Some(Resp3::new_integer(count)))
    }

    fn parse(args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        let keys = args
            .map(|k| {
                if ac.deny_reading_or_writing_key(&k, Self::CATS_FLAG) {
                    return Err(RutinError::NoPermission);
                }
                Ok(k)
            })
            .try_collect()?;

        Ok(Del { keys })
    }
}

/// 序列化给定 key ，并返回被序列化的值。
/// # Reply:
///
/// **Bulk string reply:** the serialized value of the key.
/// **Null reply:** the key does not exist.
#[derive(Debug)]
pub struct Dump {
    pub key: StaticBytes,
}

impl CmdExecutor for Dump {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut buf = BytesMut::with_capacity(1024);
        handler
            .shared
            .db()
            .visit_object(self.key.as_ref(), |obj| {
                match obj.typ() {
                    ObjectValueType::Str => encode_str_value(&mut buf, obj.on_str()?),
                    ObjectValueType::List => encode_list_value(&mut buf, obj.on_list()?),
                    ObjectValueType::Set => encode_set_value(&mut buf, obj.on_set()?),
                    ObjectValueType::Hash => encode_hash_value(&mut buf, obj.on_hash()?),
                    ObjectValueType::ZSet => encode_zset_value(&mut buf, obj.on_zset()?),
                }

                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_blob_string(buf.freeze())))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Dump { key })
    }
}

/// 检查给定 key 是否存在。
/// # Reply:
///
/// **Integer reply:** the number of keys that exist from those specified as arguments.
#[derive(Debug)]
pub struct Exists {
    pub keys: Vec<StaticBytes>,
}

impl CmdExecutor for Exists {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        for key in self.keys {
            if !handler.shared.db().contains_object(key.as_ref()).await {
                return Err(RutinError::from(0));
            }
        }

        Ok(Some(Resp3::new_integer(1)))
    }

    fn parse(args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        let keys = args
            .map(|k| {
                if ac.deny_reading_or_writing_key(&k, Self::CATS_FLAG) {
                    return Err(RutinError::NoPermission);
                }
                Ok(k)
            })
            .try_collect()?;

        Ok(Exists { keys })
    }
}

/// 为给定 key 设置过期时间，以秒计。
/// # Reply:
///
/// **Integer reply:** 0 if the timeout was not set; for example, the key doesn't exist, or the operation was skipped because of the provided arguments.
/// **Integer reply:** 1 if the timeout was set.
#[derive(Debug)]
pub struct Expire {
    key: StaticBytes,
    seconds: Duration,
    opt: Option<Opt>,
}

impl CmdExecutor for Expire {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let new_ex = Instant::now() + self.seconds;

        let mut obj = handler.shared.db().object_entry(&self.key).await?;

        let ex = obj.expire_mut().ok_or_else(|| RutinError::from(0))?;

        match self.opt {
            // 无过期时间，则设置
            Some(Opt::NX) => {
                if *ex == *NEVER_EXPIRE {
                    *ex = new_ex;
                    return Ok(Some(Resp3::new_integer(1)));
                }
            }
            // 有过期时间，则设置
            Some(Opt::XX) => {
                if *ex != *NEVER_EXPIRE {
                    *ex = new_ex;
                    return Ok(Some(Resp3::new_integer(1)));
                }
            }
            // 过期时间大于给定时间，则设置
            Some(Opt::GT) => {
                if new_ex > *ex {
                    *ex = new_ex;
                    return Ok(Some(Resp3::new_integer(1)));
                }
            }
            // 过期时间小于给定时间，则设置
            Some(Opt::LT) => {
                if new_ex < *ex {
                    *ex = new_ex;
                    return Ok(Some(Resp3::new_integer(1)));
                }
            }
            None => {
                *ex = new_ex;
                return Ok(Some(Resp3::new_integer(1)));
            }
        }

        Err(RutinError::from(0))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 && args.len() != 3 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let seconds = Duration::from_secs(atoi(&args.next().unwrap())?);
        let opt = match args.next() {
            Some(b) => Some(Opt::try_from(b.as_ref())?),
            None => None,
        };

        Ok(Expire { key, seconds, opt })
    }
}

/// # Reply:
///
/// **Integer reply:** 0 if the timeout was not set; for example, the key doesn't exist, or the operation was skipped because of the provided arguments.
/// **Integer reply:** 1 if the timeout was set.
#[derive(Debug)]
pub struct ExpireAt {
    key: StaticBytes,
    timestamp: Instant,
    opt: Option<Opt>,
}

impl CmdExecutor for ExpireAt {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let new_ex = self.timestamp;

        let mut obj = handler.shared.db().object_entry(&self.key).await?;

        let ex = obj.expire_mut().ok_or_else(|| RutinError::from(0))?;

        match self.opt {
            // 无过期时间，则设置
            Some(Opt::NX) => {
                if *ex == *NEVER_EXPIRE {
                    *ex = new_ex;
                    return Ok(Some(Resp3::new_integer(1)));
                }
            }
            // 有过期时间，则设置
            Some(Opt::XX) => {
                if *ex != *NEVER_EXPIRE {
                    *ex = new_ex;
                    return Ok(Some(Resp3::new_integer(1)));
                }
            }
            // 过期时间大于给定时间，则设置
            Some(Opt::GT) => {
                if new_ex > *ex {
                    *ex = new_ex;
                    return Ok(Some(Resp3::new_integer(1)));
                }
            }
            // 过期时间小于给定时间，则设置
            Some(Opt::LT) => {
                if new_ex < *ex {
                    *ex = new_ex;
                    return Ok(Some(Resp3::new_integer(1)));
                }
            }
            None => {
                *ex = new_ex;
                return Ok(Some(Resp3::new_integer(1)));
            }
        }

        Err(RutinError::from(0))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 && args.len() != 3 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let timestamp = atoi::<u64>(&args.next().unwrap())?;
        let timestamp = *UNIX_EPOCH + Duration::from_secs(timestamp);
        if timestamp <= Instant::now() {
            return Err("ERR invalid timestamp".into());
        }

        let opt = match args.next() {
            Some(b) => Some(Opt::try_from(b.as_ref())?),
            None => None,
        };

        Ok(ExpireAt {
            key,
            timestamp,
            opt,
        })
    }
}

/// # Reply:
///
/// **Integer reply:** the expiration Unix timestamp in seconds.
/// **Integer reply:** -1 if the key exists but has no associated expiration time.
/// **Integer reply:** -2 if the key does not exist.
#[derive(Debug)]
pub struct ExpireTime {
    pub key: StaticBytes,
}

impl CmdExecutor for ExpireTime {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let ex = handler
            .shared
            .db()
            .get_object_expired(self.key.as_ref())
            .await
            .map_err(|_| RutinError::from(-1))?;

        if ex != *NEVER_EXPIRE {
            Ok(Some(Resp3::new_integer(
                ex.duration_since(*UNIX_EPOCH).as_secs() as Int,
            )))
        } else {
            // 无过期时间
            Err((-2).into())
        }
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(ExpireTime { key })
    }
}

/// # Reply:
///
/// **Array reply:** a list of keys matching pattern.
#[derive(Debug)]
pub struct Keys {
    pub pattern: StaticBytes,
}

impl CmdExecutor for Keys {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let shared = handler.shared;
        let outbox = handler.context.outbox.clone();
        let re = regex::bytes::Regex::new(std::str::from_utf8(&self.pattern)?)?;

        // 避免阻塞woker thread
        tokio::task::spawn_blocking(move || {
            let matched_keys = shared
                .db()
                .entries
                .iter()
                .filter_map(|entry| {
                    re.is_match(entry.key())
                        .then(|| CheapResp3::new_blob_string(entry.key().clone().into()))
                })
                .collect::<Vec<CheapResp3>>();

            outbox
                .send(Letter::Resp3(Resp3::new_array(matched_keys)))
                .ok();
        });

        Ok(None)
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let pattern = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&pattern, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Keys { pattern })
    }
}

/// # Reply:
///
/// **Array reply:** a list of keys matching pattern.
// // TODO: 也许应该返回Resp3::Push?
// #[derive(Debug)]
// pub struct NBKeys {
//     pattern: StaticBytesMut,
//     redirect: Id, // 0表示不重定向
// }
//
// impl CmdExecutor for NBKeys {
//     #[instrument(level = "debug", skip(handler), ret, err)]
//     async fn execute(
//         self,
//         handler: &mut Handler<impl AsyncStream>,
//     ) -> RutinResult<Option<CheapResp3>> {
//         let re = regex::bytes::Regex::new(std::str::from_utf8(&self.pattern)?)?;
//
//         let outbox = if self.redirect != 0 {
//             &handler
//                 .shared
//                 .post_office()
//                 .get_outbox(self.redirect)
//                 .ok_or(RutinError::from("ERR The client ID does not exist"))?
//         } else {
//             &handler.context.outbox
//         };
//
//         tokio::task::block_in_place(|| {
//             let db = handler.shared.db();
//
//             let matched_keys = db
//                 .entries
//                 .iter()
//                 .filter_map(|entry| {
//                     std::str::from_utf8(&entry.key()).ok().and_then(|key| {
//                         re.is_match(key)
//                             .then(|| CheapResp3::new_blob_string(entry.key().clone()))
//                     })
//                 })
//                 .collect::<Vec<CheapResp3>>();
//
//             let _ = outbox.send(Letter::Resp3(CheapResp3::new_array(matched_keys)));
//         });
//
//         Ok(None)
//     }
//
//     fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
//         if args.len() != 2 {
//             return Err(RutinError::WrongArgNum);
//         }
//
//         let pattern = args.next().unwrap();
//         if ac.deny_reading_or_writing_key(&pattern, Self::CATS_FLAG) {
//             return Err(RutinError::NoPermission);
//         }
//
//         Ok(NBKeys {
//             pattern,
//             redirect: atoi::<Id>(args.next().unwrap())?,
//         })
//     }
// }

/// 移除 key 的过期时间，key 将持久保持。
/// # Reply:
///
/// **Integer reply:** 0 if key does not exist or does not have an associated timeout.
/// **Integer reply:** 1 if the timeout has been removed.
#[derive(Debug)]
pub struct Persist {
    pub key: StaticBytes,
}

impl CmdExecutor for Persist {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut obj = handler.shared.db().object_entry(self.key.as_ref()).await?;
        let ex = obj.expire_mut().ok_or_else(|| RutinError::from(0))?;

        if *ex == *NEVER_EXPIRE {
            return Err(0.into());
        }

        *ex = *NEVER_EXPIRE;

        Ok(Some(Resp3::new_integer(1)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Persist { key })
    }
}

/// 以毫秒为单位返回 key 的剩余的过期时间。
/// # Reply:
///
/// **Integer reply:** TTL in milliseconds.
/// **Integer reply:** -1 if the key exists but has no associated expiration.
/// **Integer reply:** -2 if the key does not exist.
#[derive(Debug)]
pub struct Pttl {
    pub key: StaticBytes,
}

impl CmdExecutor for Pttl {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let ex = handler
            .shared
            .db()
            .get_object_expired(self.key.as_ref())
            .await
            .map_err(|_| RutinError::from(-2))?;

        if ex != *NEVER_EXPIRE {
            let pttl = (ex - Instant::now()).as_millis();
            Ok(Some(Resp3::new_integer(pttl as Int)))
        } else {
            Err((-1).into())
        }
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Pttl { key })
    }
}

/// 以秒为单位，返回给定 key 的剩余生存时间(TTL, time to live)。
/// # Reply:
///
/// **Integer reply:** TTL in seconds.
/// **Integer reply:** -1 if the key exists but has no associated expiration.
/// **Integer reply:** -2 if the key does not exist.
#[derive(Debug)]
pub struct Ttl {
    pub key: StaticBytes,
}

impl CmdExecutor for Ttl {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let ex = handler
            .shared
            .db()
            .get_object_expired(self.key.as_ref())
            .await
            .map_err(|_| RutinError::from(-2))?;

        if ex != *NEVER_EXPIRE {
            let ttl = (ex - Instant::now()).as_secs();
            Ok(Some(Resp3::new_integer(ttl as Int)))
        } else {
            Err((-1).into())
        }
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Ttl { key })
    }
}

/// 返回 key 所储存的值的类型。
/// # Reply:
///
/// **Simple string reply:** the type of key, or none when key doesn't exist.
#[derive(Debug)]
pub struct Type {
    pub key: StaticBytes,
}

impl CmdExecutor for Type {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let typ = handler
            .shared
            .db()
            .get_object(self.key.as_ref())
            .await?
            .type_str();

        Ok(Some(Resp3::new_simple_string(typ.into())))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Type { key })
    }
}

#[cfg(test)]
mod cmd_key_tests {
    use super::*;
    use crate::{
        server::{NEVER_EXPIRE, UNIX_EPOCH},
        shared::db::{Hash, List, Object, Set, Str, ZSet},
        util::gen_test_handler,
    };

    // 允许的时间误差
    const ALLOWED_DELTA: u64 = 3;

    #[tokio::test]
    async fn del_test() {
        let mut handler = gen_test_handler();
        let db = handler.shared.db();

        db.insert_object(
            "key1".as_bytes(),
            Object::with_expire(Str::from("value1"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db.contains_object("key1".as_bytes()).await);

        // case: 键存在
        let del = Del::parse(
            gen_cmdunparsed_test(["DEL", "key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = del.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
        assert!(!handler.shared.db().contains_object("key1".as_bytes()).await);

        // case: 键不存在
        let del = Del::parse(
            gen_cmdunparsed_test(["DEL", "key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = del.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(0));
    }

    #[tokio::test]
    async fn exists_test() {
        let mut handler = gen_test_handler();
        let db = handler.shared.db();

        db.insert_object(
            "key1".as_bytes(),
            Object::with_expire(Str::from("value1"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db.contains_object("key1".as_bytes()).await);

        // case: 键存在
        let exists = Exists::parse(
            gen_cmdunparsed_test(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = exists.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        // case: 键不存在
        let exists = Exists::parse(
            gen_cmdunparsed_test(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = exists.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);
    }

    #[tokio::test]
    async fn expire_test() {
        let mut handler = gen_test_handler();
        let db = handler.shared.db();

        db.insert_object(
            "key1".as_bytes(),
            Object::with_expire(Str::from("value1"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert_eq!(
            db.get_object("key1".as_bytes()).await.unwrap().expire,
            *NEVER_EXPIRE
        );

        // case: 键存在，设置过期时间
        let expire = Expire::parse(
            gen_cmdunparsed_test(["key1", "10"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
        assert!(!handler
            .shared
            .db()
            .get_object("key1".as_bytes())
            .await
            .unwrap()
            .is_never_expired(),);

        // case: 键不存在
        let expire = Expire::parse(
            gen_cmdunparsed_test(["key_nil", "10"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let db = handler.shared.db();

        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(
                Str::from("value_with_ex"),
                Instant::now() + Duration::from_secs(10),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            "key_without_ex".as_bytes(),
            Object::with_expire(Str::from("value_without_ex"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        // case: with EX option
        let expire = Expire::parse(
            gen_cmdunparsed_test(["key_with_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire = Expire::parse(
            gen_cmdunparsed_test(["key_without_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(
                Str::from("value_with_ex"),
                Instant::now() + Duration::from_secs(10),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            "key_without_ex".as_bytes(),
            Object::with_expire(Str::from("value_without_ex"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with NX option
        let expire = Expire::parse(
            gen_cmdunparsed_test(["key_with_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire = Expire::parse(
            gen_cmdunparsed_test(["key_without_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(
                Str::from("value_with_ex"),
                Instant::now() + Duration::from_secs(10),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            "key_without_ex".as_bytes(),
            Object::with_expire(Str::from("value_without_ex"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with GT option
        let expire = Expire::parse(
            gen_cmdunparsed_test(["key_with_ex", "5", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire = Expire::parse(
            gen_cmdunparsed_test(["key_with_ex", "20", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(
                Str::from("value_with_ex"),
                Instant::now() + Duration::from_secs(10),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            "key_without_ex".as_bytes(),
            Object::with_expire(Str::from("value_without_ex"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with LT option
        let expire = Expire::parse(
            gen_cmdunparsed_test(["key_with_ex", "20", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire = Expire::parse(
            gen_cmdunparsed_test(["key_with_ex", "5", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
    }

    #[tokio::test]
    async fn expire_at_test() {
        let mut handler = gen_test_handler();
        let db = handler.shared.db();

        db.insert_object(
            "key1".as_bytes(),
            Object::with_expire(Str::from("value1"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db
            .get_object("key1".as_bytes())
            .await
            .unwrap()
            .is_never_expired());

        // case: 键存在，设置过期时间
        let expire_at = ExpireAt::parse(
            gen_cmdunparsed_test(
                [
                    "key1",
                    "1893427200", // 2030-01-01 00:00:00
                ]
                .as_ref(),
            ),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
        assert!(!handler
            .shared
            .db()
            .get_object("key1".as_bytes())
            .await
            .unwrap()
            .is_never_expired());

        // case: 键不存在
        let expire_at = ExpireAt::parse(
            gen_cmdunparsed_test(["key_nil", "1893427200"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let db = handler.shared.db();

        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(
                Str::from("value_with_ex"),
                *UNIX_EPOCH + Duration::from_secs(1893427200),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            "key_without_ex".as_bytes(),
            Object::with_expire(Str::from("value_without_ex"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with EX option
        let expire_at = ExpireAt::parse(
            gen_cmdunparsed_test(["key_with_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            gen_cmdunparsed_test(["key_without_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(
                Str::from("value_with_ex"),
                *UNIX_EPOCH + Duration::from_secs(1893427200),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            "key_without_ex".as_bytes(),
            Object::with_expire(Str::from("value_without_ex"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with NX option
        let expire_at = ExpireAt::parse(
            gen_cmdunparsed_test(["key_with_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            gen_cmdunparsed_test(["key_without_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(
                Str::from("value_with_ex"),
                *UNIX_EPOCH + Duration::from_secs(1893427200),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            "key_without_ex".as_bytes(),
            Object::with_expire(Str::from("value_without_ex"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with GT option
        let expire_at = ExpireAt::parse(
            gen_cmdunparsed_test(["key_with_ex", "1893427000", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            gen_cmdunparsed_test(["key_with_ex", "1893427201", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(
                Str::from("value_with_ex"),
                *UNIX_EPOCH + Duration::from_secs(1893427200),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            "key_without_ex".as_bytes(),
            Object::with_expire(Str::from("value_without_ex"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with LT option
        let expire_at = ExpireAt::parse(
            gen_cmdunparsed_test(["key_with_ex", "1893427201", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            gen_cmdunparsed_test(["key_with_ex", "1893427000", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
    }

    #[tokio::test]
    async fn expire_time_test() {
        let mut handler = gen_test_handler();
        let db = handler.shared.db();

        db.insert_object(
            "key1".as_bytes(),
            Object::with_expire(Str::from("value1"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db
            .get_object("key1".as_bytes())
            .await
            .unwrap()
            .is_never_expired());
        let expire = Instant::now() + Duration::from_secs(10);
        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(Str::from("value_with_ex"), expire),
        )
        .await
        .unwrap();

        // case: 键存在，但没有过期时间
        let expire_time = ExpireTime::parse(
            gen_cmdunparsed_test(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_time.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -1);

        // case: 键不存在
        let expire_time = ExpireTime::parse(
            gen_cmdunparsed_test(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_time.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -2);

        // case: 键存在且有过期时间
        let expire_time = ExpireTime::parse(
            gen_cmdunparsed_test(["key_with_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_time.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(
            result,
            Resp3::new_integer(expire.duration_since(*UNIX_EPOCH).as_secs() as Int)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn keys_test() {
        let mut handler = gen_test_handler();
        let db = handler.shared.db();

        db.insert_object(
            "key1".as_bytes(),
            Object::with_expire(Str::from("value1"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            "key2".as_bytes(),
            Object::with_expire(Str::from("value2"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            "key3".as_bytes(),
            Object::with_expire(Str::from("value3"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            "key4".as_bytes(),
            Object::with_expire(Str::from("value4"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        let keys = Keys::parse(
            gen_cmdunparsed_test([".*"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        keys.execute(&mut handler).await.unwrap();
        let result = handler
            .context
            .inbox
            .recv()
            .into_resp3_unchecked()
            .into_array_unchecked();
        assert!(
            result.contains(&Resp3::new_blob_string("key1".into()))
                && result.contains(&Resp3::new_blob_string("key2".into()))
                && result.contains(&Resp3::new_blob_string("key3".into()))
                && result.contains(&Resp3::new_blob_string("key4".into()))
        );

        let keys = Keys::parse(
            gen_cmdunparsed_test(["key*"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        keys.execute(&mut handler).await.unwrap();
        let result = handler
            .context
            .inbox
            .recv()
            .into_resp3_unchecked()
            .into_array_unchecked();
        assert!(
            result.contains(&Resp3::new_blob_string("key1".into()))
                && result.contains(&Resp3::new_blob_string("key2".into()))
                && result.contains(&Resp3::new_blob_string("key3".into()))
                && result.contains(&Resp3::new_blob_string("key4".into()))
        );

        let keys = Keys::parse(
            gen_cmdunparsed_test(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        keys.execute(&mut handler).await.unwrap();
        let result = handler
            .context
            .inbox
            .recv()
            .into_resp3_unchecked()
            .into_array_unchecked();
        assert!(result.contains(&Resp3::new_blob_string("key1".into())));
    }

    #[tokio::test]
    async fn persist_test() {
        let mut handler = gen_test_handler();
        let db = handler.shared.db();

        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(
                Str::from("value_with_ex"),
                Instant::now() + Duration::from_secs(10),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            "key_without_ex".as_bytes(),
            Object::with_expire(Str::from("value_without_ex"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: 键存在，有过期时间
        let persist = Persist::parse(
            gen_cmdunparsed_test(["key_with_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = persist.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
        assert!(handler
            .shared
            .db()
            .get_object("key_with_ex".as_bytes())
            .await
            .unwrap()
            .is_never_expired());

        // case: 键存在，没有过期时间
        let persist = Persist::parse(
            gen_cmdunparsed_test(["key_without_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = persist.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        // case: 键不存在
        let persist = Persist::parse(
            gen_cmdunparsed_test(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = persist.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);
    }

    #[tokio::test]
    async fn pttl_test() {
        let mut handler = gen_test_handler();
        let db = handler.shared.db();

        db.insert_object(
            "key1".as_bytes(),
            Object::with_expire(Str::from("value1"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert_eq!(
            db.get_object("key1".as_bytes()).await.unwrap().expire,
            *NEVER_EXPIRE
        );
        let dur = Duration::from_secs(10);
        let expire = Instant::now() + dur;
        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(Str::from("value_with_ex"), expire),
        )
        .await
        .unwrap();

        // case: 键存在，但没有过期时间
        let pttl = Pttl::parse(
            gen_cmdunparsed_test(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = pttl.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -1);

        // case: 键不存在
        let pttl = Pttl::parse(
            gen_cmdunparsed_test(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = pttl.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -2);

        // case: 键存在且有过期时间
        let pttl = Pttl::parse(
            gen_cmdunparsed_test(["key_with_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = pttl
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .into_integer_unchecked() as u64;
        assert!(dur.as_millis() as u64 - result < ALLOWED_DELTA);
    }

    #[tokio::test]
    async fn ttl_test() {
        let mut handler = gen_test_handler();
        let db = handler.shared.db();

        db.insert_object(
            "key1".as_bytes(),
            Object::with_expire(Str::from("value1"), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert_eq!(
            db.entries.get("key1".as_bytes()).unwrap().expire,
            *NEVER_EXPIRE
        );
        let dur = Duration::from_secs(10);
        let expire = Instant::now() + dur;
        db.insert_object(
            "key_with_ex".as_bytes(),
            Object::with_expire(Str::from("value_with_ex"), expire),
        )
        .await
        .unwrap();

        // case: 键存在，但没有过期时间
        let ttl = Ttl::parse(
            gen_cmdunparsed_test(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = ttl.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -1);

        // case: 键不存在
        let ttl = Ttl::parse(
            gen_cmdunparsed_test(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = ttl.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -2);

        // case: 键存在且有过期时间
        let ttl = Ttl::parse(
            gen_cmdunparsed_test(["key_with_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = ttl
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .into_integer_unchecked() as u64;
        assert!(dur.as_secs() - result < ALLOWED_DELTA);
    }

    #[tokio::test]
    async fn type_test() {
        let mut handler = gen_test_handler();
        let db = handler.shared.db();

        db.insert_object(
            "key1".as_bytes(),
            Object::with_expire(Str::default(), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            "key2".as_bytes(),
            Object::with_expire(List::default(), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            "key3".as_bytes(),
            Object::with_expire(Set::default(), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            "key4".as_bytes(),
            Object::with_expire(Hash::default(), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            "key5".as_bytes(),
            Object::with_expire(ZSet::default(), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: 键存在
        let typ = Type::parse(
            gen_cmdunparsed_test(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = typ
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .into_simple_string_unchecked()
            .to_string();
        assert_eq!(result, "string");

        let typ = Type::parse(
            gen_cmdunparsed_test(["key2"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = typ
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .into_simple_string_unchecked()
            .to_string();
        assert_eq!(result, "list");

        let typ = Type::parse(
            gen_cmdunparsed_test(["key3"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = typ
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .into_simple_string_unchecked()
            .to_string();
        assert_eq!(result, "set");

        let typ = Type::parse(
            gen_cmdunparsed_test(["key4"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = typ
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .into_simple_string_unchecked()
            .to_string();
        assert_eq!(result, "hash");

        let typ = Type::parse(
            gen_cmdunparsed_test(["key5"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = typ
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .into_simple_string_unchecked()
            .to_string();
        assert_eq!(result, "zset");
    }
}
