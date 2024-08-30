use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::Resp3,
    persist::rdb::{
        encode_hash_value, encode_list_value, encode_set_value, encode_str_value, encode_zset_value,
    },
    server::{AsyncStream, Handler},
    shared::{
        db::{as_bytes, ObjValueType, NEVER_EXPIRE},
        Letter, UNIX_EPOCH,
    },
    util::atoi,
    Id, Int, Key,
};
use bytes::{Bytes, BytesMut};
use rayon::prelude::*;
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
    pub keys: Vec<Key>,
}

impl CmdExecutor for Del {
    const NAME: &'static str = "DEL";
    const CATS_FLAG: CatFlag = DEL_CATS_FLAG;
    const CMD_FLAG: CmdFlag = DEL_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut count = 0;
        for key in self.keys {
            if handler.shared.db().remove_object(key).await.is_some() {
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
                Ok(k.into())
            })
            .collect::<RutinResult<Vec<Key>>>()?;

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
    pub key: Key,
}

impl CmdExecutor for Dump {
    const NAME: &'static str = "DUMP";
    const CATS_FLAG: CatFlag = DUMP_CATS_FLAG;
    const CMD_FLAG: CmdFlag = DUMP_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut buf = BytesMut::with_capacity(1024);
        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                match obj.typ() {
                    ObjValueType::Str => encode_str_value(&mut buf, obj.on_str()?.clone()),
                    ObjValueType::List => encode_list_value(&mut buf, obj.on_list()?.clone()),
                    ObjValueType::Set => encode_set_value(&mut buf, obj.on_set()?.clone()),
                    ObjValueType::Hash => encode_hash_value(&mut buf, obj.on_hash()?.clone()),
                    ObjValueType::ZSet => encode_zset_value(&mut buf, obj.on_zset()?.clone()),
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

        Ok(Dump { key: key.into() })
    }
}

/// 检查给定 key 是否存在。
/// # Reply:
///
/// **Integer reply:** the number of keys that exist from those specified as arguments.
#[derive(Debug)]
pub struct Exists {
    pub keys: Vec<Key>,
}

impl CmdExecutor for Exists {
    const NAME: &'static str = "EXISTS";
    const CATS_FLAG: CatFlag = EXISTS_CATS_FLAG;
    const CMD_FLAG: CmdFlag = EXISTS_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        for key in self.keys {
            if !handler.shared.db().contains_object(&key).await {
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
                Ok(k.into())
            })
            .collect::<RutinResult<Vec<Key>>>()?;

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
    key: Key,
    seconds: Duration,
    opt: Option<Opt>,
}

impl CmdExecutor for Expire {
    const NAME: &'static str = "EXPIRE";
    const CATS_FLAG: CatFlag = EXPIRE_CATS_FLAG;
    const CMD_FLAG: CmdFlag = EXPIRE_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut res = None;

        let new_ex = Instant::now() + self.seconds;
        handler
            .shared
            .db()
            .update_object(self.key, |obj| {
                let ex = obj.expire_expect_never();
                match self.opt {
                    Some(Opt::NX) => {
                        if ex.is_none() {
                            obj.set_expire_unchecked(new_ex)?;
                            res = Some(Resp3::new_integer(1));
                            return Ok(());
                        }
                    }
                    Some(Opt::XX) => {
                        if ex.is_some() {
                            obj.set_expire_unchecked(new_ex)?;
                            res = Some(Resp3::new_integer(1));
                            return Ok(());
                        }
                    }
                    Some(Opt::GT) => {
                        if let Some(ex) = ex {
                            if new_ex > ex {
                                obj.set_expire_unchecked(new_ex)?;

                                res = Some(Resp3::new_integer(1));
                                return Ok(());
                            }
                        }
                    }
                    Some(Opt::LT) => {
                        if let Some(ex) = ex {
                            if new_ex < ex {
                                obj.set_expire_unchecked(new_ex)?;

                                res = Some(Resp3::new_integer(1));
                                return Ok(());
                            }
                        }
                    }
                    None => {
                        obj.set_expire_unchecked(new_ex)?;

                        res = Some(Resp3::new_integer(1));
                        return Ok(());
                    }
                }

                Err(RutinError::from(0))
            })
            .await?;

        Ok(res)
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

        Ok(Expire {
            key: key.into(),
            seconds,
            opt,
        })
    }
}

/// # Reply:
///
/// **Integer reply:** 0 if the timeout was not set; for example, the key doesn't exist, or the operation was skipped because of the provided arguments.
/// **Integer reply:** 1 if the timeout was set.
#[derive(Debug)]
pub struct ExpireAt {
    key: Key,
    timestamp: Instant,
    opt: Option<Opt>,
}

impl CmdExecutor for ExpireAt {
    const NAME: &'static str = "EXPIREAT";
    const CATS_FLAG: CatFlag = EXPIREAT_CATS_FLAG;
    const CMD_FLAG: CmdFlag = EXPIREAT_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut res = None;
        handler
            .shared
            .db()
            .update_object(self.key, |obj| {
                let ex = obj.expire_expect_never();
                match self.opt {
                    Some(Opt::NX) => {
                        if ex.is_none() {
                            obj.set_expire_unchecked(self.timestamp)?;
                            res = Some(Resp3::new_integer(1));
                            return Ok(());
                        }
                    }
                    Some(Opt::XX) => {
                        if ex.is_some() {
                            obj.set_expire_unchecked(self.timestamp)?;
                            res = Some(Resp3::new_integer(1));
                            return Ok(());
                        }
                    }
                    Some(Opt::GT) => {
                        if let Some(ex) = ex {
                            if self.timestamp > ex {
                                obj.set_expire_unchecked(self.timestamp)?;
                                res = Some(Resp3::new_integer(1));
                                return Ok(());
                            }
                        }
                    }
                    Some(Opt::LT) => {
                        if let Some(ex) = ex {
                            if self.timestamp < ex {
                                obj.set_expire_unchecked(self.timestamp)?;
                                res = Some(Resp3::new_integer(1));
                                return Ok(());
                            }
                        }
                    }
                    None => {
                        obj.set_expire_unchecked(self.timestamp)?;
                        res = Some(Resp3::new_integer(1));
                        return Ok(());
                    }
                }

                Err(RutinError::from(0))
            })
            .await?;

        Ok(res)
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
            key: key.into(),
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
    pub key: Key,
}

impl CmdExecutor for ExpireTime {
    const NAME: &'static str = "EXPIRETIME";
    const CATS_FLAG: CatFlag = EXPIRETIME_CATS_FLAG;
    const CMD_FLAG: CmdFlag = EXPIRETIME_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut ex = None;
        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                ex = obj.expire_expect_never();
                Ok(())
            })
            .await
            .map_err(|_| RutinError::from(-1))?; // 键不存在

        if let Some(ex) = ex {
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

        Ok(ExpireTime { key: key.into() })
    }
}

/// # Reply:
///
/// **Array reply:** a list of keys matching pattern.
#[derive(Debug)]
pub struct Keys {
    pub pattern: Bytes,
}

// TODO: 提供非阻塞操作
impl CmdExecutor for Keys {
    const NAME: &'static str = "KEYS";
    const CATS_FLAG: CatFlag = KEYS_CATS_FLAG;
    const CMD_FLAG: CmdFlag = KEYS_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let re = regex::bytes::Regex::new(std::str::from_utf8(&self.pattern)?)?;

        let matched_keys = tokio::task::block_in_place(|| {
            let db = handler.shared.db();
            let matched_keys = if db.entries_size() > (1024 << 32) {
                db.entries()
                    .par_iter()
                    .filter_map(|entry| {
                        re.is_match(as_bytes!(entry.key()))
                            .then(|| Resp3::new_blob_string(entry.key().to_bytes()))
                    })
                    .collect::<Vec<Resp3>>()
            } else {
                db.entries()
                    .iter()
                    .filter_map(|entry| {
                        re.is_match(as_bytes!(entry.key()))
                            .then(|| Resp3::new_blob_string(entry.key().to_bytes()))
                    })
                    .collect::<Vec<Resp3>>()
            };

            matched_keys
        });

        Ok(Some(Resp3::new_array(matched_keys)))
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
// TODO: 也许应该返回Resp3::Push?
#[derive(Debug)]
pub struct NBKeys {
    pattern: Bytes,
    redirect: Id, // 0表示不重定向
}

impl CmdExecutor for NBKeys {
    const NAME: &'static str = "NBKEYS";
    const CATS_FLAG: CatFlag = NBKEYS_CATS_FLAG;
    const CMD_FLAG: CmdFlag = NBKEYS_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let re = regex::Regex::new(&String::from_utf8_lossy(&self.pattern))?;

        let outbox = if self.redirect != 0 {
            &handler
                .shared
                .post_office()
                .get_outbox(self.redirect)
                .ok_or(RutinError::from("ERR The client ID does not exist"))?
        } else {
            &handler.context.outbox
        };

        tokio::task::block_in_place(|| {
            let db = handler.shared.db();

            let matched_keys = if db.entries_size() > (1024 << 32) {
                // 并行
                db.entries()
                    .par_iter()
                    .filter_map(|entry| {
                        std::str::from_utf8(as_bytes!(entry.key()))
                            .ok()
                            .and_then(|key| {
                                re.is_match(key)
                                    .then(|| Resp3::new_blob_string(entry.key().to_bytes()))
                            })
                    })
                    .collect::<Vec<Resp3>>()
            } else {
                db.entries()
                    .iter()
                    .filter_map(|entry| {
                        std::str::from_utf8(as_bytes!(entry.key()))
                            .ok()
                            .and_then(|key| {
                                re.is_match(key)
                                    .then(|| Resp3::new_blob_string(entry.key().to_bytes()))
                            })
                    })
                    .collect::<Vec<Resp3>>()
            };

            let _ = outbox.send(Letter::Resp3(Resp3::new_array(matched_keys)));
        });

        Ok(None)
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let pattern = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&pattern, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(NBKeys {
            pattern,
            redirect: atoi::<Id>(&args.next().unwrap())?,
        })
    }
}

/// 移除 key 的过期时间，key 将持久保持。
/// # Reply:
///
/// **Integer reply:** 0 if key does not exist or does not have an associated timeout.
/// **Integer reply:** 1 if the timeout has been removed.
#[derive(Debug)]
pub struct Persist {
    pub key: Key,
}

impl CmdExecutor for Persist {
    const NAME: &'static str = "PERSIST";
    const CATS_FLAG: CatFlag = PERSIST_CATS_FLAG;
    const CMD_FLAG: CmdFlag = PERSIST_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        handler
            .shared
            .db()
            .update_object(self.key, |obj| {
                if obj.expire_expect_never().is_none() {
                    return Err(0.into());
                }

                obj.set_expire_unchecked(*NEVER_EXPIRE)?;
                Ok(())
            })
            .await
            .map_err(|_| RutinError::from(0))?;

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

        Ok(Persist { key: key.into() })
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
    pub key: Key,
}

impl CmdExecutor for Pttl {
    const NAME: &'static str = "PTTL";
    const CATS_FLAG: CatFlag = PTTL_CATS_FLAG;
    const CMD_FLAG: CmdFlag = PTTL_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut ex = None;

        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                ex = obj.expire_expect_never();
                Ok(())
            })
            .await
            .map_err(|_| RutinError::from(-2))?;

        if let Some(ex) = ex {
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

        Ok(Pttl { key: key.into() })
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
    pub key: Key,
}

impl CmdExecutor for Ttl {
    const NAME: &'static str = "TTL";
    const CATS_FLAG: CatFlag = TTL_CATS_FLAG;
    const CMD_FLAG: CmdFlag = TTL_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut ex = None;

        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                ex = obj.expire_expect_never();
                Ok(())
            })
            .await
            .map_err(|_| RutinError::from(-2))?;

        if let Some(ex) = ex {
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

        Ok(Ttl { key: key.into() })
    }
}

/// 返回 key 所储存的值的类型。
/// # Reply:
///
/// **Simple string reply:** the type of key, or none when key doesn't exist.
#[derive(Debug)]
pub struct Type {
    pub key: Key,
}

impl CmdExecutor for Type {
    const NAME: &'static str = "TYPE";
    const CATS_FLAG: CatFlag = TYPE_CATS_FLAG;
    const CMD_FLAG: CmdFlag = TYPE_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut typ = "";

        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                typ = obj.type_str();
                Ok(())
            })
            .await?;

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

        Ok(Type { key: key.into() })
    }
}

#[cfg(test)]
mod cmd_key_tests {
    use super::*;
    use crate::shared::{
        db::{Hash, List, ObjectInner, Set, Str, ZSet},
        UNIX_EPOCH,
    };

    // 允许的时间误差
    const ALLOWED_DELTA: u64 = 3;

    #[tokio::test]
    async fn del_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db();

        db.insert_object(
            Key::from("key1"),
            ObjectInner::new_str("value1", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db.contains_object(&"key1".into()).await);

        // case: 键存在
        let del = Del::parse(
            CmdUnparsed::from(["DEL", "key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = del.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
        assert!(!handler.shared.db().contains_object(&"key1".into()).await);

        // case: 键不存在
        let del = Del::parse(
            CmdUnparsed::from(["DEL", "key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = del.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(0));
    }

    #[tokio::test]
    async fn exists_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db();

        db.insert_object(
            Key::from("key1"),
            ObjectInner::new_str("value1", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db.contains_object(&"key1".into()).await);

        // case: 键存在
        let exists = Exists::parse(
            CmdUnparsed::from(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = exists.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        // case: 键不存在
        let exists = Exists::parse(
            CmdUnparsed::from(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = exists.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);
    }

    #[tokio::test]
    async fn expire_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db();

        db.insert_object(
            Key::from("key1"),
            ObjectInner::new_str("value1", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db
            .get(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire_expect_never()
            .is_none());

        // case: 键存在，设置过期时间
        let expire = Expire::parse(
            CmdUnparsed::from(["key1", "10"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
        assert!(handler
            .shared
            .db()
            .get(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire_expect_never()
            .is_some());

        // case: 键不存在
        let expire = Expire::parse(
            CmdUnparsed::from(["key_nil", "10"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let db = handler.shared.db();

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", Instant::now() + Duration::from_secs(10)),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        // case: with EX option
        let expire = Expire::parse(
            CmdUnparsed::from(["key_with_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire = Expire::parse(
            CmdUnparsed::from(["key_without_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", Instant::now() + Duration::from_secs(10)),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with NX option
        let expire = Expire::parse(
            CmdUnparsed::from(["key_with_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire = Expire::parse(
            CmdUnparsed::from(["key_without_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", Instant::now() + Duration::from_secs(10)),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with GT option
        let expire = Expire::parse(
            CmdUnparsed::from(["key_with_ex", "5", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire = Expire::parse(
            CmdUnparsed::from(["key_with_ex", "20", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", Instant::now() + Duration::from_secs(10)),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with LT option
        let expire = Expire::parse(
            CmdUnparsed::from(["key_with_ex", "20", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire = Expire::parse(
            CmdUnparsed::from(["key_with_ex", "5", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
    }

    #[tokio::test]
    async fn expire_at_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db();

        db.insert_object(
            Key::from("key1"),
            ObjectInner::new_str("value1", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db
            .get(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire_expect_never()
            .is_none());

        // case: 键存在，设置过期时间
        let expire_at = ExpireAt::parse(
            CmdUnparsed::from(
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
        assert!(handler
            .shared
            .db()
            .get(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire_expect_never()
            .is_some());

        // case: 键不存在
        let expire_at = ExpireAt::parse(
            CmdUnparsed::from(["key_nil", "1893427200"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let db = handler.shared.db();

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                *UNIX_EPOCH + Duration::from_secs(1893427200),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with EX option
        let expire_at = ExpireAt::parse(
            CmdUnparsed::from(["key_with_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            CmdUnparsed::from(["key_without_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                *UNIX_EPOCH + Duration::from_secs(1893427200),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with NX option
        let expire_at = ExpireAt::parse(
            CmdUnparsed::from(["key_with_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            CmdUnparsed::from(["key_without_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                *UNIX_EPOCH + Duration::from_secs(1893427200),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with GT option
        let expire_at = ExpireAt::parse(
            CmdUnparsed::from(["key_with_ex", "1893427000", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            CmdUnparsed::from(["key_with_ex", "1893427201", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        let db = handler.shared.db();

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                *UNIX_EPOCH + Duration::from_secs(1893427200),
            ),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: with LT option
        let expire_at = ExpireAt::parse(
            CmdUnparsed::from(["key_with_ex", "1893427201", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            CmdUnparsed::from(["key_with_ex", "1893427000", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
    }

    #[tokio::test]
    async fn expire_time_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db();

        db.insert_object(
            Key::from("key1"),
            ObjectInner::new_str("value1", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db
            .get(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire_expect_never()
            .is_none());
        let expire = Instant::now() + Duration::from_secs(10);
        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", expire),
        )
        .await
        .unwrap();

        // case: 键存在，但没有过期时间
        let expire_time = ExpireTime::parse(
            CmdUnparsed::from(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_time.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -1);

        // case: 键不存在
        let expire_time = ExpireTime::parse(
            CmdUnparsed::from(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_time.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -2);

        // case: 键存在且有过期时间
        let expire_time = ExpireTime::parse(
            CmdUnparsed::from(["key_with_ex"].as_ref()),
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
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db();

        db.insert_object(
            Key::from("key1"),
            ObjectInner::new_str("value1", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key2"),
            ObjectInner::new_str("value2", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key3"),
            ObjectInner::new_str("value3", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key4"),
            ObjectInner::new_str("value4", *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        let keys = Keys::parse(
            CmdUnparsed::from([".*"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = keys
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .try_array()
            .unwrap()
            .to_vec();
        assert!(
            result.contains(&Resp3::new_blob_string("key1".into()))
                && result.contains(&Resp3::new_blob_string("key2".into()))
                && result.contains(&Resp3::new_blob_string("key3".into()))
                && result.contains(&Resp3::new_blob_string("key4".into()))
        );

        let keys = Keys::parse(
            CmdUnparsed::from(["key*"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = keys
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .try_array()
            .unwrap()
            .to_vec();
        assert!(
            result.contains(&Resp3::new_blob_string("key1".into()))
                && result.contains(&Resp3::new_blob_string("key2".into()))
                && result.contains(&Resp3::new_blob_string("key3".into()))
                && result.contains(&Resp3::new_blob_string("key4".into()))
        );

        let keys = Keys::parse(
            CmdUnparsed::from(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = keys
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .try_array()
            .unwrap()
            .to_vec();
        assert!(result.contains(&Resp3::new_blob_string("key1".into())));
    }

    #[tokio::test]
    async fn persist_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db();

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", Instant::now() + Duration::from_secs(10)),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: 键存在，有过期时间
        let persist = Persist::parse(
            CmdUnparsed::from(["key_with_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = persist.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
        assert!(handler
            .shared
            .db()
            .get(&"key_with_ex".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire_expect_never()
            .is_none());

        // case: 键存在，没有过期时间
        let persist = Persist::parse(
            CmdUnparsed::from(["key_without_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = persist.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);

        // case: 键不存在
        let persist = Persist::parse(
            CmdUnparsed::from(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = persist.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == 0);
    }

    #[tokio::test]
    async fn pttl_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db();

        db.insert_object(
            Key::from("key1"),
            ObjectInner::new_str("value1", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db
            .get(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire_expect_never()
            .is_none());
        let dur = Duration::from_secs(10);
        let expire = Instant::now() + dur;
        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", expire),
        )
        .await
        .unwrap();

        // case: 键存在，但没有过期时间
        let pttl = Pttl::parse(
            CmdUnparsed::from(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = pttl.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -1);

        // case: 键不存在
        let pttl = Pttl::parse(
            CmdUnparsed::from(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = pttl.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -2);

        // case: 键存在且有过期时间
        let pttl = Pttl::parse(
            CmdUnparsed::from(["key_with_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = pttl
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .try_integer()
            .unwrap() as u64;
        assert!(dur.as_millis() as u64 - result < ALLOWED_DELTA);
    }

    #[tokio::test]
    async fn ttl_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db();

        db.insert_object(
            Key::from("key1"),
            ObjectInner::new_str("value1", *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        assert!(db
            .get(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire_expect_never()
            .is_none());
        let dur = Duration::from_secs(10);
        let expire = Instant::now() + dur;
        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", expire),
        )
        .await
        .unwrap();

        // case: 键存在，但没有过期时间
        let ttl = Ttl::parse(
            CmdUnparsed::from(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = ttl.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -1);

        // case: 键不存在
        let ttl = Ttl::parse(
            CmdUnparsed::from(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = ttl.execute(&mut handler).await.unwrap_err();
        matches!(result, RutinError::ErrCode { code } if code == -2);

        // case: 键存在且有过期时间
        let ttl = Ttl::parse(
            CmdUnparsed::from(["key_with_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = ttl
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .try_integer()
            .unwrap() as u64;
        assert!(dur.as_secs() - result < ALLOWED_DELTA);
    }

    #[tokio::test]
    async fn type_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db();

        db.insert_object(
            Key::from("key1"),
            ObjectInner::new_str(Str::default(), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key2"),
            ObjectInner::new_list(List::default(), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key3"),
            ObjectInner::new_set(Set::default(), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key4"),
            ObjectInner::new_hash(Hash::default(), *NEVER_EXPIRE),
        )
        .await
        .unwrap();
        db.insert_object(
            Key::from("key5"),
            ObjectInner::new_zset(ZSet::default(), *NEVER_EXPIRE),
        )
        .await
        .unwrap();

        // case: 键存在
        let typ = Type::parse(
            CmdUnparsed::from(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = typ
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .try_simple_string()
            .unwrap()
            .to_string();
        assert_eq!(result, "string");

        let typ = Type::parse(
            CmdUnparsed::from(["key2"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = typ
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .try_simple_string()
            .unwrap()
            .to_string();
        assert_eq!(result, "list");

        let typ = Type::parse(
            CmdUnparsed::from(["key3"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = typ
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .try_simple_string()
            .unwrap()
            .to_string();
        assert_eq!(result, "set");

        let typ = Type::parse(
            CmdUnparsed::from(["key4"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = typ
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .try_simple_string()
            .unwrap()
            .to_string();
        assert_eq!(result, "hash");

        let typ = Type::parse(
            CmdUnparsed::from(["key5"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = typ
            .execute(&mut handler)
            .await
            .unwrap()
            .unwrap()
            .try_simple_string()
            .unwrap()
            .to_string();
        assert_eq!(result, "zset");
    }
}
