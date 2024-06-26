use super::*;
use crate::{
    cmd::{
        error::{CmdError, Err},
        CmdExecutor, CmdType, CmdUnparsed,
    },
    conf::AccessControl,
    connection::AsyncStream,
    frame::Resp3,
    persist::rdb::{
        encode_hash_value, encode_list_value, encode_set_value, encode_str_value, encode_zset_value,
    },
    server::Handler,
    shared::db::ObjValueType,
    util::{atoi, epoch},
    CmdFlag, Id, Int, Key,
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
    type Error = &'static str;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match value {
            b"NX" => Ok(Opt::NX),
            b"XX" => Ok(Opt::XX),
            b"GT" => Ok(Opt::GT),
            b"LT" => Ok(Opt::LT),
            _ => Err("ERR invalid option is given"),
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
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = DEL_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut count = 0;
        for key in self.keys {
            if handler.shared.db().remove_object(&key).await.is_some() {
                count += 1;
            }
        }

        Ok(Some(Resp3::new_integer(count)))
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.is_empty() {
            return Err(Err::WrongArgNum.into());
        }

        let keys: Vec<_> = args.collect();
        if ac.is_forbidden_keys(&keys, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

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
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = DUMP_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
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

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    pub keys: Vec<Key>,
}

impl CmdExecutor for Exists {
    const NAME: &'static str = "EXISTS";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = EXISTS_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        for key in self.keys {
            if !handler.shared.db().contains_object(&key).await {
                return Err(0.into());
            }
        }

        Ok(Some(Resp3::new_integer(1)))
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.is_empty() {
            return Err(Err::WrongArgNum.into());
        }

        let keys: Vec<_> = args.collect();
        if ac.is_forbidden_keys(&keys, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

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
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = EXPIRE_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut res = None;

        let new_ex = Instant::now() + self.seconds;
        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                let ex = obj.expire();
                match self.opt {
                    Some(Opt::NX) => {
                        if ex.is_none() {
                            obj.set_expire(Some(new_ex))?;
                            res = Some(Resp3::new_integer(1));
                            return Ok(());
                        }
                    }
                    Some(Opt::XX) => {
                        if ex.is_some() {
                            obj.set_expire(Some(new_ex))?;
                            res = Some(Resp3::new_integer(1));
                            return Ok(());
                        }
                    }
                    Some(Opt::GT) => {
                        if let Some(ex) = ex {
                            if new_ex > ex {
                                obj.set_expire(Some(new_ex))?;

                                res = Some(Resp3::new_integer(1));
                                return Ok(());
                            }
                        }
                    }
                    Some(Opt::LT) => {
                        if let Some(ex) = ex {
                            if new_ex < ex {
                                obj.set_expire(Some(new_ex))?;

                                res = Some(Resp3::new_integer(1));
                                return Ok(());
                            }
                        }
                    }
                    None => {
                        obj.set_expire(Some(new_ex))?;

                        res = Some(Resp3::new_integer(1));
                        return Ok(());
                    }
                }

                Err(CmdError::from(0))
            })
            .await?;

        Ok(res)
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 2 && args.len() != 3 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    key: Key,
    timestamp: Instant,
    opt: Option<Opt>,
}

impl CmdExecutor for ExpireAt {
    const NAME: &'static str = "EXPIREAT";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = EXPIREAT_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut res = None;
        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                let ex = obj.expire();
                match self.opt {
                    Some(Opt::NX) => {
                        if ex.is_none() {
                            obj.set_expire(Some(self.timestamp))?;
                            res = Some(Resp3::new_integer(1));
                            return Ok(());
                        }
                    }
                    Some(Opt::XX) => {
                        if ex.is_some() {
                            obj.set_expire(Some(self.timestamp))?;
                            res = Some(Resp3::new_integer(1));
                            return Ok(());
                        }
                    }
                    Some(Opt::GT) => {
                        if let Some(ex) = ex {
                            if self.timestamp > ex {
                                obj.set_expire(Some(self.timestamp))?;
                                res = Some(Resp3::new_integer(1));
                                return Ok(());
                            }
                        }
                    }
                    Some(Opt::LT) => {
                        if let Some(ex) = ex {
                            if self.timestamp < ex {
                                obj.set_expire(Some(self.timestamp))?;
                                res = Some(Resp3::new_integer(1));
                                return Ok(());
                            }
                        }
                    }
                    None => {
                        obj.set_expire(Some(self.timestamp))?;
                        res = Some(Resp3::new_integer(1));
                        return Ok(());
                    }
                }

                Err(CmdError::from(0))
            })
            .await?;

        Ok(res)
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 2 && args.len() != 3 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

        let timestamp = atoi::<u64>(&args.next().unwrap())?;
        let timestamp = epoch() + Duration::from_secs(timestamp);
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
    pub key: Key,
}

impl CmdExecutor for ExpireTime {
    const NAME: &'static str = "EXPIRETIME";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = EXPIRETIME_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut ex = None;
        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                ex = obj.expire();
                Ok(())
            })
            .await
            .map_err(|_| CmdError::from(-1))?; // 键不存在

        if let Some(ex) = ex {
            Ok(Some(Resp3::new_integer(
                ex.duration_since(epoch()).as_secs() as Int,
            )))
        } else {
            // 无过期时间
            Err((-2).into())
        }
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

        Ok(ExpireTime { key })
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
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = KEYS_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let re = regex::bytes::Regex::new(
            std::str::from_utf8(&self.pattern).map_err(|_| "ERR invalid pattern is given")?,
        )
        .map_err(|_| "ERR invalid pattern is given")?;

        let matched_keys = tokio::task::block_in_place(|| {
            let db = handler.shared.db();
            let matched_keys = if db.size() > (1024 << 32) {
                db.entries()
                    .par_iter()
                    .filter_map(|entry| {
                        re.is_match(entry.key())
                            .then(|| Resp3::new_blob_string(entry.key().clone()))
                    })
                    .collect::<Vec<Resp3>>()
            } else {
                db.entries()
                    .iter()
                    .filter_map(|entry| {
                        re.is_match(entry.key())
                            .then(|| Resp3::new_blob_string(entry.key().clone()))
                    })
                    .collect::<Vec<Resp3>>()
            };

            matched_keys
        });

        Ok(Some(Resp3::new_array(matched_keys)))
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let pattern = args.next().unwrap();
        if ac.is_forbidden_key(&pattern, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = NBKEYS_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let re = regex::Regex::new(&String::from_utf8_lossy(&self.pattern))
            .map_err(|_| "ERR invalid pattern is given")?;

        let shared = handler.shared.clone();
        let bg_sender = if self.redirect != 0 {
            shared
                .db()
                .get_client_bg_sender(self.redirect)
                .ok_or("ERR The client ID you want redirect to does not exist")?
        } else {
            handler.bg_task_channel.new_sender()
        };

        tokio::task::block_in_place(move || {
            let db = shared.db();

            let matched_keys = if db.size() > (1024 << 32) {
                // 并行
                db.entries()
                    .par_iter()
                    .filter_map(|entry| {
                        std::str::from_utf8(entry.key()).ok().and_then(|key| {
                            re.is_match(key)
                                .then(|| Resp3::new_blob_string(entry.key().clone()))
                        })
                    })
                    .collect::<Vec<Resp3>>()
            } else {
                db.entries()
                    .iter()
                    .filter_map(|entry| {
                        std::str::from_utf8(entry.key()).ok().and_then(|key| {
                            re.is_match(key)
                                .then(|| Resp3::new_blob_string(entry.key().clone()))
                        })
                    })
                    .collect::<Vec<Resp3>>()
            };

            let _ = bg_sender.send(Resp3::new_array(matched_keys));
        });

        Ok(None)
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        let pattern = args.next().unwrap();
        if ac.is_forbidden_key(&pattern, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

        Ok(NBKeys {
            pattern,
            redirect: atoi::atoi::<Id>(&args.next().unwrap()).ok_or(Err::A2IParse)?,
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
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = PERSIST_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                if obj.expire().is_none() {
                    return Err(0.into());
                }

                obj.set_expire(None)?;
                Ok(())
            })
            .await
            .map_err(|_| CmdError::from(0))?;

        Ok(Some(Resp3::new_integer(1)))
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    pub key: Key,
}

impl CmdExecutor for Pttl {
    const NAME: &'static str = "PTTL";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = PTTL_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut ex = None;

        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                ex = obj.expire();
                Ok(())
            })
            .await
            .map_err(|_| CmdError::from(-2))?;

        if let Some(ex) = ex {
            let pttl = (ex - Instant::now()).as_millis();
            Ok(Some(Resp3::new_integer(pttl as Int)))
        } else {
            Err((-1).into())
        }
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    pub key: Key,
}

impl CmdExecutor for Ttl {
    const NAME: &'static str = "TTL";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = TTL_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut ex = None;

        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                ex = obj.expire();
                Ok(())
            })
            .await
            .map_err(|_| CmdError::from(-2))?;

        if let Some(ex) = ex {
            let ttl = (ex - Instant::now()).as_secs();
            Ok(Some(Resp3::new_integer(ttl as Int)))
        } else {
            Err((-1).into())
        }
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    pub key: Key,
}

impl CmdExecutor for Type {
    const NAME: &'static str = "TYPE";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = TYPE_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
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

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

        Ok(Type { key })
    }
}

#[cfg(test)]
mod cmd_key_tests {
    use super::*;
    use crate::{
        shared::db::{Hash, List, ObjectInner, Set, Str, ZSet},
        util::epoch,
    };

    // 允许的时间误差
    const ALLOWED_DELTA: u64 = 3;

    #[tokio::test]
    async fn del_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db().clone();

        db.insert_object(Key::from("key1"), ObjectInner::new_str("value1", None))
            .await;
        assert!(db.contains_object(&"key1".into()).await);

        // case: 键存在
        let del = Del::parse(
            &mut CmdUnparsed::from(["DEL", "key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = del.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
        assert!(!db.contains_object(&"key1".into()).await);

        // case: 键不存在
        let del = Del::parse(
            &mut CmdUnparsed::from(["DEL", "key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = del.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(0));
    }

    #[tokio::test]
    async fn exists_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db().clone();

        db.insert_object(Key::from("key1"), ObjectInner::new_str("value1", None))
            .await;
        assert!(db.contains_object(&"key1".into()).await);

        // case: 键存在
        let exists = Exists::parse(
            &mut CmdUnparsed::from(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = exists.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        // case: 键不存在
        let exists = Exists::parse(
            &mut CmdUnparsed::from(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = exists.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);
    }

    #[tokio::test]
    async fn expire_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db().clone();

        db.insert_object(Key::from("key1"), ObjectInner::new_str("value1", None))
            .await;
        assert!(db
            .get_object_entry(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire()
            .is_none());

        // case: 键存在，设置过期时间
        let expire = Expire::parse(
            &mut CmdUnparsed::from(["key1", "10"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
        assert!(db
            .get_object_entry(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire()
            .is_some());

        // case: 键不存在
        let expire = Expire::parse(
            &mut CmdUnparsed::from(["key_nil", "10"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                Some(Instant::now() + Duration::from_secs(10)),
            ),
        )
        .await;
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", None),
        )
        .await;
        // case: with EX option
        let expire = Expire::parse(
            &mut CmdUnparsed::from(["key_with_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire = Expire::parse(
            &mut CmdUnparsed::from(["key_without_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                Some(Instant::now() + Duration::from_secs(10)),
            ),
        )
        .await;
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", None),
        )
        .await;

        // case: with NX option
        let expire = Expire::parse(
            &mut CmdUnparsed::from(["key_with_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire = Expire::parse(
            &mut CmdUnparsed::from(["key_without_ex", "10", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                Some(Instant::now() + Duration::from_secs(10)),
            ),
        )
        .await;
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", None),
        )
        .await;

        // case: with GT option
        let expire = Expire::parse(
            &mut CmdUnparsed::from(["key_with_ex", "5", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire = Expire::parse(
            &mut CmdUnparsed::from(["key_with_ex", "20", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                Some(Instant::now() + Duration::from_secs(10)),
            ),
        )
        .await;
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", None),
        )
        .await;

        // case: with LT option
        let expire = Expire::parse(
            &mut CmdUnparsed::from(["key_with_ex", "20", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire = Expire::parse(
            &mut CmdUnparsed::from(["key_with_ex", "5", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
    }

    #[tokio::test]
    async fn expire_at_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db().clone();

        db.insert_object(Key::from("key1"), ObjectInner::new_str("value1", None))
            .await;
        assert!(db
            .get_object_entry(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire()
            .is_none());

        // case: 键存在，设置过期时间
        let expire_at = ExpireAt::parse(
            &mut CmdUnparsed::from(
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
        assert!(db
            .get_object_entry(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire()
            .is_some());

        // case: 键不存在
        let expire_at = ExpireAt::parse(
            &mut CmdUnparsed::from(["key_nil", "1893427200"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                Some(epoch() + Duration::from_secs(1893427200)),
            ),
        )
        .await;
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", None),
        )
        .await;

        // case: with EX option
        let expire_at = ExpireAt::parse(
            &mut CmdUnparsed::from(["key_with_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            &mut CmdUnparsed::from(["key_without_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                Some(epoch() + Duration::from_secs(1893427200)),
            ),
        )
        .await;
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", None),
        )
        .await;

        // case: with NX option
        let expire_at = ExpireAt::parse(
            &mut CmdUnparsed::from(["key_with_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            &mut CmdUnparsed::from(["key_without_ex", "1893427200", "NX"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                Some(epoch() + Duration::from_secs(1893427200)),
            ),
        )
        .await;
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", None),
        )
        .await;

        // case: with GT option
        let expire_at = ExpireAt::parse(
            &mut CmdUnparsed::from(["key_with_ex", "1893427000", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            &mut CmdUnparsed::from(["key_with_ex", "1893427201", "GT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                Some(epoch() + Duration::from_secs(1893427200)),
            ),
        )
        .await;
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", None),
        )
        .await;

        // case: with LT option
        let expire_at = ExpireAt::parse(
            &mut CmdUnparsed::from(["key_with_ex", "1893427201", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire_at = ExpireAt::parse(
            &mut CmdUnparsed::from(["key_with_ex", "1893427000", "LT"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_at.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
    }

    #[tokio::test]
    async fn expire_time_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db().clone();

        db.insert_object(Key::from("key1"), ObjectInner::new_str("value1", None))
            .await;
        assert!(db
            .get_object_entry(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire()
            .is_none());
        let expire = Instant::now() + Duration::from_secs(10);
        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", Some(expire)),
        )
        .await;

        // case: 键存在，但没有过期时间
        let expire_time = ExpireTime::parse(
            &mut CmdUnparsed::from(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_time.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -1);

        // case: 键不存在
        let expire_time = ExpireTime::parse(
            &mut CmdUnparsed::from(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_time.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -2);

        // case: 键存在且有过期时间
        let expire_time = ExpireTime::parse(
            &mut CmdUnparsed::from(["key_with_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = expire_time.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(
            result,
            Resp3::new_integer(expire.duration_since(epoch()).as_secs() as Int)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn keys_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db().clone();

        db.insert_object(Key::from("key1"), ObjectInner::new_str("value1", None))
            .await;
        db.insert_object(Key::from("key2"), ObjectInner::new_str("value2", None))
            .await;
        db.insert_object(Key::from("key3"), ObjectInner::new_str("value3", None))
            .await;
        db.insert_object(Key::from("key4"), ObjectInner::new_str("value4", None))
            .await;

        let keys = Keys::parse(
            &mut CmdUnparsed::from([".*"].as_ref()),
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
            &mut CmdUnparsed::from(["key*"].as_ref()),
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
            &mut CmdUnparsed::from(["key1"].as_ref()),
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
        let db = handler.shared.db().clone();

        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str(
                "value_with_ex",
                Some(Instant::now() + Duration::from_secs(10)),
            ),
        )
        .await;
        db.insert_object(
            Key::from("key_without_ex"),
            ObjectInner::new_str("value_without_ex", None),
        )
        .await;

        // case: 键存在，有过期时间
        let persist = Persist::parse(
            &mut CmdUnparsed::from(["key_with_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = persist.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(result, Resp3::new_integer(1));
        assert!(db
            .get_object_entry(&"key_with_ex".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire()
            .is_none());

        // case: 键存在，没有过期时间
        let persist = Persist::parse(
            &mut CmdUnparsed::from(["key_without_ex"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = persist.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        // case: 键不存在
        let persist = Persist::parse(
            &mut CmdUnparsed::from(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = persist.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);
    }

    #[tokio::test]
    async fn pttl_test() {
        let (mut handler, _) = Handler::new_fake();
        let db = handler.shared.db().clone();

        db.insert_object(Key::from("key1"), ObjectInner::new_str("value1", None))
            .await;
        assert!(db
            .get_object_entry(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire()
            .is_none());
        let dur = Duration::from_secs(10);
        let expire = Instant::now() + dur;
        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", Some(expire)),
        )
        .await;

        // case: 键存在，但没有过期时间
        let pttl = Pttl::parse(
            &mut CmdUnparsed::from(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = pttl.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -1);

        // case: 键不存在
        let pttl = Pttl::parse(
            &mut CmdUnparsed::from(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = pttl.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -2);

        // case: 键存在且有过期时间
        let pttl = Pttl::parse(
            &mut CmdUnparsed::from(["key_with_ex"].as_ref()),
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

        db.insert_object(Key::from("key1"), ObjectInner::new_str("value1", None))
            .await;
        assert!(db
            .get_object_entry(&"key1".into())
            .await
            .unwrap()
            .inner_unchecked()
            .expire()
            .is_none());
        let dur = Duration::from_secs(10);
        let expire = Instant::now() + dur;
        db.insert_object(
            Key::from("key_with_ex"),
            ObjectInner::new_str("value_with_ex", Some(expire)),
        )
        .await;

        // case: 键存在，但没有过期时间
        let ttl = Ttl::parse(
            &mut CmdUnparsed::from(["key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = ttl.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -1);

        // case: 键不存在
        let ttl = Ttl::parse(
            &mut CmdUnparsed::from(["key_nil"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let result = ttl.execute(&mut handler).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -2);

        // case: 键存在且有过期时间
        let ttl = Ttl::parse(
            &mut CmdUnparsed::from(["key_with_ex"].as_ref()),
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
        let db = handler.shared.db().clone();

        db.insert_object(
            Key::from("key1"),
            ObjectInner::new_str(Str::default(), None),
        )
        .await;
        db.insert_object(
            Key::from("key2"),
            ObjectInner::new_list(List::default(), None),
        )
        .await;
        db.insert_object(
            Key::from("key3"),
            ObjectInner::new_set(Set::default(), None),
        )
        .await;
        db.insert_object(
            Key::from("key4"),
            ObjectInner::new_hash(Hash::default(), None),
        )
        .await;
        db.insert_object(
            Key::from("key5"),
            ObjectInner::new_zset(ZSet::default(), None),
        )
        .await;

        // case: 键存在
        let typ = Type::parse(
            &mut CmdUnparsed::from(["key1"].as_ref()),
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
            &mut CmdUnparsed::from(["key2"].as_ref()),
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
            &mut CmdUnparsed::from(["key3"].as_ref()),
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
            &mut CmdUnparsed::from(["key4"].as_ref()),
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
            &mut CmdUnparsed::from(["key5"].as_ref()),
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
