use crate::{
    cmd::{
        error::{CmdError, Err},
        CmdExecutor, CmdType, CmdUnparsed,
    },
    connection::AsyncStream,
    frame::RESP3,
    persist::rdb::{
        encode_hash_value, encode_list_value, encode_set_value, encode_str_value, encode_zset_value,
    },
    server::Handler,
    shared::{db::ObjValueType, Shared},
    util::atoi,
    Id, Int, Key, EPOCH,
};
use bytes::{Bytes, BytesMut};
use rayon::prelude::*;
use std::time::Duration;
use tokio::time::Instant;

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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut count = 0;
        for key in self.keys {
            if shared.db().remove_object(&key).is_some() {
                count += 1;
            }
        }

        Ok(Some(RESP3::Integer(count)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.is_empty() {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Del {
            keys: args.collect(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut buf = BytesMut::with_capacity(1024);
        shared.db().visit_object(&self.key, |obj| {
            match obj.typ() {
                ObjValueType::Str => encode_str_value(&mut buf, obj.on_str()?.clone()),
                ObjValueType::List => encode_list_value(&mut buf, obj.on_list()?.clone()),
                ObjValueType::Set => encode_set_value(&mut buf, obj.on_set()?.clone()),
                ObjValueType::Hash => encode_hash_value(&mut buf, obj.on_hash()?.clone()),
                ObjValueType::ZSet => encode_zset_value(&mut buf, obj.on_zset()?.clone()),
            }

            Ok(())
        })?;

        Ok(Some(RESP3::Bulk(buf.freeze())))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Dump {
            key: args.next().unwrap(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        for key in self.keys {
            if !shared.db().contains_object(&key) {
                return Err(0.into());
            }
        }

        Ok(Some(RESP3::Integer(1)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.is_empty() {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Exists {
            keys: args.collect(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut res = None;

        let new_ex = Instant::now() + self.seconds;
        shared.db().update_object(&self.key, |obj| {
            let ex = obj.expire();
            match self.opt {
                Some(Opt::NX) => {
                    if ex.is_none() {
                        obj.set_expire(Some(new_ex))?;
                        res = Some(RESP3::Integer(1));
                        return Ok(());
                    }
                }
                Some(Opt::XX) => {
                    if ex.is_some() {
                        obj.set_expire(Some(new_ex))?;
                        res = Some(RESP3::Integer(1));
                        return Ok(());
                    }
                }
                Some(Opt::GT) => {
                    if let Some(ex) = ex {
                        if new_ex > ex {
                            obj.set_expire(Some(new_ex))?;

                            res = Some(RESP3::Integer(1));
                            return Ok(());
                        }
                    }
                }
                Some(Opt::LT) => {
                    if let Some(ex) = ex {
                        if new_ex < ex {
                            obj.set_expire(Some(new_ex))?;

                            res = Some(RESP3::Integer(1));
                            return Ok(());
                        }
                    }
                }
                None => {
                    obj.set_expire(Some(new_ex))?;

                    res = Some(RESP3::Integer(1));
                    return Ok(());
                }
            }

            Err(CmdError::from(0))
        })?;

        Ok(res)
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 2 && args.len() != 3 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut res = None;
        shared.db().update_object(&self.key, |obj| {
            let ex = obj.expire();
            match self.opt {
                Some(Opt::NX) => {
                    if ex.is_none() {
                        obj.set_expire(Some(self.timestamp))?;
                        res = Some(RESP3::Integer(1));
                        return Ok(());
                    }
                }
                Some(Opt::XX) => {
                    if ex.is_some() {
                        obj.set_expire(Some(self.timestamp))?;
                        res = Some(RESP3::Integer(1));
                        return Ok(());
                    }
                }
                Some(Opt::GT) => {
                    if let Some(ex) = ex {
                        if self.timestamp > ex {
                            obj.set_expire(Some(self.timestamp))?;
                            res = Some(RESP3::Integer(1));
                            return Ok(());
                        }
                    }
                }
                Some(Opt::LT) => {
                    if let Some(ex) = ex {
                        if self.timestamp < ex {
                            obj.set_expire(Some(self.timestamp))?;
                            res = Some(RESP3::Integer(1));
                            return Ok(());
                        }
                    }
                }
                None => {
                    obj.set_expire(Some(self.timestamp))?;
                    res = Some(RESP3::Integer(1));
                    return Ok(());
                }
            }

            Err(CmdError::from(0))
        })?;

        Ok(res)
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 2 && args.len() != 3 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        let timestamp = atoi::<u64>(&args.next().unwrap())?;
        let timestamp = *EPOCH + Duration::from_secs(timestamp);
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut ex = None;
        shared
            .db()
            .visit_object(&self.key, |obj| {
                ex = obj.expire();
                Ok(())
            })
            .map_err(|_| CmdError::from(-1))?; // 键不存在

        if let Some(ex) = ex {
            Ok(Some(RESP3::Integer(
                ex.duration_since(*EPOCH).as_secs() as Int
            )))
        } else {
            // 无过期时间
            Err((-2).into())
        }
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(ExpireTime {
            key: args.next().unwrap(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let re = regex::Regex::new(&String::from_utf8_lossy(&self.pattern))
            .map_err(|_| "ERR invalid pattern is given")?;

        let db = shared.db();
        let matched_keys = if db.size() > (1024 << 32) {
            db.entries()
                .par_iter()
                .filter_map(|entry| {
                    std::str::from_utf8(entry.key())
                        .ok()
                        .and_then(|key| re.is_match(key).then(|| RESP3::Bulk(entry.key().clone())))
                })
                .collect::<Vec<RESP3>>()
        } else {
            db.entries()
                .iter()
                .filter_map(|entry| {
                    std::str::from_utf8(entry.key())
                        .ok()
                        .and_then(|key| re.is_match(key).then(|| RESP3::Bulk(entry.key().clone())))
                })
                .collect::<Vec<RESP3>>()
        };

        Ok(Some(RESP3::Array(matched_keys)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Keys {
            pattern: args.next().unwrap(),
        })
    }
}
/// # Reply:
///
/// **Array reply:** a list of keys matching pattern.
#[derive(Debug)]
pub struct NBKeys {
    pattern: Bytes,
    redirect: Id, // 0表示不重定向
}

impl CmdExecutor for NBKeys {
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<RESP3>, CmdError> {
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

        tokio::task::spawn_blocking(move || {
            let db = shared.db();

            let matched_keys = if db.size() > (1024 << 32) {
                db.entries()
                    .par_iter()
                    .filter_map(|entry| {
                        std::str::from_utf8(entry.key()).ok().and_then(|key| {
                            re.is_match(key).then(|| RESP3::Bulk(entry.key().clone()))
                        })
                    })
                    .collect::<Vec<RESP3>>()
            } else {
                db.entries()
                    .iter()
                    .filter_map(|entry| {
                        std::str::from_utf8(entry.key()).ok().and_then(|key| {
                            re.is_match(key).then(|| RESP3::Bulk(entry.key().clone()))
                        })
                    })
                    .collect::<Vec<RESP3>>()
            };

            let _ = bg_sender.send(RESP3::Array(matched_keys));
        });

        Ok(None)
    }
    async fn _execute(self, _shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        Ok(None)
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(NBKeys {
            pattern: args.next().unwrap(),
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        shared
            .db()
            .update_object(&self.key, |obj| {
                if obj.expire().is_none() {
                    return Err(0.into());
                }

                obj.set_expire(None)?;
                Ok(())
            })
            .map_err(|_| CmdError::from(0))?;

        Ok(Some(RESP3::Integer(1)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Persist {
            key: args.next().unwrap(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut ex = None;

        shared
            .db()
            .visit_object(&self.key, |obj| {
                ex = obj.expire();
                Ok(())
            })
            .map_err(|_| CmdError::from(-2))?;

        if let Some(ex) = ex {
            let pttl = (ex - Instant::now()).as_millis();
            Ok(Some(RESP3::Integer(pttl as Int)))
        } else {
            Err((-1).into())
        }
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Pttl {
            key: args.next().unwrap(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut ex = None;

        shared
            .db()
            .visit_object(&self.key, |obj| {
                ex = obj.expire();
                Ok(())
            })
            .map_err(|_| CmdError::from(-2))?;

        if let Some(ex) = ex {
            let ttl = (ex - Instant::now()).as_secs();
            Ok(Some(RESP3::Integer(ttl as Int)))
        } else {
            Err((-1).into())
        }
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Ttl {
            key: args.next().unwrap(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut typ = "";

        shared.db().visit_object(&self.key, |obj| {
            typ = obj.type_str();
            Ok(())
        })?;

        Ok(Some(RESP3::SimpleString(typ.into())))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Type {
            key: args.next().unwrap(),
        })
    }
}

#[cfg(test)]
mod cmd_key_tests {
    use super::*;
    use crate::shared::{
        db::{db_tests::get_object, Hash, List, Object, Set, Str, ZSet},
        Shared,
    };

    // 允许的时间误差
    const ALLOWED_DELTA: u64 = 3;

    #[tokio::test]
    async fn del_test() {
        let shared = Shared::default();
        let db = shared.db();

        db.insert_object(Key::from("key1"), Object::new_str("value1".into(), None));
        assert!(db.contains_object(b"key1"));

        // case: 键存在
        let del = Del::parse(&mut CmdUnparsed::from(["DEL", "key1"].as_ref())).unwrap();
        let result = del._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));
        assert!(!db.contains_object(b"key1"));

        // case: 键不存在
        let del = Del::parse(&mut CmdUnparsed::from(["DEL", "key_nil"].as_ref())).unwrap();
        let result = del._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(0));
    }

    #[tokio::test]
    async fn exists_test() {
        let shared = Shared::default();
        let db = shared.db();

        db.insert_object(Key::from("key1"), Object::new_str("value1".into(), None));
        assert!(db.contains_object(b"key1"));

        // case: 键存在
        let exists = Exists::parse(&mut CmdUnparsed::from(["key1"].as_ref())).unwrap();
        let result = exists._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));

        // case: 键不存在
        let exists = Exists::parse(&mut CmdUnparsed::from(["key_nil"].as_ref())).unwrap();
        let result = exists._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);
    }

    #[tokio::test]
    async fn expire_test() {
        let shared = Shared::default();
        let db = shared.db();

        db.insert_object(Key::from("key1"), Object::new_str("value1".into(), None));
        assert!(get_object(db, b"key1").unwrap().expire().is_none());

        // case: 键存在，设置过期时间
        let expire = Expire::parse(&mut CmdUnparsed::from(["key1", "10"].as_ref())).unwrap();
        let result = expire._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));
        assert!(get_object(db, b"key1").unwrap().expire().is_some());

        // case: 键不存在
        let expire = Expire::parse(&mut CmdUnparsed::from(["key_nil", "10"].as_ref())).unwrap();
        let result = expire._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str(
                "value_with_ex".into(),
                Some(Instant::now() + Duration::from_secs(10)),
            ),
        );
        db.insert_object(
            Key::from("key_without_ex"),
            Object::new_str("value_without_ex".into(), None),
        );
        // case: with EX option
        let expire =
            Expire::parse(&mut CmdUnparsed::from(["key_with_ex", "10", "NX"].as_ref())).unwrap();
        let result = expire._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire = Expire::parse(&mut CmdUnparsed::from(
            ["key_without_ex", "10", "NX"].as_ref(),
        ))
        .unwrap();
        let result = expire._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str(
                "value_with_ex".into(),
                Some(Instant::now() + Duration::from_secs(10)),
            ),
        );
        db.insert_object(
            Key::from("key_without_ex"),
            Object::new_str("value_without_ex".into(), None),
        );

        // case: with NX option
        let expire =
            Expire::parse(&mut CmdUnparsed::from(["key_with_ex", "10", "NX"].as_ref())).unwrap();
        let result = expire._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire = Expire::parse(&mut CmdUnparsed::from(
            ["key_without_ex", "10", "NX"].as_ref(),
        ))
        .unwrap();
        let result = expire._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str(
                "value_with_ex".into(),
                Some(Instant::now() + Duration::from_secs(10)),
            ),
        );
        db.insert_object(
            Key::from("key_without_ex"),
            Object::new_str("value_without_ex".into(), None),
        );

        // case: with GT option
        let expire =
            Expire::parse(&mut CmdUnparsed::from(["key_with_ex", "5", "GT"].as_ref())).unwrap();
        let result = expire._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire =
            Expire::parse(&mut CmdUnparsed::from(["key_with_ex", "20", "GT"].as_ref())).unwrap();
        let result = expire._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str(
                "value_with_ex".into(),
                Some(Instant::now() + Duration::from_secs(10)),
            ),
        );
        db.insert_object(
            Key::from("key_without_ex"),
            Object::new_str("value_without_ex".into(), None),
        );

        // case: with LT option
        let expire =
            Expire::parse(&mut CmdUnparsed::from(["key_with_ex", "20", "LT"].as_ref())).unwrap();
        let result = expire._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire =
            Expire::parse(&mut CmdUnparsed::from(["key_with_ex", "5", "LT"].as_ref())).unwrap();
        let result = expire._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));
    }

    #[tokio::test]
    async fn expire_at_test() {
        let shared = Shared::default();
        let db = shared.db();

        db.insert_object(Key::from("key1"), Object::new_str("value1".into(), None));
        assert!(get_object(db, b"key1").unwrap().expire().is_none());

        // case: 键存在，设置过期时间
        let expire_at = ExpireAt::parse(&mut CmdUnparsed::from(
            [
                "key1",
                "1893427200", // 2030-01-01 00:00:00
            ]
            .as_ref(),
        ))
        .unwrap();
        let result = expire_at._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));
        assert!(get_object(db, b"key1").unwrap().expire().is_some());

        // case: 键不存在
        let expire_at =
            ExpireAt::parse(&mut CmdUnparsed::from(["key_nil", "1893427200"].as_ref())).unwrap();
        let result = expire_at._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str(
                "value_with_ex".into(),
                Some(*EPOCH + Duration::from_secs(1893427200)),
            ),
        );
        db.insert_object(
            Key::from("key_without_ex"),
            Object::new_str("value_without_ex".into(), None),
        );

        // case: with EX option
        let expire_at = ExpireAt::parse(&mut CmdUnparsed::from(
            ["key_with_ex", "1893427200", "NX"].as_ref(),
        ))
        .unwrap();
        let result = expire_at._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire_at = ExpireAt::parse(&mut CmdUnparsed::from(
            ["key_without_ex", "1893427200", "NX"].as_ref(),
        ))
        .unwrap();
        let result = expire_at._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str(
                "value_with_ex".into(),
                Some(*EPOCH + Duration::from_secs(1893427200)),
            ),
        );
        db.insert_object(
            Key::from("key_without_ex"),
            Object::new_str("value_without_ex".into(), None),
        );

        // case: with NX option
        let expire_at = ExpireAt::parse(&mut CmdUnparsed::from(
            ["key_with_ex", "1893427200", "NX"].as_ref(),
        ))
        .unwrap();
        let result = expire_at._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire_at = ExpireAt::parse(&mut CmdUnparsed::from(
            ["key_without_ex", "1893427200", "NX"].as_ref(),
        ))
        .unwrap();
        let result = expire_at._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str(
                "value_with_ex".into(),
                Some(*EPOCH + Duration::from_secs(1893427200)),
            ),
        );
        db.insert_object(
            Key::from("key_without_ex"),
            Object::new_str("value_without_ex".into(), None),
        );

        // case: with GT option
        let expire_at = ExpireAt::parse(&mut CmdUnparsed::from(
            ["key_with_ex", "1893427000", "GT"].as_ref(),
        ))
        .unwrap();
        let result = expire_at._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire_at = ExpireAt::parse(&mut CmdUnparsed::from(
            ["key_with_ex", "1893427201", "GT"].as_ref(),
        ))
        .unwrap();
        let result = expire_at._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));

        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str(
                "value_with_ex".into(),
                Some(*EPOCH + Duration::from_secs(1893427200)),
            ),
        );
        db.insert_object(
            Key::from("key_without_ex"),
            Object::new_str("value_without_ex".into(), None),
        );

        // case: with LT option
        let expire_at = ExpireAt::parse(&mut CmdUnparsed::from(
            ["key_with_ex", "1893427201", "LT"].as_ref(),
        ))
        .unwrap();
        let result = expire_at._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        let expire_at = ExpireAt::parse(&mut CmdUnparsed::from(
            ["key_with_ex", "1893427000", "LT"].as_ref(),
        ))
        .unwrap();
        let result = expire_at._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));
    }

    #[tokio::test]
    async fn expire_time_test() {
        let shared = Shared::default();
        let db = shared.db();

        db.insert_object(Key::from("key1"), Object::new_str("value1".into(), None));
        assert!(get_object(db, b"key1").unwrap().expire().is_none());
        let expire = Instant::now() + Duration::from_secs(10);
        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str("value_with_ex".into(), Some(expire)),
        );

        // case: 键存在，但没有过期时间
        let expire_time = ExpireTime::parse(&mut CmdUnparsed::from(["key1"].as_ref())).unwrap();
        let result = expire_time._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -1);

        // case: 键不存在
        let expire_time = ExpireTime::parse(&mut CmdUnparsed::from(["key_nil"].as_ref())).unwrap();
        let result = expire_time._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -2);

        // case: 键存在且有过期时间
        let expire_time =
            ExpireTime::parse(&mut CmdUnparsed::from(["key_with_ex"].as_ref())).unwrap();
        let result = expire_time._execute(&shared).await.unwrap().unwrap();
        assert_eq!(
            result,
            RESP3::Integer(expire.duration_since(*EPOCH).as_secs() as Int)
        );
    }

    #[tokio::test]
    async fn keys_test() {
        let shared = Shared::default();
        let db = shared.db();

        db.insert_object(Key::from("key1"), Object::new_str("value1".into(), None));
        db.insert_object(Key::from("key2"), Object::new_str("value2".into(), None));
        db.insert_object(Key::from("key3"), Object::new_str("value3".into(), None));
        db.insert_object(Key::from("key4"), Object::new_str("value4".into(), None));

        let keys = Keys::parse(&mut CmdUnparsed::from([".*"].as_ref())).unwrap();
        let result = keys
            ._execute(&shared)
            .await
            .unwrap()
            .unwrap()
            .try_array()
            .unwrap()
            .to_vec();
        assert!(
            result.contains(&RESP3::Bulk("key1".into()))
                && result.contains(&RESP3::Bulk("key2".into()))
                && result.contains(&RESP3::Bulk("key3".into()))
                && result.contains(&RESP3::Bulk("key4".into()))
        );

        let keys = Keys::parse(&mut CmdUnparsed::from(["key*"].as_ref())).unwrap();
        let result = keys
            ._execute(&shared)
            .await
            .unwrap()
            .unwrap()
            .try_array()
            .unwrap()
            .to_vec();
        assert!(
            result.contains(&RESP3::Bulk("key1".into()))
                && result.contains(&RESP3::Bulk("key2".into()))
                && result.contains(&RESP3::Bulk("key3".into()))
                && result.contains(&RESP3::Bulk("key4".into()))
        );

        let keys = Keys::parse(&mut CmdUnparsed::from(["key1"].as_ref())).unwrap();
        let result = keys
            ._execute(&shared)
            .await
            .unwrap()
            .unwrap()
            .try_array()
            .unwrap()
            .to_vec();
        assert!(result.contains(&RESP3::Bulk("key1".into())));
    }

    #[tokio::test]
    async fn persist_test() {
        let shared = Shared::default();
        let db = shared.db();

        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str(
                "value_with_ex".into(),
                Some(Instant::now() + Duration::from_secs(10)),
            ),
        );
        db.insert_object(
            Key::from("key_without_ex"),
            Object::new_str("value_without_ex".into(), None),
        );

        // case: 键存在，有过期时间
        let persist = Persist::parse(&mut CmdUnparsed::from(["key_with_ex"].as_ref())).unwrap();
        let result = persist._execute(&shared).await.unwrap().unwrap();
        assert_eq!(result, RESP3::Integer(1));
        assert!(get_object(db, b"key_with_ex").unwrap().expire().is_none());

        // case: 键存在，没有过期时间
        let persist = Persist::parse(&mut CmdUnparsed::from(["key_without_ex"].as_ref())).unwrap();
        let result = persist._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);

        // case: 键不存在
        let persist = Persist::parse(&mut CmdUnparsed::from(["key_nil"].as_ref())).unwrap();
        let result = persist._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == 0);
    }

    #[tokio::test]
    async fn pttl_test() {
        let shared = Shared::default();
        let db = shared.db();

        db.insert_object(Key::from("key1"), Object::new_str("value1".into(), None));
        assert!(get_object(db, b"key1").unwrap().expire().is_none());
        let dur = Duration::from_secs(10);
        let expire = Instant::now() + dur;
        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str("value_with_ex".into(), Some(expire)),
        );

        // case: 键存在，但没有过期时间
        let pttl = Pttl::parse(&mut CmdUnparsed::from(["key1"].as_ref())).unwrap();
        let result = pttl._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -1);

        // case: 键不存在
        let pttl = Pttl::parse(&mut CmdUnparsed::from(["key_nil"].as_ref())).unwrap();
        let result = pttl._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -2);

        // case: 键存在且有过期时间
        let pttl = Pttl::parse(&mut CmdUnparsed::from(["key_with_ex"].as_ref())).unwrap();
        let result = pttl
            ._execute(&shared)
            .await
            .unwrap()
            .unwrap()
            .try_integer()
            .unwrap() as u64;
        assert!(dur.as_millis() as u64 - result < ALLOWED_DELTA);
    }

    #[tokio::test]
    async fn ttl_test() {
        let shared = Shared::default();
        let db = shared.db();

        db.insert_object(Key::from("key1"), Object::new_str("value1".into(), None));
        assert!(get_object(db, b"key1").unwrap().expire().is_none());
        let dur = Duration::from_secs(10);
        let expire = Instant::now() + dur;
        db.insert_object(
            Key::from("key_with_ex"),
            Object::new_str("value_with_ex".into(), Some(expire)),
        );

        // case: 键存在，但没有过期时间
        let ttl = Ttl::parse(&mut CmdUnparsed::from(["key1"].as_ref())).unwrap();
        let result = ttl._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -1);

        // case: 键不存在
        let ttl = Ttl::parse(&mut CmdUnparsed::from(["key_nil"].as_ref())).unwrap();
        let result = ttl._execute(&shared).await.unwrap_err();
        matches!(result, CmdError::ErrorCode { code } if code == -2);

        // case: 键存在且有过期时间
        let ttl = Ttl::parse(&mut CmdUnparsed::from(["key_with_ex"].as_ref())).unwrap();
        let result = ttl
            ._execute(&shared)
            .await
            .unwrap()
            .unwrap()
            .try_integer()
            .unwrap() as u64;
        assert!(dur.as_secs() - result < ALLOWED_DELTA);
    }

    #[tokio::test]
    async fn type_test() {
        let shared = Shared::default();
        let db = shared.db();

        db.insert_object(Key::from("key1"), Object::new_str(Str::default(), None));
        db.insert_object(Key::from("key2"), Object::new_list(List::default(), None));
        db.insert_object(Key::from("key3"), Object::new_set(Set::default(), None));
        db.insert_object(Key::from("key4"), Object::new_hash(Hash::default(), None));
        db.insert_object(Key::from("key5"), Object::new_zset(ZSet::default(), None));

        // case: 键存在
        let typ = Type::parse(&mut CmdUnparsed::from(["key1"].as_ref())).unwrap();
        let result = typ
            ._execute(&shared)
            .await
            .unwrap()
            .unwrap()
            .try_simple_string()
            .unwrap()
            .to_string();
        assert_eq!(result, "string");

        let typ = Type::parse(&mut CmdUnparsed::from(["key2"].as_ref())).unwrap();
        let result = typ
            ._execute(&shared)
            .await
            .unwrap()
            .unwrap()
            .try_simple_string()
            .unwrap()
            .to_string();
        assert_eq!(result, "list");

        let typ = Type::parse(&mut CmdUnparsed::from(["key3"].as_ref())).unwrap();
        let result = typ
            ._execute(&shared)
            .await
            .unwrap()
            .unwrap()
            .try_simple_string()
            .unwrap()
            .to_string();
        assert_eq!(result, "set");

        let typ = Type::parse(&mut CmdUnparsed::from(["key4"].as_ref())).unwrap();
        let result = typ
            ._execute(&shared)
            .await
            .unwrap()
            .unwrap()
            .try_simple_string()
            .unwrap()
            .to_string();
        assert_eq!(result, "hash");

        let typ = Type::parse(&mut CmdUnparsed::from(["key5"].as_ref())).unwrap();
        let result = typ
            ._execute(&shared)
            .await
            .unwrap()
            .unwrap()
            .try_simple_string()
            .unwrap()
            .to_string();
        assert_eq!(result, "zset");
    }
}
