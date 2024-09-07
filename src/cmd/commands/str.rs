use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{AsyncStream, Handler, NEVER_EXPIRE, UNIX_EPOCH},
    shared::db::{ObjValueType, ObjectInner},
    util::atoi,
    Int, Key,
};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::Instant;
use tracing::instrument;

// 如果 key 已经存在并且是一个字符串， APPEND 命令将指定的 value 追加到该 key 原来值（value）的末尾。
/// # Reply:
///
/// **Integer reply:** the length of the string after the append operation.
#[derive(Debug)]
pub struct Append {
    pub key: Key,
    pub value: Bytes,
}

impl CmdExecutor for Append {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut length = None;

        handler
            .shared
            .db()
            .update_or_create_object(self.key, ObjValueType::Str, |obj| {
                let str = obj.on_str_mut()?;
                str.append(self.value);

                length = Some(Resp3::new_integer(str.len() as Int));
                Ok(())
            })
            .await?;

        Ok(length)
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Append {
            key: key.into(),
            value: args.next().unwrap(),
        })
    }
}

/// 将 key 中储存的数字值减一。
/// # Reply:
///
/// **Integer reply:** the value of the key after decrementing it.
#[derive(Debug)]
pub struct Decr {
    pub key: Key,
}

impl CmdExecutor for Decr {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut new_i = 0;
        handler
            .shared
            .db()
            .update_object(self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.decr_by(1)?;
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(new_i)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Decr { key: key.into() })
    }
}

/// key 所储存的值减去给定的减量值（decrement） 。
/// # Reply:
///
/// **Integer reply:** the value of the key after decrementing it.
#[derive(Debug)]
pub struct DecrBy {
    pub key: Key,
    pub decrement: Int,
}

impl CmdExecutor for DecrBy {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut new_i = 0;
        handler
            .shared
            .db()
            .update_object(self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.decr_by(self.decrement)?;
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(new_i)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(DecrBy {
            key: key.into(),
            decrement: atoi(&args.next().unwrap())
                .map_err(|_| RutinError::from("ERR decrement is not an integer"))?,
        })
    }
}

/// # Reply:
///
/// **Bulk string reply:** the value of the key.
/// **Null reply:** key does not exist.
#[derive(Debug)]
pub struct Get {
    pub key: Key,
}

impl CmdExecutor for Get {
    #[inline]
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut res = None;

        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                res = Some(Resp3::new_blob_string(obj.on_str()?.to_bytes()));
                Ok(())
            })
            .await?;

        Ok(res)
    }

    #[inline]
    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if unlikely(args.len() != 1) {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Get { key: key.into() })
    }
}

/// 返回 key 中字符串值的子字符
/// # Reply:
///
/// **Bulk string reply:** The substring of the string value stored at key, determined by the offsets start and end (both are inclusive).
#[derive(Debug)]
pub struct GetRange {
    pub key: Key,
    pub start: Int,
    pub end: Int,
}

impl CmdExecutor for GetRange {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut res = "".into();

        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                let str = obj.on_str()?;

                let mut buf = itoa::Buffer::new();
                res = Bytes::copy_from_slice(str.get_range(&mut buf, self.start, self.end));
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_blob_string(res)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 3 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let start = atoi(&args.next().unwrap()).map_err(|_| {
            RutinError::from("ERR index parameter is not a positive integer or out of range")
        })?;
        let end = atoi(&args.next().unwrap()).map_err(|_| {
            RutinError::from("ERR index parameter is not a positive integer or out of range")
        })?;

        Ok(GetRange {
            key: key.into(),
            start,
            end,
        })
    }
}

/// 将给定 key 的值设为 value ，并返回 key 的旧值(old value)。
/// # Reply:
///
/// **Bulk string reply:** the old value stored at the key.
/// **Null reply:** if the key does not exist.
#[derive(Debug)]
pub struct GetSet {
    pub key: Key,
    pub new_value: Bytes,
}

impl CmdExecutor for GetSet {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut old = "".into();

        handler
            .shared
            .db()
            .update_object(self.key, |obj| {
                let str = obj.on_str_mut()?;
                old = str.replce(self.new_value).to_bytes();
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_blob_string(old)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(GetSet {
            key: key.into(),
            new_value: args.next().unwrap(),
        })
    }
}

/// 将 key 中储存的数字值增一。
/// # Reply:
///
/// **Integer reply:** the value of the key after the increment.
#[derive(Debug)]
pub struct Incr {
    pub key: Key,
}

impl CmdExecutor for Incr {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut new_i = 0;

        handler
            .shared
            .db()
            .update_object(self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.incr_by(1)?;
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(new_i)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Incr { key: key.into() })
    }
}

/// 将 key 所储存的值加上给定的增量值（increment） 。
/// # Reply:
///
/// **Integer reply:** the value of the key after the increment.
#[derive(Debug)]
pub struct IncrBy {
    pub key: Key,
    pub increment: Int,
}

impl CmdExecutor for IncrBy {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut new_i = 0;
        handler
            .shared
            .db()
            .update_object(self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.incr_by(self.increment)?;
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(new_i)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(IncrBy {
            key: key.into(),
            increment: atoi(&args.next().unwrap())
                .map_err(|_| RutinError::from("ERR increment is not an integer"))?,
        })
    }
}

/// 获取所有(一个或多个)给定 key 的值。
/// # Reply:
///
/// **Array reply:** a list of values at the specified keys.
#[derive(Debug)]
pub struct MGet {
    pub keys: Vec<Key>,
}

impl CmdExecutor for MGet {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut res = Vec::with_capacity(self.keys.len());
        for key in self.keys.iter() {
            let mut str = "".into();

            handler
                .shared
                .db()
                .visit_object(key, |obj| {
                    str = obj.on_str()?.to_bytes();
                    Ok(())
                })
                .await?;

            res.push(Resp3::new_blob_string(str));
        }

        Ok(Some(Resp3::new_array(res)))
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

        Ok(MGet { keys })
    }
}

/// 同时设置一个或多个 key-value 对。
/// # Reply:
///
/// **Simple string reply:** always OK because MSET can't fail.
#[derive(Debug)]
pub struct MSet {
    pub pairs: Vec<(Key, Bytes)>,
}

impl CmdExecutor for MSet {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        for (key, value) in self.pairs {
            handler
                .shared
                .db()
                .insert_object(key, ObjectInner::new_str(value, *NEVER_EXPIRE))
                .await?;
        }

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 || args.len() % 2 != 0 {
            return Err(RutinError::WrongArgNum);
        }

        let mut pairs = Vec::with_capacity((args.len() - 1) / 2);

        while let (Some(key), Some(value)) = (args.next(), args.next()) {
            if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
                return Err(RutinError::NoPermission);
            }

            pairs.push((key.into(), value));
        }

        Ok(MSet { pairs })
    }
}

/// 同时设置一个或多个 key-value 对，当且仅当所有给定 key 都不存在。
/// # Reply:
///
/// **Integer reply:** 0 if no key was set (at least one key already existed).
/// **Integer reply:** 1 if all the keys were set.
#[derive(Debug)]
pub struct MSetNx {
    pub pairs: Vec<(Key, Bytes)>,
}

impl CmdExecutor for MSetNx {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        for (key, _) in &self.pairs {
            if handler.shared.db().contains_object(key).await {
                return Err(0.into());
            }
        }

        for (key, value) in self.pairs {
            handler
                .shared
                .db()
                .insert_object(key, ObjectInner::new_str(value, *NEVER_EXPIRE))
                .await?;
        }

        Ok(Some(Resp3::new_integer(1)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 || args.len() % 2 != 0 {
            return Err(RutinError::WrongArgNum);
        }

        let mut pairs = Vec::with_capacity((args.len() - 1) / 2);

        while let (Some(key), Some(value)) = (args.next(), args.next()) {
            if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
                return Err(RutinError::NoPermission);
            }

            pairs.push((key.into(), value));
        }

        Ok(MSetNx { pairs })
    }
}

/// # Reply:
///
/// **Null reply:** GET not given: Operation was aborted (conflict with one of the XX/NX options).
/// **Simple string reply:** OK. GET not given: The key was set.
/// **Null reply:** GET given: The key didn't exist before the SET.
/// **Bulk string reply:** GET given: The previous value of the key.
#[derive(Debug)]
pub struct Set {
    key: Key,
    value: Bytes,
    opt: Option<SetOpt>,
    get: bool,
    expire: Option<Instant>, // None代表无要求，Some(EPOCH)代表保持原expire
}

#[derive(Debug)]
enum SetOpt {
    /// Only set the key if it does not already exist.
    NX,
    /// Only set the key if it already exist.
    XX,
}

impl CmdExecutor for Set {
    #[inline]
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        // 1. 是否要求键存在？
        // 2. 满足命令对键的要求后，更新值
        // 3. 是否需要更新expire?

        let mut key_flag = match self.opt {
            Some(SetOpt::NX) => Some(false),
            Some(SetOpt::XX) => Some(true),
            _ => None,
        };
        if self.get {
            key_flag = Some(true);
        }
        if let Some(ex) = self.expire {
            if ex == *UNIX_EPOCH {
                key_flag = Some(true);
            }
        }

        let entry = handler.shared.db().get_mut(self.key).await?;
        if let Some(flag) = key_flag {
            if flag != entry.is_object_existed() {
                return Err(RutinError::Null);
            }
        }

        let new_ex = if let Some(ex) = self.expire {
            if ex.duration_since(*UNIX_EPOCH) < Duration::from_millis(10) {
                // 保持不变
                entry.value().unwrap().expire_unchecked()
            } else {
                // 更新
                ex
            }
        } else {
            // 永不过期
            *NEVER_EXPIRE
        };

        let new_obj = ObjectInner::new_str(self.value, new_ex);
        let (_, old) = entry.insert_object(new_obj);

        if self.get {
            Ok(Some(Resp3::new_blob_string(
                old.unwrap().on_str()?.to_bytes(),
            )))
        } else {
            Ok(Some(Resp3::new_simple_string("OK".into())))
        }
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if unlikely(args.len() < 2) {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }
        let key = key.into();

        let value = args.next().unwrap();

        let mut buf = [0; 7];
        let mut next = args.get_uppercase(0, &mut buf);
        let _ = args.advance_by(1);
        let opt = match next {
            None => {
                // 已经没有参数了
                return Ok(Set {
                    key,
                    value,
                    opt: None,
                    get: false,
                    expire: None,
                });
            }
            Some(opt) => {
                match opt {
                    b"NX" => {
                        next = args.get_uppercase(0, &mut buf);
                        let _ = args.advance_by(1);
                        Some(SetOpt::NX)
                    }
                    b"XX" => {
                        next = args.get_uppercase(0, &mut buf);
                        let _ = args.advance_by(1);
                        Some(SetOpt::XX)
                    }
                    // 该参数不是设置选项的参数
                    _ => None,
                }
            }
        };

        let get = match next {
            None => {
                // 已经没有参数了
                return Ok(Set {
                    key,
                    value,
                    opt,
                    get: false,
                    expire: None,
                });
            }
            Some(get) => {
                match get {
                    b"GET" => {
                        next = args.get_uppercase(0, &mut buf);
                        let _ = args.advance_by(1);
                        true
                    }
                    // 该参数不是设置GET的参数
                    _ => false,
                }
            }
        };

        let expire = match next {
            None => {
                // 已经没有参数了
                return Ok(Set {
                    key,
                    value,
                    opt,
                    get,
                    expire: None,
                });
            }
            Some(ex) => {
                match ex {
                    b"KEEPTTL" => Some(*UNIX_EPOCH),
                    b"EX" => {
                        let expire_value = args.next().ok_or(RutinError::WrongArgNum)?;
                        Some(Instant::now() + Duration::from_secs(atoi(&expire_value)?))
                    }
                    // PX milliseconds -- 以毫秒为单位设置键的过期时间
                    b"PX" => {
                        let expire_value = args.next().ok_or(RutinError::WrongArgNum)?;
                        Some(Instant::now() + Duration::from_millis(atoi(&expire_value)?))
                    }
                    // EXAT timestamp -- timestamp是以秒为单位的Unix时间戳
                    b"EXAT" => {
                        let expire_value = args.next().ok_or(RutinError::WrongArgNum)?;
                        Some(*UNIX_EPOCH + Duration::from_secs(atoi(&expire_value)?))
                    }
                    // PXAT timestamp -- timestamp是以毫秒为单位的Unix时间戳
                    b"PXAT" => {
                        let expire_value = args.next().ok_or(RutinError::WrongArgNum)?;
                        Some(*UNIX_EPOCH + Duration::from_millis(atoi(&expire_value)?))
                    }
                    _ => return Err(RutinError::Syntax),
                }
            }
        };

        // 如果还有多余的参数，说明参数数目不对
        if !args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }
        Ok(Set {
            key,
            value,
            opt,
            get,
            expire,
        })
    }
}

/// 将值 value 关联到 key ，并将 key 的过期时间设为 seconds (以秒为单位)。
/// # Reply:
///
/// **Simple string reply:** OK.
#[derive(Debug)]
pub struct SetEx {
    pub key: Key,
    pub expire: Duration,
    pub value: Bytes,
}

impl CmdExecutor for SetEx {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        handler
            .shared
            .db()
            .insert_object(
                self.key,
                ObjectInner::new_str(self.value, Instant::now() + self.expire),
            )
            .await?;

        Ok(Some(Resp3::new_simple_string("OK".into())))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 3 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let expire = Duration::from_secs(atoi(&args.next().unwrap())?);
        let value = args.next().unwrap();

        Ok(SetEx {
            key: key.into(),
            value,
            expire,
        })
    }
}

/// 只有在 key 不存在时设置 key 的值。
/// # Reply:
///
/// **Integer reply:** 0 if the key was not set.
/// **Integer reply:** 1 if the key was set.
#[derive(Debug)]
pub struct SetNx {
    pub key: Key,
    pub value: Bytes,
}

impl CmdExecutor for SetNx {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        if handler.shared.db().contains_object(&self.key).await {
            return Err(0.into());
        }

        handler
            .shared
            .db()
            .insert_object(self.key, ObjectInner::new_str(self.value, *NEVER_EXPIRE))
            .await?;

        Ok(Some(Resp3::new_integer(1)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(SetNx {
            key: key.into(),
            value: args.next().unwrap(),
        })
    }
}

/// 返回 key 所储存的字符串值的长度。
/// # Reply:
///
/// **Integer reply:** the length of the string stored at key, or 0 when the key does not exist.
#[derive(Debug)]
pub struct StrLen {
    pub key: Key,
}

impl CmdExecutor for StrLen {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut len = 0;
        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                len = obj.on_str()?.len();
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(len as Int)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(StrLen { key: key.into() })
    }
}

#[cfg(test)]
mod cmd_str_tests {
    use super::*;
    use crate::util::test_init;
    use std::{
        thread::sleep,
        time::{Duration, SystemTime},
    };

    #[tokio::test]
    async fn get_and_set_test() {
        test_init();
        let (mut handler, _) = Handler::new_fake();

        /************************************/
        /* 测试简单的无过期时间的键值对存取 */
        /************************************/
        let set = Set::parse(
            ["key_never_expire", "value_never_expire"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            set.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(
            ["key_never_expire"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();

        assert_eq!(
            get.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_blob_string()
                .unwrap(),
            b"value_never_expire".as_ref()
        );

        /******************************/
        /* 测试带有NX和XX的键值对存取 */
        /******************************/
        let set = Set::parse(
            ["key_nx", "value_nx", "NX"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            set.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(["key_nx"].as_ref().into(), &AccessControl::new_loose()).unwrap();
        assert_eq!(
            get.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_blob_string()
                .unwrap(),
            b"value_nx".as_ref()
        );

        let set = Set::parse(
            ["key_nx", "value_nx", "NX"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert!(matches!(
            set.execute(&mut handler).await.unwrap_err(),
            RutinError::Null
        ));

        let set = Set::parse(
            ["key_xx", "value_xx", "XX"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert!(matches!(
            set.execute(&mut handler).await.unwrap_err(),
            RutinError::Null
        ));

        let set = Set::parse(
            ["key_nx", "value_xx", "XX"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            set.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(["key_nx"].as_ref().into(), &AccessControl::new_loose()).unwrap();
        assert_eq!(
            get.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_blob_string()
                .unwrap(),
            "value_xx".as_bytes()
        );

        /******************************/
        /* 测试带有GET的键值对存取 */
        /******************************/
        let set = Set::parse(
            ["key_never_expire", "value_never_expire", "GET"]
                .as_ref()
                .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            set.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_blob_string()
                .unwrap(),
            "value_never_expire".as_bytes()
        );

        let set = Set::parse(
            ["key_never_exist", "value_never_exist", "GET"]
                .as_ref()
                .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert!(matches!(
            set.execute(&mut handler).await.unwrap_err(),
            RutinError::Null
        ));

        /**********************************/
        /* 测试带有EX过期时间的键值对存取 */
        /**********************************/
        let set = Set::parse(
            ["key_expire", "value_expire", "ex", "1"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            set.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(["key_expire"].as_ref().into(), &AccessControl::new_loose()).unwrap();
        assert_eq!(
            get.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_blob_string()
                .unwrap(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_secs(1));
        let get = Get::parse(["key_expire"].as_ref().into(), &AccessControl::new_loose()).unwrap();
        assert!(matches!(
            get.execute(&mut handler).await,
            Err(RutinError::Null)
        ));

        /**********************************/
        /* 测试带有PX过期时间的键值对存取 */
        /**********************************/
        let set = Set::parse(
            ["key_expire", "value_expire", "PX", "500"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            set.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(["key_expire"].as_ref().into(), &AccessControl::new_loose()).unwrap();
        assert_eq!(
            get.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_blob_string()
                .unwrap(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_millis(500));
        let get = Get::parse(["key_expire"].as_ref().into(), &AccessControl::new_loose()).unwrap();
        assert!(matches!(
            get.execute(&mut handler).await,
            Err(RutinError::Null)
        ));

        /************************************/
        /* 测试带有EXAT过期时间的键值对存取 */
        /************************************/
        let exat = SystemTime::now() + Duration::from_millis(1000);
        let exat = exat
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let set = Set::parse(
            [
                "key_expire",
                "value_expire",
                "EXAT",
                exat.to_string().as_str(),
            ]
            .as_ref()
            .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            set.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(["key_expire"].as_ref().into(), &AccessControl::new_loose()).unwrap();
        assert_eq!(
            get.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_blob_string()
                .unwrap(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_millis(1000));
        let get = Get::parse(["key_expire"].as_ref().into(), &AccessControl::new_loose()).unwrap();
        assert!(matches!(
            get.execute(&mut handler).await,
            Err(RutinError::Null)
        ));

        /************************************/
        /* 测试带有PXAT过期时间的键值对存取 */
        /************************************/
        let exat = SystemTime::now() + Duration::from_millis(500);
        let exat = exat
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let set = Set::parse(
            [
                "key_expire",
                "value_expire",
                "PXAT",
                exat.to_string().as_str(),
            ]
            .as_ref()
            .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            set.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(["key_expire"].as_ref().into(), &AccessControl::new_loose()).unwrap();
        assert_eq!(
            get.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_blob_string()
                .unwrap(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_millis(500));
        let get = Get::parse(["key_expire"].as_ref().into(), &AccessControl::new_loose()).unwrap();
        assert!(matches!(
            get.execute(&mut handler).await,
            Err(RutinError::Null)
        ));

        /***************/
        /* 测试KEEPTTL */
        /***************/
        let now = Instant::now();
        let set = Set::parse(
            ["key_expire", "value_expire", "PX", "100000"]
                .as_ref()
                .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            set.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let set = Set::parse(
            ["key_expire", "value_expire_modified", "KEEPTTL"]
                .as_ref()
                .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            set.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let obj = handler.shared.db().get(&"key_expire".into()).await.unwrap();
        assert_eq!(
            obj.on_str().unwrap().unwrap().to_bytes().as_ref(),
            b"value_expire_modified"
        );
        assert!(
            // 误差在10ms以内
            (obj.value().inner().unwrap().expire_unchecked() - now) - Duration::from_millis(100000)
                < Duration::from_millis(10)
        );
    }
}
