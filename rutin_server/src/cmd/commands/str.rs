use super::*;
use crate::{
    Int,
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{AsyncStream, Handler, NEVER_EXPIRE, UNIX_EPOCH},
    shared::db::{Object, Str},
    util::atoi,
};
use bytes::Bytes;
use itertools::Itertools;
use std::{fmt::Debug, slice, time::Duration};
use tokio::time::Instant;
use tracing::instrument;

// 如果 key 已经存在并且是一个字符串， APPEND 命令将指定的 value 追加到该 key 原来值（value）的末尾。
/// # Reply:
///
/// **Integer reply:** the length of the string after the append operation.
#[derive(Debug)]
pub struct Append<A> {
    pub key: A,
    pub value: A,
}

impl<A> CmdExecutor<A> for Append<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut length = None;

        handler
            .shared
            .db()
            .update_object_force(
                &self.key,
                || Str::default().into(),
                |obj| {
                    let str = obj.on_str_mut()?;
                    str.append(self.value.as_ref());

                    length = Some(Resp3::new_integer(str.len() as Int));
                    Ok(())
                },
            )
            .await?;

        Ok(length)
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Append {
            key,
            value: args.next().unwrap(),
        })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// 将 key 中储存的数字值减一。
/// # Reply:
///
/// **Integer reply:** the value of the key after decrementing it.
#[derive(Debug)]
pub struct Decr<A> {
    pub key: A,
}

impl<A> CmdExecutor<A> for Decr<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut new_i = 0;
        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.decr_by(1)?;
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(new_i)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Decr { key })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// key 所储存的值减去给定的减量值（decrement） 。
/// # Reply:
///
/// **Integer reply:** the value of the key after decrementing it.
#[derive(Debug)]
pub struct DecrBy<A> {
    pub key: A,
    pub decrement: Int,
}

impl<A> CmdExecutor<A> for DecrBy<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut new_i = 0;
        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.decr_by(self.decrement)?;
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(new_i)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(DecrBy {
            key,
            decrement: atoi(args.next().unwrap())
                .map_err(|_| RutinError::from("ERR decrement is not an integer"))?,
        })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// # Reply:
///
/// **Bulk string reply:** the value of the key.
/// **Null reply:** key does not exist.
#[derive(Debug)]
pub struct Get<A> {
    pub key: A,
}

impl<A> CmdExecutor<A> for Get<A>
where
    A: CmdArg,
{
    #[inline]
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut res = None;

        handler
            .shared
            .db()
            .visit_object(self.key.as_ref(), |obj| {
                res = Some(Resp3::new_blob_string(obj.on_str()?.to_bytes()));
                Ok(())
            })
            .await?;

        Ok(res)
    }

    #[inline]
    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if unlikely(args.len() != 1) {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Get { key })
    }

    #[inline]
    async fn may_track(&self, handler: &mut Handler<impl AsyncStream>) -> bool {
        if let Some(client_track) = &mut handler.context.client_track {
            client_track.keys.push(self.key.as_ref().into());
            true
        } else {
            false
        }
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// 返回 key 中字符串值的子字符
/// # Reply:
///
/// **Bulk string reply:** The substring of the string value stored at key, determined by the offsets start and end (both are inclusive).
#[derive(Debug)]
pub struct GetRange<A> {
    pub key: A,
    pub start: Int,
    pub end: Int,
}

impl<A> CmdExecutor<A> for GetRange<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut res = "".into();

        handler
            .shared
            .db()
            .visit_object(self.key.as_ref(), |obj| {
                let str = obj.on_str()?;

                let mut buf = itoa::Buffer::new();
                res = Bytes::copy_from_slice(str.get_range(&mut buf, self.start, self.end));
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_blob_string(res)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 3 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let start = atoi(args.next().unwrap()).map_err(|_| {
            RutinError::from("ERR index parameter is not a positive integer or out of range")
        })?;
        let end = atoi(args.next().unwrap()).map_err(|_| {
            RutinError::from("ERR index parameter is not a positive integer or out of range")
        })?;

        Ok(GetRange { key, start, end })
    }

    #[inline]
    async fn may_track(&self, handler: &mut Handler<impl AsyncStream>) -> bool {
        if let Some(client_track) = &mut handler.context.client_track {
            client_track.keys.push(self.key.as_ref().into());
            true
        } else {
            false
        }
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// 将给定 key 的值设为 value ，并返回 key 的旧值(old value)。
/// # Reply:
///
/// **Bulk string reply:** the old value stored at the key.
/// **Null reply:** if the key does not exist.
#[derive(Debug)]
pub struct GetSet<A> {
    pub key: A,
    pub new_value: Str,
}

impl<A> CmdExecutor<A> for GetSet<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut old = "";

        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                let str = obj.on_str_mut()?;
                old = str.set(self.new_value).into();
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_blob_string(old)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(GetSet {
            key,
            new_value: args.next().unwrap().into(),
        })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// 将 key 中储存的数字值增一。
/// # Reply:
///
/// **Integer reply:** the value of the key after the increment.
#[derive(Debug)]
pub struct Incr<A> {
    pub key: A,
}

impl<A> CmdExecutor<A> for Incr<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut new_i = 0;

        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.incr_by(1)?;
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(new_i)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Incr { key })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// 将 key 所储存的值加上给定的增量值（increment） 。
/// # Reply:
///
/// **Integer reply:** the value of the key after the increment.
#[derive(Debug)]
pub struct IncrBy<A> {
    pub key: A,
    pub increment: Int,
}

impl<A> CmdExecutor<A> for IncrBy<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut new_i = 0;
        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.incr_by(self.increment)?;
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(new_i)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(IncrBy {
            key,
            increment: atoi(args.next().unwrap())
                .map_err(|_| RutinError::from("ERR increment is not an integer"))?,
        })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// 获取所有(一个或多个)给定 key 的值。
/// # Reply:
///
/// **Array reply:** a list of values at the specified keys.
#[derive(Debug)]
pub struct MGet<A> {
    pub keys: Vec<A>,
}

impl<A> CmdExecutor<A> for MGet<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut res = Vec::with_capacity(self.keys.len());
        for key in self.keys.iter() {
            let mut str = "".into();

            handler
                .shared
                .db()
                .visit_object(key.as_ref(), |obj| {
                    str = obj.on_str()?.to_bytes();
                    Ok(())
                })
                .await?;

            res.push(Resp3::new_blob_string(str));
        }

        Ok(Some(Resp3::new_array(res)))
    }

    fn parse(args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
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

        Ok(MGet { keys })
    }

    #[inline]
    async fn may_track(&self, handler: &mut Handler<impl AsyncStream>) -> bool {
        if let Some(client_track) = &mut handler.context.client_track {
            client_track
                .keys
                .extend(self.keys.iter().map(|b| b.as_ref().into()));
            true
        } else {
            false
        }
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(self.keys.iter())
    }
}

/// 同时设置一个或多个 key-value 对。
/// # Reply:
///
/// **Simple string reply:** always OK because MSET can't fail.
#[derive(Debug)]
pub struct MSet<A> {
    pub pairs: Vec<(A, Str)>,
}

impl<A> CmdExecutor<A> for MSet<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let db = handler.shared.db();

        for (key, value) in self.pairs {
            db.insert_object(&key, value.into()).await?;
        }

        Ok(Some(Resp3::new_simple_string("OK")))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 || args.len() % 2 != 0 {
            return Err(RutinError::WrongArgNum);
        }

        let mut pairs = Vec::with_capacity((args.len() - 1) / 2);

        while let (Some(key), Some(value)) = (args.next(), args.next()) {
            if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
                return Err(RutinError::NoPermission);
            }

            pairs.push((key, value.into()));
        }

        Ok(MSet { pairs })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(self.pairs.iter().map(|(k, _)| k))
    }
}

/// 同时设置一个或多个 key-value 对，当且仅当所有给定 key 都不存在。
/// # Reply:
///
/// **Integer reply:** 0 if no key was set (at least one key already existed).
/// **Integer reply:** 1 if all the keys were set.
#[derive(Debug)]
pub struct MSetNx<A> {
    pub pairs: Vec<(A, Str)>,
}

impl<A> CmdExecutor<A> for MSetNx<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let db = handler.shared.db();

        for (key, _) in &self.pairs {
            if db.contains_object(key.as_ref()).await {
                return Err(0.into());
            }
        }

        for (key, value) in self.pairs {
            db.insert_object(&key, Object::with_expire(value, *NEVER_EXPIRE))
                .await?;
        }

        Ok(Some(Resp3::new_integer(1)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 || args.len() % 2 != 0 {
            return Err(RutinError::WrongArgNum);
        }

        let mut pairs = Vec::with_capacity((args.len() - 1) / 2);

        while let (Some(key), Some(value)) = (args.next(), args.next()) {
            if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
                return Err(RutinError::NoPermission);
            }

            pairs.push((key, value.into()));
        }

        Ok(MSetNx { pairs })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(self.pairs.iter().map(|(k, _)| k))
    }
}

/// # Reply:
///
/// **Null reply:** GET not given: Operation was aborted (conflict with one of the XX/NX options).
/// **Simple string reply:** OK. GET not given: The key was set.
/// **Null reply:** GET given: The key didn't exist before the SET.
/// **Bulk string reply:** GET given: The previous value of the key.
#[derive(Debug)]
pub struct Set<A> {
    key: A,
    value: A,
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

impl<A> CmdExecutor<A> for Set<A>
where
    A: CmdArg,
{
    #[inline]
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let db = handler.shared.db();

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

        let entry = db.object_entry(&self.key).await?;
        if let Some(flag) = key_flag
            && flag != entry.is_occupied()
        {
            // 不满足键的要求
            return Err(RutinError::Null);
        }

        let new_ex = if let Some(ex) = self.expire {
            if ex.duration_since(*UNIX_EPOCH) < Duration::from_millis(10) {
                // 保持不变
                entry.expire().unwrap()
            } else {
                // 更新
                ex
            }
        } else {
            // 永不过期
            *NEVER_EXPIRE
        };

        let new_obj = Object::with_expire(
            {
                let s: Str = self.value.into();
                s
            },
            new_ex,
        );

        let (_, old) = entry.insert2(new_obj);

        if self.get {
            Ok(Some(Resp3::new_blob_string(
                old.expect("old object must exist when 'get' option is set")
                    .value
                    .on_str()?
                    .to_bytes(),
            )))
        } else {
            Ok(Some(Resp3::new_simple_string("OK")))
        }
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if unlikely(args.len() < 2) {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }
        let value = args.next().unwrap();

        let mut next = args.next_uppercase::<8>();
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
            Some(ref opt) => {
                match opt as &[u8] {
                    b"NX" => {
                        next = args.next_uppercase::<8>();
                        Some(SetOpt::NX)
                    }
                    b"XX" => {
                        next = args.next_uppercase::<8>();
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
            Some(ref get) => {
                match get as &[u8] {
                    b"GET" => {
                        next = args.next_uppercase::<8>();
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
            Some(ref ex) => {
                match ex as &[u8] {
                    b"KEEPTTL" => Some(*UNIX_EPOCH),
                    b"EX" => {
                        let expire_value = args.next().ok_or(RutinError::WrongArgNum)?;
                        Some(Instant::now() + Duration::from_secs(atoi(expire_value)?))
                    }
                    // PX milliseconds -- 以毫秒为单位设置键的过期时间
                    b"PX" => {
                        let expire_value = args.next().ok_or(RutinError::WrongArgNum)?;
                        Some(Instant::now() + Duration::from_millis(atoi(expire_value)?))
                    }
                    // EXAT timestamp -- timestamp是以秒为单位的Unix时间戳
                    b"EXAT" => {
                        let expire_value = args.next().ok_or(RutinError::WrongArgNum)?;
                        Some(*UNIX_EPOCH + Duration::from_secs(atoi(expire_value)?))
                    }
                    // PXAT timestamp -- timestamp是以毫秒为单位的Unix时间戳
                    b"PXAT" => {
                        let expire_value = args.next().ok_or(RutinError::WrongArgNum)?;
                        Some(*UNIX_EPOCH + Duration::from_millis(atoi(expire_value)?))
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

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// 将值 value 关联到 key ，并将 key 的过期时间设为 seconds (以秒为单位)。
/// # Reply:
///
/// **Simple string reply:** OK.
#[derive(Debug)]
pub struct SetEx<A> {
    pub key: A,
    pub expire: Duration,
    pub value: Str,
}

impl<A> CmdExecutor<A> for SetEx<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        handler
            .shared
            .db()
            .insert_object(
                &self.key,
                Object::with_expire(self.value, Instant::now() + self.expire),
            )
            .await?;

        Ok(Some(Resp3::new_simple_string("OK")))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 3 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let expire = Duration::from_secs(atoi(args.next().unwrap())?);
        let value = args.next().unwrap().into();

        Ok(SetEx { key, value, expire })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// 只有在 key 不存在时设置 key 的值。
/// # Reply:
///
/// **Integer reply:** 0 if the key was not set.
/// **Integer reply:** 1 if the key was set.
#[derive(Debug)]
pub struct SetNx<A> {
    pub key: A,
    pub value: A,
}

impl<A> CmdExecutor<A> for SetNx<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let db = handler.shared.db();

        let entry = db.object_entry(&self.key).await?;
        if entry.is_occupied() {
            return Err(0.into());
        }

        entry.insert1(Object::with_expire(
            {
                let s: Str = self.value.into();
                s
            },
            *NEVER_EXPIRE,
        ));

        Ok(Some(Resp3::new_integer(1)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(SetNx {
            key,
            value: args.next().unwrap(),
        })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

/// 返回 key 所储存的字符串值的长度。
/// # Reply:
///
/// **Integer reply:** the length of the string stored at key, or 0 when the key does not exist.
#[derive(Debug)]
pub struct StrLen<A> {
    pub key: A,
}

impl<A> CmdExecutor<A> for StrLen<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut len = 0;
        handler
            .shared
            .db()
            .visit_object(self.key.as_ref(), |obj| {
                len = obj.on_str()?.len();
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(len as Int)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(StrLen { key })
    }

    fn keys<'a>(&'a self) -> Option<impl Iterator<Item = &'a A>>
    where
        A: 'a,
    {
        Some(slice::from_ref(&self.key).iter())
    }
}

#[cfg(test)]
mod cmd_str_tests {
    use tokio::time::sleep;

    use super::*;
    use crate::util::{gen_test_handler, test_init};
    use std::time::{Duration, SystemTime};

    #[tokio::test]
    async fn get_and_set_test() {
        test_init();
        let mut handler = gen_test_handler();

        handler
            .shared
            .db()
            .object_entry(&Key::from("key".as_bytes()))
            .await
            .unwrap()
            .insert2(Object::with_expire(Str::from("value"), *NEVER_EXPIRE));

        assert!(
            handler
                .shared
                .db()
                .contains_object(&Key::from("key".as_bytes()))
                .await
        );

        // 测试简单的无过期时间的键值对存取
        let set_res = Set::test(&["key_never_expire", "value_never_expire"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(set_res.into_simple_string_unchecked(), "OK");

        let get_res = Get::test(&["key_never_expire"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            get_res.into_blob_string_unchecked(),
            b"value_never_expire".as_ref()
        );

        // 测试带有NX和XX的键值对存取
        let set_res = Set::test(&["key_nx", "value_nx", "NX"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(set_res.into_simple_string_unchecked(), "OK");

        let get_res = Get::test(&["key_nx"], &mut handler).await.unwrap().unwrap();
        assert_eq!(get_res.into_blob_string_unchecked(), b"value_nx".as_ref());

        let set_res = Set::test(&["key_nx", "value_nx", "NX"], &mut handler)
            .await
            .unwrap_err();
        assert!(matches!(set_res, RutinError::Null));

        let set_res = Set::test(&["key_xx", "value_xx", "XX"], &mut handler)
            .await
            .unwrap_err();
        assert!(matches!(set_res, RutinError::Null));

        let set_res = Set::test(&["key_nx", "value_xx", "XX"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(set_res.into_simple_string_unchecked(), "OK");

        let get_res = Get::test(&["key_nx"], &mut handler).await.unwrap().unwrap();
        assert_eq!(get_res.into_blob_string_unchecked(), "value_xx".as_bytes());

        // 测试带有GET的键值对存取
        let set_res = Set::test(
            &["key_never_expire", "value_never_expire", "GET"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            set_res.into_blob_string_unchecked(),
            "value_never_expire".as_bytes()
        );

        let set_res = Set::test(
            &["key_never_exist", "value_never_exist", "GET"],
            &mut handler,
        )
        .await
        .unwrap_err();
        assert!(matches!(set_res, RutinError::Null));

        // 测试带有EX过期时间的键值对存取
        let set_res = Set::test(&["key_expire", "value_expire", "EX", "1"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(set_res.into_simple_string_unchecked(), "OK");

        let get_res = Get::test(&["key_expire"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            get_res.into_blob_string_unchecked(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_secs(1)).await;
        let get_res = Get::test(&["key_expire"], &mut handler).await.unwrap_err();
        assert!(matches!(get_res, RutinError::Null));

        // 测试带有PX过期时间的键值对存取
        let set_res = Set::test(&["key_expire", "value_expire", "PX", "500"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(set_res.into_simple_string_unchecked(), "OK");

        let get_res = Get::test(&["key_expire"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            get_res.into_blob_string_unchecked(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_millis(500)).await;
        let get_res = Get::test(&["key_expire"], &mut handler).await.unwrap_err();
        assert!(matches!(get_res, RutinError::Null));

        // 测试带有EXAT过期时间的键值对存取
        let exat = (SystemTime::now() + Duration::from_secs(1))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let set_res = Set::test(
            &["key_expire", "value_expire", "EXAT", &exat.to_string()],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(set_res.into_simple_string_unchecked(), "OK");

        let get_res = Get::test(&["key_expire"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            get_res.into_blob_string_unchecked(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_millis(1000)).await;
        let get_res = Get::test(&["key_expire"], &mut handler).await.unwrap_err();
        assert!(matches!(get_res, RutinError::Null));

        // 测试带有PXAT过期时间的键值对存取
        let pxat = (SystemTime::now() + Duration::from_millis(500))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let set_res = Set::test(
            &["key_expire", "value_expire", "PXAT", &pxat.to_string()],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(set_res.into_simple_string_unchecked(), "OK");

        let get_res = Get::test(&["key_expire"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            get_res.into_blob_string_unchecked(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_millis(500)).await;
        let get_res = Get::test(&["key_expire"], &mut handler).await.unwrap_err();
        assert!(matches!(get_res, RutinError::Null));

        // 测试 KEEPTTL
        let now = Instant::now();
        let set_res = Set::test(
            &["key_expire", "value_expire", "PX", "100000"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(set_res.into_simple_string_unchecked(), "OK");

        let set_res = Set::test(
            &["key_expire", "value_expire_modified", "KEEPTTL"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(set_res.into_simple_string_unchecked(), "OK");

        let obj = handler
            .shared
            .db()
            .entries
            .get("key_expire".as_bytes())
            .unwrap();
        assert_eq!(
            obj.value.on_str().unwrap().to_bytes().as_ref(),
            b"value_expire_modified"
        );
        assert!(
            (obj.value().expire - now) - Duration::from_millis(100000) < Duration::from_millis(10)
        );
    }
}
