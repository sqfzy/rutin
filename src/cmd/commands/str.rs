use crate::{
    cmd::{
        error::{CmdError, Err},
        CmdExecutor, CmdType, CmdUnparsed,
    },
    frame::RESP3,
    shared::{
        db::{ObjValueType, ObjectInner},
        Shared,
    },
    util::{atoi, epoch},
    Int, Key,
};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::Instant;

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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut length = None;

        shared
            .db()
            .update_or_create_object(&self.key, ObjValueType::Str, |obj| {
                let str = obj.on_str_mut()?;
                str.append(self.value);

                length = Some(RESP3::Integer(str.len() as Int));
                Ok(())
            })
            .await?;

        Ok(length)
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }
        Ok(Append {
            key: args.next().unwrap(),
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut new_i = 0;
        shared
            .db()
            .update_object(&self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.decr_by(1)?;
                Ok(())
            })
            .await?;

        Ok(Some(RESP3::Integer(new_i)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }
        Ok(Decr {
            key: args.next().unwrap(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut new_i = 0;
        shared
            .db()
            .update_object(&self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.decr_by(self.decrement)?;
                Ok(())
            })
            .await?;

        Ok(Some(RESP3::Integer(new_i)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(DecrBy {
            key: args.next().unwrap(),
            decrement: atoi(&args.next().unwrap())
                .map_err(|_| CmdError::from("ERR decrement is not an integer"))?,
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
    const CMD_TYPE: crate::cmd::CmdType = CmdType::Read;

    #[inline]
    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut res = None;

        shared
            .db()
            .visit_object(&self.key, |obj| {
                res = Some(RESP3::Bulk(obj.on_str()?.to_bytes()));
                Ok(())
            })
            .await?;

        Ok(res)
    }

    #[inline]
    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Get {
            key: args.next().unwrap(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Read;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut res = "".into();

        shared
            .db()
            .visit_object(&self.key, |obj| {
                let str = obj.on_str()?;

                let mut buf = itoa::Buffer::new();
                res = Bytes::copy_from_slice(str.get_range(&mut buf, self.start, self.end));
                Ok(())
            })
            .await?;

        Ok(Some(RESP3::Bulk(res)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 3 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        let start = atoi(&args.next().unwrap()).map_err(|_| {
            CmdError::from("ERR index parameter is not a positive integer or out of range")
        })?;
        let end = atoi(&args.next().unwrap()).map_err(|_| {
            CmdError::from("ERR index parameter is not a positive integer or out of range")
        })?;

        Ok(GetRange { key, start, end })
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut old = "".into();

        shared
            .db()
            .update_object(&self.key, |obj| {
                let str = obj.on_str_mut()?;
                old = str.set(self.new_value).to_bytes();
                Ok(())
            })
            .await?;

        Ok(Some(RESP3::Bulk(old)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(GetSet {
            key: args.next().unwrap(),
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut new_i = 0;

        shared
            .db()
            .update_object(&self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.incr_by(1)?;
                Ok(())
            })
            .await?;

        Ok(Some(RESP3::Integer(new_i)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Incr {
            key: args.next().unwrap(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut new_i = 0;
        shared
            .db()
            .update_object(&self.key, |obj| {
                let str = obj.on_str_mut()?;
                new_i = str.incr_by(self.increment)?;
                Ok(())
            })
            .await?;

        Ok(Some(RESP3::Integer(new_i)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(IncrBy {
            key: args.next().unwrap(),
            increment: atoi(&args.next().unwrap())
                .map_err(|_| CmdError::from("ERR increment is not an integer"))?,
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
    const CMD_TYPE: CmdType = CmdType::Read;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut res = Vec::with_capacity(self.keys.len());
        for key in self.keys.iter() {
            let mut str = "".into();

            shared
                .db()
                .visit_object(key, |obj| {
                    str = obj.on_str()?.to_bytes();
                    Ok(())
                })
                .await?;

            res.push(RESP3::Bulk(str));
        }

        Ok(Some(RESP3::Array(res)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.is_empty() {
            return Err(Err::WrongArgNum.into());
        }

        Ok(MGet {
            keys: args.collect(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        for (key, value) in self.pairs {
            shared
                .db()
                .insert_object(key, ObjectInner::new_str(value.into(), None))
                .await;
        }

        Ok(Some(RESP3::SimpleString("OK".into())))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() < 2 || args.len() % 2 != 0 {
            return Err(Err::WrongArgNum.into());
        }

        let mut pairs = Vec::with_capacity((args.len() - 1) / 2);

        while let (Some(key), Some(value)) = (args.next(), args.next()) {
            pairs.push((key, value));
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        for (key, _) in &self.pairs {
            if shared.db().contains_object(key).await {
                return Err(0.into());
            }
        }

        for (key, value) in self.pairs {
            shared
                .db()
                .insert_object(key, ObjectInner::new_str(value.into(), None))
                .await;
        }

        Ok(Some(RESP3::Integer(1)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() < 2 || args.len() % 2 != 0 {
            return Err(Err::WrongArgNum.into());
        }

        let mut pairs = Vec::with_capacity((args.len() - 1) / 2);

        while let (Some(key), Some(value)) = (args.next(), args.next()) {
            pairs.push((key, value));
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
    const CMD_TYPE: CmdType = CmdType::Write;

    #[inline]
    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
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
            if ex == epoch() {
                key_flag = Some(true);
            }
        }

        let entry = shared.db().get_object_entry_mut(self.key).await;
        if let Some(flag) = key_flag {
            if flag != entry.is_object_existed() {
                return Err(CmdError::Null);
            }
        }

        let new_ex = if let Some(ex) = self.expire {
            if ex.duration_since(epoch()) < Duration::from_millis(10) {
                // 保持不变
                entry.value().unwrap().expire()
            } else {
                // 更新
                Some(ex)
            }
        } else {
            // 永不过期
            None
        };

        let new_obj = ObjectInner::new_str(self.value.into(), new_ex);
        let (_, old) = entry.insert_object(new_obj);

        if self.get {
            Ok(Some(RESP3::Bulk(old.unwrap().on_str()?.to_bytes())))
        } else {
            Ok(Some(RESP3::SimpleString("OK".into())))
        }
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        let value = args.next().unwrap();

        let mut next = [0; 7];
        let mut next_len = args.get_uppercase(0, &mut next);
        args.advance(1);
        let opt = match next_len {
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
            Some(len) => {
                let opt = &next[..len];
                match opt {
                    b"NX" => {
                        next_len = args.get_uppercase(0, &mut next);
                        args.advance(1);
                        Some(SetOpt::NX)
                    }
                    b"XX" => {
                        next_len = args.get_uppercase(0, &mut next);
                        args.advance(1);
                        Some(SetOpt::XX)
                    }
                    // 该参数不是设置选项的参数
                    _ => None,
                }
            }
        };

        let get = match next_len {
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
            Some(len) => {
                let get = &next[..len];
                match get {
                    b"GET" => {
                        next_len = args.get_uppercase(0, &mut next);
                        args.advance(1);
                        true
                    }
                    // 该参数不是设置GET的参数
                    _ => false,
                }
            }
        };

        let expire = match next_len {
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
            Some(len) => {
                let ex = &next[..len];
                match ex {
                    b"KEEPTTL" => Some(epoch()),
                    b"EX" => {
                        let expire_value = args.next().ok_or(Err::WrongArgNum)?;
                        Some(Instant::now() + Duration::from_secs(atoi(&expire_value)?))
                    }
                    // PX milliseconds -- 以毫秒为单位设置键的过期时间
                    b"PX" => {
                        let expire_value = args.next().ok_or(Err::WrongArgNum)?;
                        Some(Instant::now() + Duration::from_millis(atoi(&expire_value)?))
                    }
                    // EXAT timestamp -- timestamp是以秒为单位的Unix时间戳
                    b"EXAT" => {
                        let expire_value = args.next().ok_or(Err::WrongArgNum)?;
                        Some(epoch() + Duration::from_secs(atoi(&expire_value)?))
                    }
                    // PXAT timestamp -- timestamp是以毫秒为单位的Unix时间戳
                    b"PXAT" => {
                        let expire_value = args.next().ok_or(Err::WrongArgNum)?;
                        Some(epoch() + Duration::from_millis(atoi(&expire_value)?))
                    }
                    _ => return Err(Err::Syntax.into()),
                }
            }
        };

        // 如果还有多余的参数，说明参数数目不对
        if !args.is_empty() {
            return Err(Err::WrongArgNum.into());
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        shared
            .db()
            .insert_object(
                self.key,
                ObjectInner::new_str(self.value.into(), Some(Instant::now() + self.expire)),
            )
            .await;

        Ok(Some(RESP3::SimpleString("OK".into())))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 3 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        let expire = Duration::from_secs(atoi(&args.next().unwrap())?);
        let value = args.next().unwrap();

        Ok(SetEx { key, value, expire })
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        if shared.db().contains_object(&self.key).await {
            return Err(0.into());
        }

        shared
            .db()
            .insert_object(self.key, ObjectInner::new_str(self.value.into(), None))
            .await;

        Ok(Some(RESP3::Integer(1)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(SetNx {
            key: args.next().unwrap(),
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
    const CMD_TYPE: CmdType = CmdType::Read;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut len = 0;
        shared
            .db()
            .visit_object(&self.key, |obj| {
                len = obj.on_str()?.len();
                Ok(())
            })
            .await?;

        Ok(Some(RESP3::Integer(len as Int)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(StrLen {
            key: args.next().unwrap(),
        })
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
        let shared = Shared::default();

        /************************************/
        /* 测试简单的无过期时间的键值对存取 */
        /************************************/
        let set =
            Set::parse(&mut ["key_never_expire", "value_never_expire"].as_ref().into()).unwrap();
        assert_eq!(
            set._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(&mut ["key_never_expire"].as_ref().into()).unwrap();

        assert_eq!(
            get._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_bulk()
                .unwrap(),
            b"value_never_expire".as_ref()
        );

        /******************************/
        /* 测试带有NX和XX的键值对存取 */
        /******************************/
        let set = Set::parse(&mut ["key_nx", "value_nx", "NX"].as_ref().into()).unwrap();
        assert_eq!(
            set._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(&mut ["key_nx"].as_ref().into()).unwrap();
        assert_eq!(
            get._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_bulk()
                .unwrap(),
            b"value_nx".as_ref()
        );

        let set = Set::parse(&mut ["key_nx", "value_nx", "NX"].as_ref().into()).unwrap();
        assert!(matches!(
            set._execute(&shared).await.unwrap_err(),
            CmdError::Null
        ));

        let set = Set::parse(&mut ["key_xx", "value_xx", "XX"].as_ref().into()).unwrap();
        assert!(matches!(
            set._execute(&shared).await.unwrap_err(),
            CmdError::Null
        ));

        let set = Set::parse(&mut ["key_nx", "value_xx", "XX"].as_ref().into()).unwrap();
        assert_eq!(
            set._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(&mut ["key_nx"].as_ref().into()).unwrap();
        assert_eq!(
            get._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_bulk()
                .unwrap(),
            "value_xx".as_bytes()
        );

        /******************************/
        /* 测试带有GET的键值对存取 */
        /******************************/
        let set = Set::parse(
            &mut ["key_never_expire", "value_never_expire", "GET"]
                .as_ref()
                .into(),
        )
        .unwrap();
        assert_eq!(
            set._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_bulk()
                .unwrap(),
            "value_never_expire".as_bytes()
        );

        let set = Set::parse(
            &mut ["key_never_exist", "value_never_exist", "GET"]
                .as_ref()
                .into(),
        )
        .unwrap();
        assert!(matches!(
            set._execute(&shared).await.unwrap_err(),
            CmdError::Null
        ));

        /**********************************/
        /* 测试带有EX过期时间的键值对存取 */
        /**********************************/
        let set =
            Set::parse(&mut ["key_expire", "value_expire", "ex", "1"].as_ref().into()).unwrap();
        assert_eq!(
            set._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(&mut ["key_expire"].as_ref().into()).unwrap();
        assert_eq!(
            get._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_bulk()
                .unwrap(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_secs(1));
        let get = Get::parse(&mut ["key_expire"].as_ref().into()).unwrap();
        assert!(matches!(get._execute(&shared).await, Err(CmdError::Null)));

        /**********************************/
        /* 测试带有PX过期时间的键值对存取 */
        /**********************************/
        let set =
            Set::parse(&mut ["key_expire", "value_expire", "PX", "500"].as_ref().into()).unwrap();
        assert_eq!(
            set._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(&mut ["key_expire"].as_ref().into()).unwrap();
        assert_eq!(
            get._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_bulk()
                .unwrap(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_millis(500));
        let get = Get::parse(&mut ["key_expire"].as_ref().into()).unwrap();
        assert!(matches!(get._execute(&shared).await, Err(CmdError::Null)));

        /************************************/
        /* 测试带有EXAT过期时间的键值对存取 */
        /************************************/
        let exat = SystemTime::now() + Duration::from_millis(1000);
        let exat = exat
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let set = Set::parse(
            &mut [
                "key_expire",
                "value_expire",
                "EXAT",
                exat.to_string().as_str(),
            ]
            .as_ref()
            .into(),
        )
        .unwrap();
        assert_eq!(
            set._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(&mut ["key_expire"].as_ref().into()).unwrap();
        assert_eq!(
            get._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_bulk()
                .unwrap(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_millis(1000));
        let get = Get::parse(&mut ["key_expire"].as_ref().into()).unwrap();
        assert!(matches!(get._execute(&shared).await, Err(CmdError::Null)));

        /************************************/
        /* 测试带有PXAT过期时间的键值对存取 */
        /************************************/
        let exat = SystemTime::now() + Duration::from_millis(500);
        let exat = exat
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let set = Set::parse(
            &mut [
                "key_expire",
                "value_expire",
                "PXAT",
                exat.to_string().as_str(),
            ]
            .as_ref()
            .into(),
        )
        .unwrap();
        assert_eq!(
            set._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let get = Get::parse(&mut ["key_expire"].as_ref().into()).unwrap();
        assert_eq!(
            get._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_bulk()
                .unwrap(),
            "value_expire".as_bytes()
        );

        sleep(Duration::from_millis(500));
        let get = Get::parse(&mut ["key_expire"].as_ref().into()).unwrap();
        assert!(matches!(get._execute(&shared).await, Err(CmdError::Null)));

        /***************/
        /* 测试KEEPTTL */
        /***************/
        let now = Instant::now();
        let set = Set::parse(
            &mut ["key_expire", "value_expire", "PX", "100000"]
                .as_ref()
                .into(),
        )
        .unwrap();
        assert_eq!(
            set._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let set = Set::parse(
            &mut ["key_expire", "value_expire_modified", "KEEPTTL"]
                .as_ref()
                .into(),
        )
        .unwrap();
        assert_eq!(
            set._execute(&shared)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap(),
            &"OK"
        );

        let obj = shared
            .db()
            .get_object_entry(&"key_expire".into())
            .await
            .unwrap();
        assert_eq!(
            obj.on_str().unwrap().unwrap().to_bytes().as_ref(),
            b"value_expire_modified"
        );
        assert!(
            // 误差在10ms以内
            (obj.value().inner().unwrap().expire().unwrap() - now) - Duration::from_millis(100000)
                < Duration::from_millis(10)
        );
    }
}
