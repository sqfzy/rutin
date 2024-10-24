use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{AsyncStream, Handler},
    shared::db::{self, Str},
};
use std::{fmt::Debug, hash::Hash};
use tracing::instrument;

/// **Integer reply:** The number of fields that were removed from the hash, excluding any specified but non-existing fields.
#[derive(Debug)]
pub struct HDel<A = StaticBytes> {
    pub key: A,
    pub fields: Vec<A>,
}

impl<A> CmdExecutor<A> for HDel<A>
where
    A: CmdArg,
    Key: for<'a> From<&'a A>,
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
        let mut count = 0;

        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                let hash = obj.on_hash_mut()?;
                for field in self.fields {
                    if hash.remove(field.as_ref()).is_some() {
                        count += 1;
                    }
                }

                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(count)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(HDel {
            key,
            fields: args.collect(),
        })
    }
}

/// **Integer reply:** 0 if the hash does not contain the field, or the key does not exist.
/// **Integer reply:** 1 if the hash contains the field.
#[derive(Debug)]
pub struct HExists<A> {
    pub key: A,
    pub field: A,
}

impl<A> CmdExecutor<A> for HExists<A>
where
    A: CmdArg,
    Key: for<'a> From<&'a A>,
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
        let mut exists = false;

        handler
            .shared
            .db()
            .visit_object(self.key.as_ref(), |obj| {
                let hash = obj.on_hash()?;
                exists = hash.contains_key(self.field.as_ref());

                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(if exists { 1 } else { 0 })))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(HExists {
            key,
            field: args.next().unwrap(),
        })
    }
}

/// **Bulk string reply:** The value associated with the field.
/// **Null reply:** If the field is not present in the hash or key does not exist.
#[derive(Debug)]
pub struct HGet<A> {
    pub key: A,
    pub field: A,
}

impl<A> CmdExecutor<A> for HGet<A>
where
    A: CmdArg,
    Key: for<'a> From<&'a A>,
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
        let mut value = None;

        handler
            .shared
            .db()
            .visit_object(self.key.as_ref(), |obj| {
                let hash = obj.on_hash()?;
                value.clone_from(&hash.get(self.field.as_ref()));

                Ok(())
            })
            .await?;

        Ok(value.map(|b| Resp3::new_blob_string(b.to_bytes())))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(HGet {
            key,
            field: args.next().unwrap(),
        })
    }
}

/// **Integer reply:** the number of fields that were added.
#[derive(Debug)]
pub struct HSet<A> {
    pub key: A,
    pub fields: Vec<(A, Str)>,
}

impl<A> CmdExecutor<A> for HSet<A>
where
    A: CmdArg,
    Key: for<'a> From<&'a A>,
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
        let mut count = 0;

        handler
            .shared
            .db()
            .object_entry(&self.key)
            .await?
            .or_insert_with(|| db::Hash::default().into())
            .update1(|obj| {
                let hash = obj.on_hash_mut()?;
                for (field, value) in self.fields {
                    hash.insert(field.into(), value);
                    count += 1;
                }

                Ok(())
            })?;

        Ok(Some(Resp3::new_integer(count)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 3 || args.len() % 2 != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let mut fields = Vec::with_capacity(args.len() / 2);
        while !args.is_empty() {
            let field = args.next().unwrap();
            let value = args.next().unwrap().into();
            fields.push((field, value));
        }

        Ok(HSet { key, fields })
    }
}

#[cfg(test)]
mod cmd_hash_tests {
    use super::*;
    use crate::util::{gen_test_handler, test_init};

    #[tokio::test]
    async fn hdel_test() {
        test_init();
        let mut handler = gen_test_handler();

        let hset_res = HSet::test(
            &["key", "field1", "value1", "field2", "value2"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(hset_res.into_integer_unchecked(), 2);

        let hdel_res = HDel::test(&["key", "field1", "field2"], &mut handler)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(hdel_res.into_integer_unchecked(), 2);

        let held_res = HDel::test(&["key", "field1", "field2"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(held_res.into_integer_unchecked(), 0);
    }

    #[tokio::test]
    async fn hexists_test() {
        test_init();
        let mut handler = gen_test_handler();

        let hset_res = HSet::test(
            &["key", "field1", "value1", "field2", "value2"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(hset_res.into_integer_unchecked(), 2);

        let hexists_res = HExists::test(&["key", "field1"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hexists_res.into_integer_unchecked(), 1);

        let hexists_res = HExists::test(&["key", "field3"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hexists_res.into_integer_unchecked(), 0);
    }

    #[tokio::test]
    async fn hget_test() {
        test_init();
        let mut handler = gen_test_handler();

        let hset_res = HSet::test(
            &["key", "field1", "value1", "field2", "value2"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(hset_res.into_integer_unchecked(), 2);

        // 测试存在的字段
        let hget_res = HGet::test(&["key", "field1"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hget_res.into_blob_string_unchecked(), "value1");

        // 测试不存在的字段
        let hget_res = HGet::test(&["key", "field3"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert!(hget_res.is_null());
    }

    #[tokio::test]
    async fn hset_test() {
        test_init();
        let mut handler = gen_test_handler();

        // 设置字段并测试返回结果
        let hset_res = HSet::test(
            &["key", "field1", "value1", "field2", "value2"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(hset_res.into_integer_unchecked(), 2);

        // 测试存在的字段 "field1"
        let hget_res = HGet::test(&["key", "field1"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hget_res.into_blob_string_unchecked(), "value1");

        // 测试存在的字段 "field2"
        let hget_res = HGet::test(&["key", "field2"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hget_res.into_blob_string_unchecked(), "value2");
    }
}
