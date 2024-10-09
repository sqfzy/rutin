use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{AsyncStream, Handler},
    shared::db::{Hash, Str},
};
use tracing::instrument;

/// **Integer reply:** The number of fields that were removed from the hash, excluding any specified but non-existing fields.
#[derive(Debug)]
pub struct HDel {
    pub key: StaticBytes,
    pub fields: Vec<StaticBytes>,
}

impl CmdExecutor for HDel {
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
                    if hash.remove(&field).is_some() {
                        count += 1;
                    }
                }

                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(count)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
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
pub struct HExists {
    pub key: StaticBytes,
    pub field: StaticBytes,
}

impl CmdExecutor for HExists {
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
                exists = hash.contains_key(&self.field);

                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(if exists { 1 } else { 0 })))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
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
pub struct HGet {
    pub key: StaticBytes,
    pub field: StaticBytes,
}

impl CmdExecutor for HGet {
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
                value.clone_from(&hash.get(&self.field));

                Ok(())
            })
            .await?;

        Ok(value.map(|b| Resp3::new_blob_string(b.to_bytes())))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
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
pub struct HSet {
    pub key: StaticBytes,
    pub fields: Vec<(StaticBytes, Str)>,
}

impl CmdExecutor for HSet {
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
            .or_insert_with(|| Hash::default().into())
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

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
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

        let hset = HSet::parse(
            gen_cmdunparsed_test(&["key", "field1", "value1", "field2", "value2"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hset.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hdel = HDel::parse(
            gen_cmdunparsed_test(&["key", "field1", "field2"]),
            &AccessControl::new_loose(),
        )
        .unwrap();

        assert_eq!(
            hdel.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hdel = HDel::parse(
            gen_cmdunparsed_test(&["key", "field1", "field2"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hdel.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(0)
        );
    }

    #[tokio::test]
    async fn hexists_test() {
        test_init();
        let mut handler = gen_test_handler();

        let hset = HSet::parse(
            gen_cmdunparsed_test(&["key", "field1", "value1", "field2", "value2"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hset.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hexists = HExists::parse(
            gen_cmdunparsed_test(&["key", "field1"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hexists.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(1)
        );

        let hexists = HExists::parse(
            gen_cmdunparsed_test(&["key", "field3"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hexists.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(0)
        );
    }

    #[tokio::test]
    async fn hget_test() {
        test_init();
        let mut handler = gen_test_handler();

        let hset = HSet::parse(
            gen_cmdunparsed_test(&["key", "field1", "value1", "field2", "value2"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hset.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hget = HGet::parse(
            gen_cmdunparsed_test(&["key", "field1"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hget.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_blob_string("value1".into())
        );

        let hget = HGet::parse(
            gen_cmdunparsed_test(&["key", "field3"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert!(hget.execute(&mut handler).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn hset_test() {
        test_init();
        let mut handler = gen_test_handler();

        let hset = HSet::parse(
            gen_cmdunparsed_test(&["key", "field1", "value1", "field2", "value2"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hset.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hget = HGet::parse(
            gen_cmdunparsed_test(&["key", "field1"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hget.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_blob_string("value1".into())
        );

        let hget = HGet::parse(
            gen_cmdunparsed_test(&["key", "field2"]),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hget.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_blob_string("value2".into())
        );
    }
}
