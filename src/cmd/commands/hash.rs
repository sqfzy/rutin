// HDel
// HExists
// HGet
// HSet

use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::AsyncStream,
    server::Handler,
    shared::db::ObjValueType::Hash,
    Key,
};
use bytes::Bytes;
use tracing::instrument;

/// **Integer reply:** The number of fields that were removed from the hash, excluding any specified but non-existing fields.
#[derive(Debug)]
pub struct HDel {
    pub key: Key,
    pub fields: Vec<Key>,
}

impl CmdExecutor for HDel {
    const NAME: &'static str = "HDEL";
    const CATS_FLAG: CatFlag = HDEL_CATS_FLAG;
    const CMD_FLAG: CmdFlag = HDEL_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut count = 0;

        handler
            .shared
            .db()
            .update_object(self.key, |obj| {
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
            key: key.into(),
            fields: args.map(Key::from).collect(),
        })
    }
}

/// **Integer reply:** 0 if the hash does not contain the field, or the key does not exist.
/// **Integer reply:** 1 if the hash contains the field.
#[derive(Debug)]
pub struct HExists {
    pub key: Key,
    pub field: Key,
}

impl CmdExecutor for HExists {
    const NAME: &'static str = "HEXISTS";
    const CATS_FLAG: CatFlag = HEXISTS_CATS_FLAG;
    const CMD_FLAG: CmdFlag = HEXISTS_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut exists = false;

        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
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
            key: key.into(),
            field: args.next().unwrap().into(),
        })
    }
}

/// **Bulk string reply:** The value associated with the field.
/// **Null reply:** If the field is not present in the hash or key does not exist.
#[derive(Debug)]
pub struct HGet {
    pub key: Key,
    pub field: Key,
}

impl CmdExecutor for HGet {
    const NAME: &'static str = "HGET";
    const CATS_FLAG: CatFlag = HGET_CATS_FLAG;
    const CMD_FLAG: CmdFlag = HGET_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut value = None;

        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
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
            key: key.into(),
            field: args.next().unwrap().into(),
        })
    }
}

/// **Integer reply:** the number of fields that were added.
#[derive(Debug)]
pub struct HSet {
    pub key: Key,
    pub fields: Vec<(Key, Bytes)>,
}

impl CmdExecutor for HSet {
    const NAME: &'static str = "HSET";
    const CATS_FLAG: CatFlag = HSET_CATS_FLAG;
    const CMD_FLAG: CmdFlag = HSET_CMD_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(self, handler: &mut Handler<impl AsyncStream>) -> RutinResult<Option<Resp3>> {
        let mut count = 0;

        handler
            .shared
            .db()
            .update_or_create_object(self.key, Hash, |obj| {
                let hash = obj.on_hash_mut()?;
                for (field, value) in self.fields {
                    hash.insert(field, value);
                    count += 1;
                }

                Ok(())
            })
            .await?;

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
            let field = args.next().unwrap().into();
            let value = args.next().unwrap();
            fields.push((field, value));
        }

        Ok(HSet {
            key: key.into(),
            fields,
        })
    }
}

#[cfg(test)]
mod cmd_hash_tests {
    use super::*;
    use crate::util::test_init;

    #[tokio::test]
    async fn hdel_test() {
        test_init();
        let (mut handler, _) = Handler::new_fake();

        let hset = HSet::parse(
            ["key", "field1", "value1", "field2", "value2"]
                .as_ref()
                .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hset.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hdel = HDel::parse(
            ["key", "field1", "field2"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();

        assert_eq!(
            hdel.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hdel = HDel::parse(
            ["key", "field1", "field2"].as_ref().into(),
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
        let (mut handler, _) = Handler::new_fake();

        let hset = HSet::parse(
            ["key", "field1", "value1", "field2", "value2"]
                .as_ref()
                .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hset.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hexists = HExists::parse(
            ["key", "field1"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hexists.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(1)
        );

        let hexists = HExists::parse(
            ["key", "field3"].as_ref().into(),
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
        let (mut handler, _) = Handler::new_fake();

        let hset = HSet::parse(
            ["key", "field1", "value1", "field2", "value2"]
                .as_ref()
                .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hset.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hget = HGet::parse(
            ["key", "field1"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hget.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_blob_string("value1".into())
        );

        let hget = HGet::parse(
            ["key", "field3"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert!(hget.execute(&mut handler).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn hset_test() {
        test_init();
        let (mut handler, _) = Handler::new_fake();

        let hset = HSet::parse(
            ["key", "field1", "value1", "field2", "value2"]
                .as_ref()
                .into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hset.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hget = HGet::parse(
            ["key", "field1"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hget.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_blob_string("value1".into())
        );

        let hget = HGet::parse(
            ["key", "field2"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hget.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_blob_string("value2".into())
        );
    }
}
