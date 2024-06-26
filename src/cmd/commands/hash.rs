// HDel
// HExists
// HGet
// HSet

use super::*;
use crate::{
    cmd::{CmdError, CmdExecutor, CmdType, CmdUnparsed, Err},
    conf::AccessControl,
    connection::AsyncStream,
    frame::Resp3,
    server::Handler,
    shared::db::ObjValueType::Hash,
    CmdFlag, Key,
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
    const TYPE: CmdType = CmdType::Write;
    const FLAG: CmdFlag = HDEL_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
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

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    pub key: Key,
    pub field: Bytes,
}

impl CmdExecutor for HExists {
    const NAME: &'static str = "HEXISTS";
    const TYPE: CmdType = CmdType::Read;
    const FLAG: CmdFlag = HEXISTS_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
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

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    pub key: Key,
    pub field: Bytes,
}

impl CmdExecutor for HGet {
    const NAME: &'static str = "HGET";
    const TYPE: CmdType = CmdType::Read;
    const FLAG: CmdFlag = HGET_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
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

        Ok(value.map(|b| Resp3::new_blob_string(b.clone())))
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    pub key: Key,
    pub fields: Vec<(Bytes, Bytes)>,
}

impl CmdExecutor for HSet {
    const NAME: &'static str = "HSET";
    const TYPE: CmdType = CmdType::Write;
    const FLAG: CmdFlag = HSET_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut count = 0;

        handler
            .shared
            .db()
            .update_or_create_object(&self.key, Hash, |obj| {
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

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() < 3 || args.len() % 2 != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

        let mut fields = Vec::with_capacity(args.len() / 2);
        while !args.is_empty() {
            let field = args.next().unwrap();
            let value = args.next().unwrap();
            fields.push((field, value));
        }

        Ok(HSet { key, fields })
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
            &mut ["key", "field1", "value1", "field2", "value2"]
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
            &mut ["key", "field1", "field2"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();

        assert_eq!(
            hdel.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(2)
        );

        let hdel = HDel::parse(
            &mut ["key", "field1", "field2"].as_ref().into(),
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
            &mut ["key", "field1", "value1", "field2", "value2"]
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
            &mut ["key", "field1"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hexists.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_integer(1)
        );

        let hexists = HExists::parse(
            &mut ["key", "field3"].as_ref().into(),
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
            &mut ["key", "field1", "value1", "field2", "value2"]
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
            &mut ["key", "field1"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hget.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_blob_string("value1".into())
        );

        let hget = HGet::parse(
            &mut ["key", "field3"].as_ref().into(),
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
            &mut ["key", "field1", "value1", "field2", "value2"]
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
            &mut ["key", "field1"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hget.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_blob_string("value1".into())
        );

        let hget = HGet::parse(
            &mut ["key", "field2"].as_ref().into(),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            hget.execute(&mut handler).await.unwrap().unwrap(),
            Resp3::new_blob_string("value2".into())
        );
    }
}
