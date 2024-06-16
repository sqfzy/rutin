// HDel
// HExists
// HGet
// HSet

use bytes::Bytes;

use crate::{
    cmd::{CmdError, CmdExecutor, CmdType, CmdUnparsed, Err},
    frame::RESP3,
    shared::{db::ObjValueType::Hash, Shared},
    Key,
};

/// **Integer reply:** The number of fields that were removed from the hash, excluding any specified but non-existing fields.
#[derive(Debug)]
pub struct HDel {
    pub key: Key,
    pub fields: Vec<Key>,
}

impl CmdExecutor for HDel {
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut count = 0;

        shared
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

        Ok(Some(RESP3::Integer(count)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(HDel {
            key: args.next().unwrap(),
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
    const CMD_TYPE: CmdType = CmdType::Read;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut exists = false;

        shared
            .db()
            .visit_object(&self.key, |obj| {
                let hash = obj.on_hash()?;
                exists = hash.contains_key(&self.field);

                Ok(())
            })
            .await?;

        Ok(Some(RESP3::Integer(if exists { 1 } else { 0 })))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(HExists {
            key: args.next().unwrap(),
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
    const CMD_TYPE: CmdType = CmdType::Read;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut value = None;

        shared
            .db()
            .visit_object(&self.key, |obj| {
                let hash = obj.on_hash()?;
                value.clone_from(&hash.get(&self.field));

                Ok(())
            })
            .await?;

        Ok(value.map(|b| RESP3::Bulk(b.clone())))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(HGet {
            key: args.next().unwrap(),
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<RESP3>, CmdError> {
        let mut count = 0;

        shared
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

        Ok(Some(RESP3::Integer(count)))
    }

    fn parse(args: &mut CmdUnparsed) -> Result<Self, CmdError> {
        if args.len() < 3 || args.len() % 2 != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
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
        let shared = Shared::default();

        let hset = HSet::parse(
            &mut ["key", "field1", "value1", "field2", "value2"]
                .as_ref()
                .into(),
        )
        .unwrap();
        assert_eq!(
            hset._execute(&shared).await.unwrap().unwrap(),
            RESP3::Integer(2)
        );

        let hdel = HDel::parse(&mut ["key", "field1", "field2"].as_ref().into()).unwrap();
        assert_eq!(
            hdel._execute(&shared).await.unwrap().unwrap(),
            RESP3::Integer(2)
        );

        let hdel = HDel::parse(&mut ["key", "field1", "field2"].as_ref().into()).unwrap();
        assert_eq!(
            hdel._execute(&shared).await.unwrap().unwrap(),
            RESP3::Integer(0)
        );
    }

    #[tokio::test]
    async fn hexists_test() {
        test_init();
        let shared = Shared::default();

        let hset = HSet::parse(
            &mut ["key", "field1", "value1", "field2", "value2"]
                .as_ref()
                .into(),
        )
        .unwrap();
        assert_eq!(
            hset._execute(&shared).await.unwrap().unwrap(),
            RESP3::Integer(2)
        );

        let hexists = HExists::parse(&mut ["key", "field1"].as_ref().into()).unwrap();
        assert_eq!(
            hexists._execute(&shared).await.unwrap().unwrap(),
            RESP3::Integer(1)
        );

        let hexists = HExists::parse(&mut ["key", "field3"].as_ref().into()).unwrap();
        assert_eq!(
            hexists._execute(&shared).await.unwrap().unwrap(),
            RESP3::Integer(0)
        );
    }

    #[tokio::test]
    async fn hget_test() {
        test_init();
        let shared = Shared::default();

        let hset = HSet::parse(
            &mut ["key", "field1", "value1", "field2", "value2"]
                .as_ref()
                .into(),
        )
        .unwrap();
        assert_eq!(
            hset._execute(&shared).await.unwrap().unwrap(),
            RESP3::Integer(2)
        );

        let hget = HGet::parse(&mut ["key", "field1"].as_ref().into()).unwrap();
        assert_eq!(
            hget._execute(&shared).await.unwrap().unwrap(),
            RESP3::Bulk("value1".into())
        );

        let hget = HGet::parse(&mut ["key", "field3"].as_ref().into()).unwrap();
        assert!(hget._execute(&shared).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn hset_test() {
        test_init();
        let shared = Shared::default();

        let hset = HSet::parse(
            &mut ["key", "field1", "value1", "field2", "value2"]
                .as_ref()
                .into(),
        )
        .unwrap();
        assert_eq!(
            hset._execute(&shared).await.unwrap().unwrap(),
            RESP3::Integer(2)
        );

        let hget = HGet::parse(&mut ["key", "field1"].as_ref().into()).unwrap();
        assert_eq!(
            hget._execute(&shared).await.unwrap().unwrap(),
            RESP3::Bulk("value1".into())
        );

        let hget = HGet::parse(&mut ["key", "field2"].as_ref().into()).unwrap();
        assert_eq!(
            hget._execute(&shared).await.unwrap().unwrap(),
            RESP3::Bulk("value2".into())
        );
    }
}
