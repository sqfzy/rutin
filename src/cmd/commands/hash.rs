// HDel
// HExists
// HGet
// HSet

use bytes::Bytes;

use crate::{
    cmd::{CmdError, CmdExecutor, CmdType, Err},
    frame::{Bulks, Frame},
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

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
        let mut count = 0;

        shared.db().update_object(&self.key, |obj| {
            let hash = obj.on_hash_mut()?;
            for field in self.fields {
                if hash.remove(&field).is_some() {
                    count += 1;
                }
            }

            Ok(())
        })?;

        Ok(Some(Frame::new_integer(count)))
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(HDel {
            key: args.pop_front().unwrap(),
            fields: args.iter().cloned().collect(),
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

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
        let mut exists = false;

        shared.db().visit_object(&self.key, |obj| {
            let hash = obj.on_hash()?;
            exists = hash.contains_key(&self.field);

            Ok(())
        })?;

        Ok(Some(Frame::new_integer(if exists { 1 } else { 0 })))
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(HExists {
            key: args.pop_front().unwrap(),
            field: args.pop_front().unwrap(),
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

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
        let mut value = None;

        shared.db().visit_object(&self.key, |obj| {
            let hash = obj.on_hash()?;
            value.clone_from(&hash.get(&self.field));

            Ok(())
        })?;

        Ok(value.map(Frame::new_bulk_owned))
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(HGet {
            key: args.pop_front().unwrap(),
            field: args.pop_front().unwrap(),
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

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
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
            })?;

        Ok(Some(Frame::new_integer(count)))
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.len() < 3 || args.len() % 2 != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.pop_front().unwrap();
        let mut fields = Vec::with_capacity(args.len() / 2);
        while !args.is_empty() {
            let field = args.pop_front().unwrap();
            let value = args.pop_front().unwrap();
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
            Frame::new_integer(2)
        );

        let hdel = HDel::parse(&mut ["key", "field1", "field2"].as_ref().into()).unwrap();
        assert_eq!(
            hdel._execute(&shared).await.unwrap().unwrap(),
            Frame::new_integer(2)
        );

        let hdel = HDel::parse(&mut ["key", "field1", "field2"].as_ref().into()).unwrap();
        assert_eq!(
            hdel._execute(&shared).await.unwrap().unwrap(),
            Frame::new_integer(0)
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
            Frame::new_integer(2)
        );

        let hexists = HExists::parse(&mut ["key", "field1"].as_ref().into()).unwrap();
        assert_eq!(
            hexists._execute(&shared).await.unwrap().unwrap(),
            Frame::new_integer(1)
        );

        let hexists = HExists::parse(&mut ["key", "field3"].as_ref().into()).unwrap();
        assert_eq!(
            hexists._execute(&shared).await.unwrap().unwrap(),
            Frame::new_integer(0)
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
            Frame::new_integer(2)
        );

        let hget = HGet::parse(&mut ["key", "field1"].as_ref().into()).unwrap();
        assert_eq!(
            hget._execute(&shared).await.unwrap().unwrap(),
            Frame::Bulk(Bytes::from("value1"))
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
            Frame::new_integer(2)
        );

        let hget = HGet::parse(&mut ["key", "field1"].as_ref().into()).unwrap();
        assert_eq!(
            hget._execute(&shared).await.unwrap().unwrap(),
            Frame::Bulk(Bytes::from("value1"))
        );

        let hget = HGet::parse(&mut ["key", "field2"].as_ref().into()).unwrap();
        assert_eq!(
            hget._execute(&shared).await.unwrap().unwrap(),
            Frame::Bulk(Bytes::from("value2"))
        );
    }
}
