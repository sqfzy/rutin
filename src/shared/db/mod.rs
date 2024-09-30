mod object;
mod object_entry;

use std::fmt::Debug;

pub use object::*;
pub use object_entry::ObjectEntry;

use crate::{
    conf::OomConf,
    error::{RutinError, RutinResult},
    shared::Outbox,
    Key,
};
use ahash::RandomState;
use dashmap::{
    mapref::{
        entry_ref::EntryRef,
        one::{Ref, RefMut},
    },
    DashMap, DashSet, Map,
};
use tokio::time::Instant;
use tracing::instrument;

#[derive(Debug)]
pub struct Db {
    // 支持：
    //  键过期
    //  OOM处理
    //  事件回调
    pub entries: DashMap<Key, Object, RandomState>,

    // 记录具有expire的键，以便进行**定期删除**，所有修改过期时间的操作都应该更新记录
    // TODO: 复用entries的hash值
    entry_expire_records: DashSet<(Instant, Key), RandomState>,

    // Key代表频道名，每个频道名映射着一组Sender，通过这些Sender可以发送消息给订阅频道
    // 的客户端
    pub_sub: DashMap<Key, Vec<Outbox>, RandomState>,

    // // 记录已经连接的客户端，并且映射到该连接的`BgTaskSender`，使用该sender可以向该连接
    // // 的客户端发送消息。利用client_records，一个连接可以代表另一个连接向其客户端发送
    // // 消息
    // client_records: DashMap<Id, BgTaskSender, nohash::BuildNoHashHasher<u64>>,
    oom_conf: Option<OomConf>,
}

impl Db {
    pub fn new(mem_conf: Option<OomConf>) -> Self {
        let num_cpus = num_cpus::get();

        Self {
            entries: DashMap::with_capacity_and_hasher_and_shard_amount(
                1024 * 16,
                RandomState::new(),
                (num_cpus * 2).next_power_of_two(),
            ),
            entry_expire_records: DashSet::with_capacity_and_hasher(512, RandomState::new()),
            pub_sub: DashMap::with_capacity_and_hasher(8, RandomState::new()),
            oom_conf: mem_conf,
        }
    }

    // pub fn entries(&self) -> &DashMap<Key, Object, RandomState> {
    //     &self.entries
    // }
    //
    // pub fn entries_expire_records(&self) -> &DashSet<(Instant, Key), RandomState> {
    //     &self.entry_expire_records
    // }

    pub fn entries_size(&self) -> usize {
        self.entries.len()
    }

    pub fn clear(&self) {
        self.entries.clear();
        self.entry_expire_records.clear();
        self.pub_sub.clear();
    }

    #[inline]
    pub async fn try_evict(&self) -> RutinResult<()> {
        if let Some(mem_conf) = &self.oom_conf {
            mem_conf.try_evict(self).await
        } else {
            Ok(())
        }
    }

    // 该函数仅供Db模块内部使用，也可以用于测试
    //
    // # Error:
    // 对象不存在
    // 对象已过期
    #[instrument(level = "debug", skip(self))]
    pub async fn get_object(&self, key: &[u8]) -> RutinResult<Ref<'_, Key, Object>> {
        // 键存在
        if let Some(e) = self.entries._get(key) {
            let e = Events::try_trigger_lock_event1(e, &self.entries).await?;
            let value = e.value();

            // 对象未过期
            if !value.is_expired() {
                value.update_atc1();

                return Ok(e);
            }

            // 对象已过期，移除该键值对(不触发事件)
            drop(e);
            if let Some((_, mut object)) = self.entries.remove(key) {
                Events::try_trigger_read_and_write_event(&mut object);
            }
        }

        Err(RutinError::Null)
    }

    // 该函数仅供Db模块内部使用，也可以用于测试
    //
    // # Error:
    // OOM
    // 对象不存在
    // 对象已过期
    #[instrument(level = "debug", skip(self))]
    pub async fn get_object_mut(&self, key: &[u8]) -> RutinResult<RefMut<'_, Key, Object>> {
        self.try_evict().await?;

        // 键存在
        if let Some(e) = self.entries._get_mut(key) {
            let mut e = Events::try_trigger_lock_event2(e, &self.entries).await?;
            let value = e.value_mut();

            // 对象未过期
            if !value.is_expired() {
                value.update_atc2();

                return Ok(e);
            }

            // 对象已过期，移除该键值对
            drop(e);
            if let Some((_, mut object)) = self.entries.remove(key) {
                Events::try_trigger_read_and_write_event(&mut object);
            }
        }

        Err(RutinError::Null)
    }

    #[inline]
    pub async fn object_entry<'a, 'b, Q>(
        &'a self,
        key: &'b Q,
    ) -> RutinResult<ObjectEntry<'a, 'b, Q>>
    where
        Q: std::hash::Hash + equivalent::Equivalent<Key> + Debug,
    {
        self.try_evict().await?;
        Ok(self.entry_unchecked_oom(key).await)
    }

    #[instrument(level = "debug", skip(self))]
    async fn entry_unchecked_oom<'a, 'b, Q>(&'a self, key: &'b Q) -> ObjectEntry<'a, 'b, Q>
    where
        Q: std::hash::Hash + equivalent::Equivalent<Key> + ?Sized + Debug,
    {
        let entry = self.entries._entry_ref(key);

        match entry {
            EntryRef::Occupied(e) => {
                let entry = Events::try_trigger_lock_event3(key, e, &self.entries).await;

                match entry {
                    EntryRef::Occupied(mut e) => {
                        let value = e.get_mut();

                        if !value.is_expired() {
                            value.update_atc2();

                            return EntryRef::Occupied(e).into();
                        }

                        // 对象已过期，移除该键值对，并触发事件
                        let mut object = e.remove();
                        Events::try_trigger_read_and_write_event(&mut object);

                        // 重新获取一个Vacant
                        let e = self.entries.entry_ref(key);

                        debug_assert!(matches!(e, EntryRef::Vacant(_)));

                        e.into()
                    }
                    EntryRef::Vacant(_) => entry.into(),
                }
            }
            EntryRef::Vacant(_) => entry.into(),
        }
    }
}

// 以下函数依赖于get(), get_mut(), entry(), entry_unchecked_oom()
impl Db {
    /// 检验对象是否合法存在。如果对象不存在，对象为空或者对象已过期则返回false，否则返回true
    #[instrument(level = "debug", skip(self), ret)]
    pub async fn contains_object(&self, key: &[u8]) -> bool {
        match self.get_object(key).await {
            Ok(e) => {
                Events::try_trigger_read_event(e.value());
                true
            }
            Err(_) => false,
        }
    }

    /// # Desc:
    ///
    /// 尝试访问对象，之后可以通过on_str(), on_list()等接口获取不可变的对象值的引用
    ///
    /// # Error:
    ///
    /// 对象不存在
    /// 对象已过期
    /// f返回错误
    #[instrument(level = "debug", skip(self, f))]
    pub async fn visit_object(
        &self,
        key: &[u8],
        f: impl FnOnce(&Object) -> RutinResult<()>,
    ) -> RutinResult<()> {
        let e = self.get_object(key).await?;
        let obj = e.value();

        f(obj)?;

        Events::try_trigger_read_event(obj);

        Ok(())
    }

    #[instrument(level = "debug", skip(self, f), err)]
    pub async fn update_object(
        &self,
        key: &[u8],
        f: impl FnOnce(&mut Object) -> RutinResult<()>,
    ) -> RutinResult<()> {
        let mut e = self.get_object_mut(key).await?;
        let obj = e.value_mut();

        f(obj)?;

        Events::try_trigger_read_and_write_event(obj);

        Ok(())
    }

    #[inline]
    pub async fn insert_object<'a, 'b, Q>(
        &'a self,
        key: &'b Q,
        object: Object,
    ) -> RutinResult<Option<Object>>
    where
        Q: std::hash::Hash + equivalent::Equivalent<Key> + Debug,
        bytes::Bytes: From<&'b Q>,
    {
        Ok(self.object_entry(key).await?.insert2(object).1)
    }

    #[inline]
    #[instrument(level = "debug", skip(self), ret)]
    pub async fn remove_object<'a, 'b, Q>(&'a self, key: &'b Q) -> Option<(Key, Object)>
    where
        Q: std::hash::Hash + equivalent::Equivalent<Key> + Debug,
        bytes::Bytes: From<&'b Q>,
    {
        self.entry_unchecked_oom(key).await.remove()
    }

    // #[inline]
    // #[instrument(level = "debug", skip(self), ret)]
    // pub async fn remove_object2<'a, 'b, Q>(&'a self, key: &'b Q) -> Option<(Key, Object)>
    // where
    //     Q: std::hash::Hash + equivalent::Equivalent<Key> + Debug,
    // {
    //     // Safety: remove操作不会影响key的生命周期
    //     self.entry_unchecked_oom(key).await.remove()
    // }

    #[instrument(level = "debug", skip(self, f), err)]
    pub async fn update_object_force<'a, 'b, Q>(
        &'a self,
        key: &'b Q,
        typ: ObjectValueType,
        f: impl FnOnce(&mut Object) -> RutinResult<()>,
    ) -> RutinResult<()>
    where
        Q: std::hash::Hash + equivalent::Equivalent<Key> + Debug,
        bytes::Bytes: From<&'b Q>,
    {
        if let (e, Some(f)) = self.object_entry(key).await?.update2(f)? {
            e.or_try_insert_with(|| -> RutinResult<Object> {
                let mut obj = match typ {
                    ObjectValueType::Str => Str::default().into(),
                    ObjectValueType::List => List::default().into(),
                    ObjectValueType::Set => Set::default().into(),
                    ObjectValueType::Hash => Hash::default().into(),
                    ObjectValueType::ZSet => ZSet::default().into(),
                };

                f(&mut obj)?;

                Ok(obj)
            })?;
        }

        Ok(())
    }
}

impl Db {
    // 获取该频道的所有监听者
    #[instrument(level = "debug", skip(self))]
    pub fn get_channel_all_listener(&self, topic: &[u8]) -> Option<Vec<Outbox>> {
        self.pub_sub.get(topic).map(|listener| listener.clone())
    }

    // 向频道添加一个监听者
    #[instrument(level = "debug", skip(self, listener))]
    pub fn add_channel_listener(&self, topic: Key, listener: Outbox) {
        self.pub_sub.entry(topic).or_default().push(listener);
    }

    // 移除频道的一个监听者
    #[instrument(level = "debug", skip(self, listener))]
    pub fn remove_channel_listener(&self, topic: &[u8], listener: &Outbox) -> Option<Outbox> {
        if let Some(mut pubs) = self.pub_sub.get_mut(topic) {
            // 如果找到匹配的listener，则移除
            if let Some(index) = pubs.iter().position(|l| l.same_channel(listener)) {
                let publ = pubs.remove(index);
                // 如果移除后，该频道已经没有订阅者，则删除该频道
                if pubs.is_empty() {
                    drop(pubs);
                    self.pub_sub.remove(topic);
                }
                return Some(publ);
            }
        }
        None
    }
}

// #[cfg(test)]
// pub mod db_tests {
//     use crate::{
//         server::NEVER_EXPIRE,
//         util::{get_test_db, test_init},
//     };
//
//     use super::*;
//
//     #[tokio::test]
//     async fn insert_object_test() {
//         test_init();
//
//         let db = get_test_db();
//
//         // 无对象时插入对象
//         db.insert_object("key1".into(), ObjectValue::new_str("value1", *NEVER_EXPIRE))
//             .await
//             .unwrap();
//
//         let res = db
//             .get(&"key1".into())
//             .await
//             .unwrap()
//             .on_str()
//             .unwrap()
//             .unwrap()
//             .to_bytes();
//         assert_eq!(res, "value1".as_bytes());
//
//         // 有对象时插入对象，触发旧对象的Update事件
//         let (tx, rx) = flume::unbounded();
//         db.add_may_update_event("key1".into(), tx.clone())
//             .await
//             .unwrap();
//
//         db.insert_object("key1".into(), ObjectValue::new_str("value2", *NEVER_EXPIRE))
//             .await
//             .unwrap();
//         let res = db
//             .get(&"key1".into())
//             .await
//             .unwrap()
//             .on_str()
//             .unwrap()
//             .unwrap()
//             .to_bytes();
//         assert_eq!(res, "value2".as_bytes());
//
//         let res = rx.recv().unwrap();
//         assert_eq!(&res.to_bytes(), "key1");
//
//         // 存在空对象时插入对象，触发Update事件
//         db.add_may_update_event("key2".into(), tx.clone())
//             .await
//             .unwrap();
//
//         db.insert_object("key2".into(), ObjectValue::new_str("value2", *NEVER_EXPIRE))
//             .await
//             .unwrap();
//         let res = db
//             .get(&"key1".into())
//             .await
//             .unwrap()
//             .on_str()
//             .unwrap()
//             .unwrap()
//             .to_bytes();
//         assert_eq!(res, "value2".as_bytes());
//
//         let res = rx.recv().unwrap();
//         assert_eq!(&res.to_bytes(), "key2");
//     }
//
//     #[tokio::test]
//     async fn visit_object_test() {
//         test_init();
//
//         let db = get_test_db();
//
//         db.insert_object("key1".into(), ObjectValue::new_str("value1", *NEVER_EXPIRE))
//             .await
//             .unwrap();
//
//         // 访问有效对象，应该成功
//         db.visit_object(&"key1".into(), |_| Ok(())).await.unwrap();
//
//         // 访问不存在的对象，应该失败
//         db.visit_object(&"key_not_exist".into(), |_| Ok(()))
//             .await
//             .unwrap_err();
//
//         // 访问空对象时，应该失败
//         let (tx, _) = flume::unbounded();
//
//         db.add_may_update_event("key_none".into(), tx.clone())
//             .await
//             .unwrap(); // 这会创建一个空对象
//         db.visit_object(&"key_none".into(), |_| Ok(()))
//             .await
//             .unwrap_err();
//
//         // 访问过期对象时，应该失败，并且过期对象会被删除
//         db.insert_object(
//             "key_expired".into(),
//             ObjectValue::new_str("key_expired", Instant::now()),
//         )
//         .await
//         .unwrap();
//         db.visit_object(&"key_expired".into(), |_| Ok(()))
//             .await
//             .unwrap_err();
//         assert!(db.get(&"key_expired".into()).await.is_err());
//     }
//
//     #[tokio::test]
//     async fn update_object_test() {
//         test_init();
//
//         let db = get_test_db();
//         let (tx, rx) = flume::unbounded();
//
//         db.insert_object("key1".into(), ObjectValue::new_str("value1", *NEVER_EXPIRE))
//             .await
//             .unwrap();
//
//         // 更新有效对象，应该成功，并且触发Update事件
//         db.add_may_update_event("key1".into(), tx.clone())
//             .await
//             .unwrap();
//
//         db.update_object("key1".into(), |obj| {
//             obj.on_str_mut().unwrap().set("value2");
//             Ok(())
//         })
//         .await
//         .unwrap();
//
//         let update_res = db
//             .get(&"key1".into())
//             .await
//             .unwrap()
//             .on_str()
//             .unwrap()
//             .unwrap()
//             .to_bytes();
//         assert_eq!(update_res, "value2".as_bytes());
//
//         let event_res = rx.recv().unwrap();
//         assert_eq!(&event_res.to_bytes(), "key1");
//
//         // 更新不存在的对象，应该失败
//         db.update_object("key_not_exist".into(), |_| Ok(()))
//             .await
//             .unwrap_err();
//
//         // 更新空对象时，应该失败
//         db.add_may_update_event("key_none".into(), tx.clone())
//             .await
//             .unwrap(); // 这会创建一个空对象
//
//         db.update_object("key_none".into(), |_| Ok(()))
//             .await
//             .unwrap_err();
//
//         // // 更新过期对象时，应该失败，并且过期对象会被删除
//         db.insert_object(
//             "key_expired".into(),
//             ObjectValue::new_str("value", Instant::now()),
//         )
//         .await
//         .unwrap();
//         db.update_object("key_expired".into(), |_| Ok(()))
//             .await
//             .unwrap_err();
//         assert!(db.get(&"key_expired".into()).await.is_err());
//     }
//
//     #[tokio::test]
//     async fn update_or_create_object_test() {
//         test_init();
//
//         let db = get_test_db();
//         let (tx, rx) = flume::unbounded();
//
//         db.insert_object("key1".into(), ObjectValue::new_str("value1", *NEVER_EXPIRE))
//             .await
//             .unwrap();
//
//         // 更新或创建，更新有效象应该成功，并且触发Update事件
//         db.add_may_update_event("key1".into(), tx.clone())
//             .await
//             .unwrap();
//
//         db.update_or_create_object("key1".into(), ObjValueType::Str, |obj| {
//             obj.on_str_mut().unwrap().set("value2");
//             Ok(())
//         })
//         .await
//         .unwrap();
//
//         let update_res = db
//             .get(&"key1".into())
//             .await
//             .unwrap()
//             .on_str()
//             .unwrap()
//             .unwrap()
//             .to_bytes();
//         assert_eq!(update_res, "value2".as_bytes());
//
//         let event_res = rx.recv().unwrap();
//         assert_eq!(&event_res.to_bytes(), "key1");
//
//         // 更新或创建，更新不存在的对象，应该创建新对象
//         db.update_or_create_object("key_not_exist".into(), ObjValueType::Str, |obj| {
//             obj.on_str_mut().unwrap().set("value");
//             Ok(())
//         })
//         .await
//         .unwrap();
//
//         let update_res = db
//             .get(&"key_not_exist".into())
//             .await
//             .unwrap()
//             .on_str()
//             .unwrap()
//             .unwrap()
//             .to_bytes();
//         assert_eq!(update_res, "value".as_bytes());
//
//         // 更新或创建，更新空对象，应该创建新对象并触发空对象的Update事件
//         db.add_may_update_event("key_none".into(), tx.clone())
//             .await
//             .unwrap(); // 这会创建一个空对象
//         db.update_or_create_object("key_none".into(), ObjValueType::Str, |obj| {
//             obj.on_str_mut().unwrap().set("value");
//             Ok(())
//         })
//         .await
//         .unwrap();
//
//         let update_res = db
//             .get(&"key_none".into())
//             .await
//             .unwrap()
//             .on_str()
//             .unwrap()
//             .unwrap()
//             .to_bytes();
//         assert_eq!(update_res, "value".as_bytes());
//
//         let event_res = rx.recv().unwrap();
//         assert_eq!(&event_res.to_bytes(), "key_none");
//     }
// }
