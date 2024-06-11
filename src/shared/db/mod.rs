mod error;
mod event;
mod object;
mod object_entry;

pub use error::DbError;
pub use event::EventType;
pub use object::*;
use object_entry::ObjectEntryMut;

use crate::{cmd::CmdResult, frame::RESP3, server::BgTaskSender, Id, Key};
use ahash::RandomState;
use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use flume::Sender;
use tokio::time::Instant;
use tracing::{error, instrument};

#[derive(Debug)]
pub struct Db {
    // 键值对存在不一定代表着对象一定是有效的，例如当只希望监听键的事件而不希望创建对
    // 象时，对象为空
    // 1. 对象存在，事件存在
    // 2. 对象不存在，事件存在
    entries: DashMap<Key, Object, RandomState>,

    // 记录具有expire的键，以便进行**定期删除**，所有修改过期时间的操作都应该更新记录
    entry_expire_records: DashSet<(Instant, Key), RandomState>,

    // Key代表频道名，每个频道名映射着一组Sender，通过这些Sender可以发送消息给订阅频道
    // 的客户端
    pub_sub: DashMap<Key, Vec<Sender<RESP3>>, RandomState>,

    // 记录已经连接的客户端，并且映射到该连接的`BgTaskSender`，使用该sender可以向该连接
    // 的客户端发送消息。利用client_records，一个连接可以代表另一个连接向其客户端发送
    // 消息
    client_records: DashMap<Id, BgTaskSender, RandomState>,
}

impl Db {
    pub fn entries(&self) -> &DashMap<Key, Object, RandomState> {
        &self.entries
    }

    pub fn size(&self) -> usize {
        self.entries.len()
    }

    // 记录客户端ID和其对应的`BgTaskSender`，用于向客户端发送消息
    #[inline]
    #[instrument(level = "debug", skip(self, id, bg_sender), ret)]
    pub fn record_client_id(&self, mut id: u128, bg_sender: BgTaskSender) -> Id {
        loop {
            match self.client_records.entry(id) {
                // 如果id已经存在，则自增1
                Entry::Occupied(_) => id += 1,
                // 如果id不存在，则插入
                Entry::Vacant(e) => {
                    e.insert(bg_sender);
                    return id;
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self), ret)]
    pub fn get_client_bg_sender(&self, client_id: u128) -> Option<BgTaskSender> {
        self.client_records.get(&client_id).map(|e| e.clone())
    }

    #[instrument(level = "debug", skip(self, sender))]
    pub async fn add_event(&self, key: Key, sender: Sender<RESP3>, event: EventType) {
        match self.entries.entry(key) {
            Entry::Occupied(mut e) => {
                let obj = e.get_mut();
                obj.event_mut().await.add_event(sender, event);
            }
            Entry::Vacant(e) => {
                // 不存在对象，创建一个空对象
                let mut obj = Object::new(None, Default::default());
                obj.event_mut().await.add_event(sender, event);
                e.insert(obj);
            }
        }
    }

    #[inline]
    pub fn entry_expire_records(&self) -> &DashSet<(Instant, Key), RandomState> {
        &self.entry_expire_records
    }

    #[inline]
    pub fn update_expire_records(
        &self,
        key: &Key,
        new_ex: Option<Instant>,
        old_ex: Option<Instant>,
    ) {
        // 相等则无需更新记录
        if new_ex == old_ex {
            return;
        }

        // 如果old_expire不为None，则记录中存有旧的过期时间需要移除
        if let Some(old_ex) = old_ex {
            self.entry_expire_records.remove(&(old_ex, key.clone()));
        }
        // 如果new_expire不为None，则需要记录新的过期时间
        if let Some(new_ex) = new_ex {
            if new_ex > Instant::now() {
                self.entry_expire_records.insert((new_ex, key.clone()));
            }
        }
    }

    pub fn remove_expire_record(&self, record: &(Instant, Key)) {
        self.entry_expire_records.remove(record);
    }
}

// cmd模块只应该使用以下接口操作数据库
impl Db {
    /// 检验对象是否合法存在。如果对象不存在，对象为空或者对象已过期则返回false，否则返回true
    pub async fn contains_object(&self, key: &Key) -> bool {
        if let Some(e) = self.entries.get(key) {
            if let Some(obj) = e.inner().await {
                if !obj.is_expired() {
                    return true;
                }

                // 对象已过期，移除该键值对
                drop(e);
                self.remove_object(key).await;
            }
        }
        false
    }

    // pub fn get_object_entry(&self, key: &Key) -> Option<Ref<'_, Bytes, Object, RandomState>> {
    //     // 键存在
    //     if let Some(e) = self.entries.get(key) {
    //         // 对象不为空
    //         if let Some(inner) = &e.value().inner {
    //             // 对象未过期
    //             if !inner.is_expired() {
    //                 return Some(e);
    //             }
    //
    //             // 对象已过期，移除该键值对
    //             drop(e);
    //             self.remove_object(key);
    //         }
    //     }
    //
    //     None
    // }

    #[inline]
    pub fn get_object_entry_mut(&self, key: Key) -> ObjectEntryMut {
        ObjectEntryMut::new(self.entries.entry(key), self)
    }

    /// # Desc:
    ///
    /// 尝试访问对象，之后可以通过on_str(), on_list()等接口获取不可变的对象值的引用
    ///
    /// # Error:
    ///
    /// 如果对象不存在，对象为空或者对象已过期则返回CmdError::from(DbError::KeyNotFound)
    #[instrument(level = "debug", skip(self, f))]
    pub async fn visit_object(
        &self,
        key: &Key,
        f: impl FnOnce(&ObjectInner) -> CmdResult<()>,
    ) -> CmdResult<()> {
        let entry = if let Some(e) = self.entries.get(key) {
            e
        } else {
            // 对象不存在
            error!("object not found");
            return Err(DbError::KeyNotFound.into());
        };

        let obj_inner = if let Some(inner) = entry.inner().await {
            inner
        } else {
            // 对象为空对象
            error!("object is None");
            return Err(DbError::KeyNotFound.into());
        };

        if obj_inner.is_expired() {
            // 对象已过期，移除该键值对
            error!("object is expired");
            drop(entry);
            self.remove_object(key).await;
            return Err(DbError::KeyNotFound.into());
        }

        // 对象合法，可以进行访问
        f(obj_inner)
    }

    pub async fn insert_object(&self, key: Key, object: ObjectInner) -> Option<ObjectInner> {
        self.get_object_entry_mut(key).insert_object(object).await.1
    }

    // /// # Desc:
    // ///
    // /// 当键值对不存在时插入键值对，对象一定不为空。会触发旧对象中的Update事件
    // ///
    // /// # Error:
    // ///
    // /// 如果对象存在则返回CmdError::Null
    // #[instrument(level = "debug", skip(self), ret)]
    // pub fn insert_object_if_nx(&self, key: Key, object: Object) -> CmdResult<Option<ObjectInner>> {
    //     let obj_inner = object.inner.unwrap();
    //     let new_ex = obj_inner.expire();
    //
    //     match self.entries.entry(key.clone()) {
    //         Entry::Occupied(mut e) => {
    //             let obj = e.get_mut();
    //
    //             if obj.inner.is_some() {
    //                 return Err(CmdError::Null);
    //             }
    //             // 对象为空对象
    //
    //             let mut old = e.insert(ObjectInner::new(obj_inner));
    //             drop(e); // 尽早释放锁
    //
    //             // 空对象可能存在事件
    //             old.event
    //                 .trigger_events(&key, &[EventType::MayUpdate, EventType::Track]);
    //
    //             self.update_expire_records(&key, new_ex, None);
    //
    //             Ok(None)
    //         }
    //         Entry::Vacant(e) => {
    //             e.insert(ObjectInner::new(obj_inner));
    //
    //             self.update_expire_records(&key, new_ex, None);
    //
    //             Ok(None)
    //         }
    //     }
    // }

    // /// # Desc:
    // ///
    // /// 当键存在时插入键值对，对象一定不为空。会触发旧对象中的Update事件
    // ///
    // /// # Error:
    // ///
    // /// 如果对象不存在则返回CmdError::Null
    // #[instrument(level = "debug", skip(self), ret)]
    // pub fn insert_object_if_xx(&self, key: Key, object: Object) -> CmdResult<Option<ObjectInner>> {
    //     let obj_inner = object.inner.as_ref().unwrap();
    //     let new_ex = obj_inner.expire();
    //
    //     match self.entries.entry(key.clone()) {
    //         Entry::Occupied(mut e) => {
    //             if e.get().inner.is_none() {
    //                 return Err(CmdError::Null);
    //             }
    //
    //             let mut old_obj = e.insert(object);
    //             drop(e); // 尽早释放锁
    //
    //             // 尝试触发事件
    //             old_obj
    //                 .event
    //                 .trigger_events(&key, &[EventType::MayUpdate, EventType::Track]);
    //
    //             self.update_expire_records(&key, new_ex, old_obj.inner.as_ref().unwrap().expire());
    //
    //             Ok(old_obj.inner)
    //         }
    //         Entry::Vacant(_) => Err(CmdError::Null),
    //     }
    // }

    /// # Desc:
    ///
    /// 移除键值对。如果存在旧对象，则会触发旧对象中的Remove事件
    pub async fn remove_object(&self, key: &Key) -> Option<(Key, Object)> {
        self.get_object_entry_mut(key.clone()).remove_object().await
    }

    pub async fn update_object(
        &self,
        key: &Key,
        f: impl FnOnce(&mut ObjectInner) -> CmdResult<()>,
    ) -> CmdResult<()> {
        self.get_object_entry_mut(key.clone())
            .update_object_value(f)
            .await
    }

    pub async fn update_or_create_object(
        &self,
        key: &Key,
        typ: ObjValueType,
        f: impl FnOnce(&mut ObjectInner) -> CmdResult<()>,
    ) -> CmdResult<()> {
        self.get_object_entry_mut(key.clone())
            .update_or_create_object(typ, f)
            .await?;

        Ok(())
    }
}

impl Db {
    // 获取该频道的所有监听者
    #[instrument(level = "debug", skip(self))]
    pub fn get_channel_all_listener(&self, topic: &[u8]) -> Option<Vec<Sender<RESP3>>> {
        self.pub_sub.get(topic).map(|listener| listener.clone())
    }

    // 向频道添加一个监听者
    #[instrument(level = "debug", skip(self, listener))]
    pub fn add_channel_listener(&self, topic: Key, listener: Sender<RESP3>) {
        self.pub_sub.entry(topic).or_default().push(listener);
    }

    // 移除频道的一个监听者
    #[instrument(level = "debug", skip(self, listener))]
    pub fn remove_channel_listener(
        &self,
        topic: &[u8],
        listener: &Sender<RESP3>,
    ) -> Option<Sender<RESP3>> {
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

impl Default for Db {
    fn default() -> Self {
        Self {
            entries: DashMap::with_capacity_and_hasher_and_shard_amount(
                1024 * 16,
                RandomState::new(),
                64, // TODO: 根据max_memory配置
            ),
            entry_expire_records: DashSet::with_capacity_and_hasher(512, RandomState::new()),
            pub_sub: DashMap::with_capacity_and_hasher(8, RandomState::new()),
            client_records: DashMap::with_capacity_and_hasher(1024, RandomState::new()),
        }
    }
}

#[cfg(test)]
pub mod db_tests {
    use super::*;
    use crate::util::test_init;

    pub async fn get_object_inner(db: &Db, key: &Key) -> Option<ObjectInner> {
        // 键存在
        if let Some(e) = db.entries.get(key) {
            // 对象不为空
            if let Some(inner) = e.value().inner().await {
                // 对象未过期
                if !inner.is_expired() {
                    return Some(inner.clone());
                }

                // 对象已过期，移除该键值对
                drop(e);
                db.remove_object(key).await;
            }
        }

        None
    }

    #[tokio::test]
    async fn insert_object_test() {
        test_init();

        let db = Db::default();

        // 无对象时插入对象
        db.insert_object("key1".into(), ObjectInner::new_str("value1".into(), None))
            .await;
        let res = get_object_inner(&db, &"key1".into())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .to_bytes();
        assert_eq!(res, "value1".as_bytes());

        // 有对象时插入对象，触发旧对象的Update事件
        let (tx, rx) = flume::unbounded();
        db.add_event("key1".into(), tx.clone(), EventType::MayUpdate)
            .await;

        db.insert_object("key1".into(), ObjectInner::new_str("value2".into(), None))
            .await;
        let res = get_object_inner(&db, &"key1".into())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .to_bytes();
        assert_eq!(res, "value2".as_bytes());

        let res = rx.recv().unwrap();
        assert_eq!(res.try_bulk().unwrap().as_ref(), b"key1");

        // 存在空对象时插入对象，触发Update事件
        db.add_event("key2".into(), tx.clone(), EventType::MayUpdate)
            .await;

        db.insert_object("key2".into(), ObjectInner::new_str("value2".into(), None))
            .await;
        let res = get_object_inner(&db, &"key1".into())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .to_bytes();
        assert_eq!(res, "value2".as_bytes());

        let res = rx.recv().unwrap();
        assert_eq!(res.try_bulk().unwrap().as_ref(), b"key2");
    }

    #[tokio::test]
    async fn visit_object_test() {
        test_init();

        let db = Db::default();

        db.insert_object("key1".into(), ObjectInner::new_str("value1".into(), None))
            .await;

        // 访问有效对象，应该成功
        db.visit_object(&"key1".into(), |_| Ok(())).await.unwrap();

        // 访问不存在的对象，应该失败
        db.visit_object(&"key_not_exist".into(), |_| Ok(()))
            .await
            .unwrap_err();

        // 访问空对象时，应该失败
        let (tx, _) = flume::unbounded();
        db.add_event("key_none".into(), tx, EventType::MayUpdate)
            .await; // 这会创建一个空对象
        db.visit_object(&"key_none".into(), |_| Ok(()))
            .await
            .unwrap_err();

        // 访问过期对象时，应该失败，并且过期对象会被删除
        db.insert_object(
            "key_expired".into(),
            ObjectInner::new_str("key_expired".into(), Some(Instant::now())),
        )
        .await;
        db.visit_object(&"key_expired".into(), |_| Ok(()))
            .await
            .unwrap_err();
        assert!(get_object_inner(&db, &"key_expired".into()).await.is_none());
    }

    #[tokio::test]
    async fn update_object_test() {
        test_init();

        let db = Db::default();
        let (tx, rx) = flume::unbounded();

        db.insert_object("key1".into(), ObjectInner::new_str("value1".into(), None))
            .await;

        // 更新有效对象，应该成功，并且触发Update事件
        db.add_event("key1".into(), tx.clone(), EventType::MayUpdate)
            .await;

        db.update_object(&"key1".into(), |obj| {
            obj.on_str_mut().unwrap().set("value2".into());
            Ok(())
        })
        .await
        .unwrap();

        let update_res = get_object_inner(&db, &"key1".into())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .to_bytes();
        assert_eq!(update_res, "value2".as_bytes());

        let event_res = rx.recv().unwrap();
        assert_eq!(event_res.try_bulk().unwrap().as_ref(), b"key1");

        // 更新不存在的对象，应该失败
        db.update_object(&"key_not_exist".into(), |_| Ok(()))
            .await
            .unwrap_err();

        // 更新空对象时，应该失败
        db.add_event("key_none".into(), tx, EventType::MayUpdate)
            .await; // 这会创建一个空对象
        db.update_object(&"key_none".into(), |_| Ok(()))
            .await
            .unwrap_err();

        // 更新过期对象时，应该失败，并且过期对象会被删除
        db.insert_object(
            "key_expired".into(),
            ObjectInner::new_str("value".into(), Some(Instant::now())),
        )
        .await;
        db.update_object(&"key_expired".into(), |_| Ok(()))
            .await
            .unwrap_err();
        assert!(get_object_inner(&db, &"key_expired".into()).await.is_none());
    }

    #[tokio::test]
    async fn update_or_create_object_test() {
        test_init();

        let db = Db::default();
        let (tx, rx) = flume::unbounded();

        db.insert_object("key1".into(), ObjectInner::new_str("value1".into(), None))
            .await;

        // 更新或创建，更新有效象应该成功，并且触发Update事件
        db.add_event("key1".into(), tx.clone(), EventType::MayUpdate)
            .await;

        db.update_or_create_object(&"key1".into(), ObjValueType::Str, |obj| {
            obj.on_str_mut().unwrap().set("value2".into());
            Ok(())
        })
        .await
        .unwrap();

        let update_res = get_object_inner(&db, &"key1".into())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .to_bytes();
        assert_eq!(update_res, "value2".as_bytes());

        let event_res = rx.recv().unwrap();
        assert_eq!(event_res.try_bulk().unwrap().as_ref(), b"key1");

        // 更新或创建，更新不存在的对象，应该创建新对象
        db.update_or_create_object(&"key_not_exist".into(), ObjValueType::Str, |obj| {
            obj.on_str_mut().unwrap().set("value".into());
            Ok(())
        })
        .await
        .unwrap();

        let update_res = get_object_inner(&db, &"key_not_exist".into())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .to_bytes();
        assert_eq!(update_res, "value".as_bytes());

        // 更新或创建，更新空对象，应该创建新对象并触发空对象的Update事件
        db.add_event("key_none".into(), tx, EventType::MayUpdate)
            .await; // 这会创建一个空对象
        db.update_or_create_object(&"key_none".into(), ObjValueType::Str, |obj| {
            obj.on_str_mut().unwrap().set("value".into());
            Ok(())
        })
        .await
        .unwrap();

        let update_res = get_object_inner(&db, &"key_none".into())
            .await
            .unwrap()
            .on_str()
            .unwrap()
            .to_bytes();
        assert_eq!(update_res, "value".as_bytes());

        let event_res = rx.recv().unwrap();
        assert_eq!(event_res.try_bulk().unwrap().as_ref(), b"key_none");
    }

    // #[tokio::test]
    // async fn remove_object_test() {
    //     test_init();
    //
    //     let db = Db::default();
    //     let (tx, rx) = flume::unbounded();
    //
    //     db.insert_object("key1".into(), ObjectInner::new_str("value1".into(), None));
    //
    //     // 移除有效对象，应该成功，并且触发Remove事件
    //     db.add_event("key1".into(), tx.clone(), EventType::Remove);
    //
    //     db.remove_object(&"key1".into()).unwrap();
    //     get_object(&db, "key1".as_bytes()).unwrap_err();
    //
    //     let event_res = rx.recv().unwrap();
    //     assert_eq!(event_res.try_bulk().unwrap().as_ref(), b"key1");
    //
    //     // 移除不存在的对象，应该返回None
    //     assert!(db.remove_object(&"key_not_exist".into()).is_none());
    // }
}
