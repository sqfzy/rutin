use crate::{
    error::{RutinError, RutinResult},
    server::ID,
    shared::db::{
        object_entry::{StaticEntryRef, StaticOccupiedEntryRef},
        Key, Object, ObjectValue,
    },
    Id,
};
use ahash::RandomState;
use dashmap::{
    mapref::{
        entry_ref::EntryRef,
        one::{Ref, RefMut},
    },
    DashMap,
};
use std::{
    any::{Any, TypeId},
    fmt::{self, Debug, Formatter},
    hash::Hash,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::{sync::Notify, time::Instant};

const READ_EVENT_FLAG: u8 = 0b0000_0001;
const WRITE_EVENT_FLAG: u8 = 0b0000_0010;
const LOCK_EVENT_FLAG: u8 = 0b0000_0100;

#[derive(Default, Debug)]
pub struct Events {
    // 使用tinyvec
    inner: Vec<Event>,
    // 是否存在读事件，是否存在写事件，是否存在锁事件
    flags: AtomicU8,
}

impl Events {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn add_read_event(&mut self, event: ReadEvent) {
        self.inner.push(Event::Read(event));
        let flags = self.flags.get_mut();
        *flags |= READ_EVENT_FLAG;
    }

    pub fn add_write_event(&mut self, event: WriteEvent) {
        self.inner.push(Event::Write(event));
        let flags = self.flags.get_mut();
        *flags |= WRITE_EVENT_FLAG;
    }

    pub fn add_lock_event(&mut self) {
        let Events { inner, flags } = self;
        let flags = flags.get_mut();

        if *flags & LOCK_EVENT_FLAG == 0 {
            // 不存在锁事件则直接添加
            inner.insert(
                0,
                Event::IntentionLock {
                    target_id: ID.get(),
                    notify: Arc::new(Notify::new()),
                    count: AtomicUsize::new(0),
                },
            );

            *flags |= LOCK_EVENT_FLAG;
            return;
        }

        // 存在锁事件则覆盖target_id
        if let Some(Event::IntentionLock { target_id, .. }) = inner.first_mut() {
            *target_id = ID.get();
            *flags |= LOCK_EVENT_FLAG;
        } else {
            unreachable!()
        }
    }

    #[inline(always)]
    pub fn try_trigger_read_event(object: &Object) {
        let flag = object.events.flags.load(Ordering::Relaxed);
        if flag & READ_EVENT_FLAG == 0 {
            return;
        }

        #[inline]
        fn trigger_read_event(object: &Object) {
            // 如果还有读事件没有完成则不应移除flag中的标记
            let mut should_remove_read_flag = true;

            let Object {
                value,
                expire,
                events: Events { inner: events, .. },
                ..
            } = object;

            // 遍历找到所有的读事件并执行
            for event in events.iter() {
                match event {
                    // 取出FnOnce，如果未超时则执行，如果超时则仅移除事件而不执行
                    Event::Read(ReadEvent::FnOnce { deadline, callback }) => {
                        if let Ok(mut guard) = callback.lock()
                            && let Some(c) = guard.take()
                            && Instant::now() < *deadline
                        {
                            c(value, *expire).ok();
                        }
                    }
                    // 取出FnMut，如果未超时则执行，如果超时则仅移除事件而不执行
                    Event::Read(ReadEvent::FnMut { deadline, callback }) => {
                        if let Ok(mut guard) = callback.lock()
                            && let Some(mut c) = guard.take()
                            && Instant::now() < *deadline
                            && c(value, *expire).is_err()
                        {
                            should_remove_read_flag = false;
                            guard.replace(c);
                        }
                    }
                    Event::Write(_) => {}
                    Event::IntentionLock { .. } => {}
                }
            }

            if should_remove_read_flag {
                object
                    .events
                    .flags
                    .fetch_and(!READ_EVENT_FLAG, Ordering::Relaxed);
            }
        }

        trigger_read_event(object)
    }

    #[inline(always)]
    pub fn try_trigger_read_and_write_event(object: &mut Object) {
        let flags = object.events.flags.get_mut();
        if *flags & (WRITE_EVENT_FLAG | READ_EVENT_FLAG) == 0 {
            return;
        }

        #[inline]
        fn trigger_read_and_write_event(object: &mut Object) {
            // 如果还有写事件没有完成则不应清除flag中的标记
            let mut should_remove_write_flag = true;
            let mut should_remove_read_flag = true;

            let mut index = 0;

            loop {
                let Object {
                    value,
                    expire,
                    events: Events { inner: events, .. },
                    ..
                } = object;

                if index >= events.len() {
                    break;
                }

                match &mut events[index] {
                    Event::Write(WriteEvent::FnOnce { .. }) => {
                        if let Event::Write(WriteEvent::FnOnce { deadline, callback }) =
                            events.swap_remove(index)
                        {
                            if Instant::now() < deadline {
                                callback(value, *expire).ok();
                            }
                        } else {
                            unreachable!()
                        }
                    }
                    Event::Write(WriteEvent::FnMut {
                        deadline,
                        ref mut callback,
                    }) => {
                        if Instant::now() < *deadline && callback(value, *expire).is_err() {
                            should_remove_write_flag = false;
                            index += 1;
                            continue;
                        }
                        // 事件超时或者事件已完成

                        events.swap_remove(index);
                    }
                    Event::Read(ReadEvent::FnOnce { .. }) => {
                        if let Event::Read(ReadEvent::FnOnce { deadline, callback }) =
                            events.swap_remove(index)
                        {
                            if let Ok(Some(c)) = callback.into_inner()
                                && Instant::now() < deadline
                            {
                                c(value, *expire).ok();
                            }
                        } else {
                            unreachable!()
                        }
                        // if let Ok(Some(c)) = callback.into_inner()
                        //     && Instant::now() < *deadline
                        // {
                        //     c(object).ok();
                        // }
                    }
                    Event::Read(ReadEvent::FnMut {
                        deadline,
                        ref mut callback,
                    }) => {
                        if Instant::now() < *deadline
                            && let Ok(Some(c)) = callback.get_mut()
                            && c(value, *expire).is_err()
                        {
                            should_remove_read_flag = false;
                            index += 1;
                            continue;
                        }
                        // 事件超时或者事件已完成

                        events.swap_remove(index);
                        //
                        // if let Ok(Some(c)) = callback.get_mut()
                        //     && Instant::now() < deadline
                        //     && c(object).is_err()
                        // {
                        //     object
                        //         .events
                        //         .inner
                        //         .push(Event::Read(ReadEvent::FnMut { deadline, callback }));
                        //     should_remove_read_flag = false;
                        // }
                    }

                    Event::IntentionLock { .. } => index += 1,
                }
            }

            // while let Some(event) = object
            //     .events
            //     .inner
            //     .pop_if(|e| !matches!(e, Event::IntentionLock { .. }))
            // {
            //     match event {
            //         Event::Write(WriteEvent::FnOnce { deadline, callback }) => {
            //             if Instant::now() < deadline {
            //                 callback(object).ok();
            //             }
            //         }
            //         Event::Write(WriteEvent::FnMut {
            //             deadline,
            //             mut callback,
            //         }) => {
            //             if Instant::now() < deadline && callback(object).is_err() {
            //                 object
            //                     .events
            //                     .inner
            //                     .push(Event::Write(WriteEvent::FnMut { deadline, callback }));
            //                 should_remove_write_flag = false;
            //             }
            //         }
            //         Event::Read(ReadEvent::FnOnce { deadline, callback }) => {
            //             if let Ok(Some(c)) = callback.into_inner()
            //                 && Instant::now() < deadline
            //             {
            //                 c(object).ok();
            //             }
            //         }
            //         Event::Read(ReadEvent::FnMut {
            //             deadline,
            //             mut callback,
            //         }) => {
            //             if let Ok(Some(c)) = callback.get_mut()
            //                 && Instant::now() < deadline
            //                 && c(object).is_err()
            //             {
            //                 object
            //                     .events
            //                     .inner
            //                     .push(Event::Read(ReadEvent::FnMut { deadline, callback }));
            //                 should_remove_read_flag = false;
            //             }
            //         }
            //
            //         Event::IntentionLock { .. } => unreachable!(),
            //     }
            // }

            if should_remove_write_flag {
                *object.events.flags.get_mut() &= !WRITE_EVENT_FLAG;
            }

            if should_remove_read_flag {
                *object.events.flags.get_mut() &= !READ_EVENT_FLAG;
            }
        }

        trigger_read_and_write_event(object)
    }

    // 返回的Ref不保证是合法的
    // 唤醒时，键值对可能已被移除(因为该键值对已过期)，此时返回Err
    #[inline(always)]
    pub async fn try_trigger_lock_event1<'a>(
        entry: Ref<'a, Key, Object>,
        entries: &'a DashMap<Key, Object, RandomState>,
    ) -> RutinResult<Ref<'a, Key, Object>> {
        let flag = entry.events.flags.load(Ordering::Relaxed);
        if flag & LOCK_EVENT_FLAG == 0 {
            return Ok(entry);
        }

        #[inline]
        async fn trigger_lock_event<'a>(
            entry: Ref<'a, Key, Object>,
            entries: &'a DashMap<Key, Object, RandomState>,
        ) -> RutinResult<Ref<'a, Key, Object>> {
            // 遍历找到所有的锁事件并执行

            if let Some(Event::IntentionLock {
                target_id,
                notify,
                count,
            }) = entry.events.inner.first()
            {
                if *target_id == ID.get() {
                    notify.notify_one();
                    return Ok(entry);
                }

                let seq = count.fetch_add(1, Ordering::Relaxed) + 1;

                let key = entry.key().clone();
                let notify = notify.clone();

                // 释放锁
                drop(entry);

                // 等待
                notify.clone().notified().await;

                // 重新获取锁，如果键值对已被移除(因为该键值对已过期)则返回Err
                let mut new_entry = entries.get_mut(&key).ok_or_else(|| RutinError::Null)?;
                let events = &mut new_entry.events;

                // 最后一个等待者负责移除锁事件
                if let Some(Event::IntentionLock { count, .. }) = events.inner.first_mut()
                    && *count.get_mut() == seq
                {
                    events.inner.swap_remove(0);
                    let flags = events.flags.get_mut();
                    *flags &= !LOCK_EVENT_FLAG;
                }

                // 唤醒下一个等待者
                notify.notify_one();

                return Ok(new_entry.downgrade());
            }

            unreachable!()
        }

        trigger_lock_event(entry, entries).await
    }

    #[inline(always)]
    pub async fn try_trigger_lock_event2<'a>(
        mut entry: RefMut<'a, Key, Object>,
        entries: &'a DashMap<Key, Object, RandomState>,
    ) -> RutinResult<RefMut<'a, Key, Object>> {
        let flags = entry.events.flags.get_mut();
        if *flags & LOCK_EVENT_FLAG == 0 {
            return Ok(entry);
        }

        #[inline]
        async fn trigger_lock_event<'a>(
            mut entry: RefMut<'a, Key, Object>,
            entries: &'a DashMap<Key, Object, RandomState>,
        ) -> RutinResult<RefMut<'a, Key, Object>> {
            let key = entry.key().clone();

            // 锁事件一定在第一个
            if let Some(Event::IntentionLock {
                target_id,
                notify,
                count,
            }) = entry.events.inner.first_mut()
            {
                if *target_id == ID.get() {
                    notify.notify_one();
                    return Ok(entry);
                }

                let count = count.get_mut();
                *count += 1;
                let seq = *count;

                let notify = notify.clone();

                // 释放锁
                drop(entry);

                // 等待
                notify.notified().await;

                let mut new_entry = entries.get_mut(&key).ok_or_else(|| RutinError::Null)?;
                let events = &mut new_entry.events;

                // 最后一个等待者负责移除锁事件
                if let Some(Event::IntentionLock { count, .. }) = events.inner.first_mut()
                    && *count.get_mut() == seq
                {
                    events.inner.swap_remove(0);
                    let flags = events.flags.get_mut();
                    *flags &= !LOCK_EVENT_FLAG;
                }

                // 唤醒下一个等待者
                notify.notify_one();

                return Ok(new_entry);
            }

            unreachable!()
        }

        trigger_lock_event(entry, entries).await
    }

    #[inline(always)]
    pub async fn try_trigger_lock_event3<'a, 'b, Q>(
        key: &'b Q,
        mut entry: StaticOccupiedEntryRef<'a>,
        entries: &'a DashMap<Key, Object, RandomState>,
    ) -> StaticEntryRef<'a, 'b, Q>
    where
        Q: Hash + equivalent::Equivalent<Key> + ?Sized,
    {
        let flags = entry.get_mut().events.flags.get_mut();
        if *flags & LOCK_EVENT_FLAG == 0 {
            return StaticEntryRef::Occupied(entry);
        }

        #[inline]
        async fn trigger_lock_event<'a, 'b, Q>(
            key: &'b Q,
            mut entry: StaticOccupiedEntryRef<'a>,
            entries: &'a DashMap<Key, Object, RandomState>,
        ) -> StaticEntryRef<'a, 'b, Q>
        where
            Q: Hash + equivalent::Equivalent<Key> + ?Sized,
        {
            // 锁事件一定在第一个
            if let Some(Event::IntentionLock {
                target_id,
                notify,
                count,
            }) = entry.get_mut().events.inner.first_mut()
            {
                if *target_id == ID.get() {
                    notify.notify_one();
                    return StaticEntryRef::Occupied(entry);
                }

                let count = count.get_mut();
                *count += 1;
                let seq = *count;

                let notify = notify.clone();

                // 释放锁
                drop(entry);

                // 等待
                notify.notified().await;

                let mut new_entry = entries.entry_ref(key);

                if let EntryRef::Occupied(e) = &mut new_entry {
                    let events = &mut e.get_mut().events;

                    // 最后一个等待者负责移除锁事件
                    if let Some(Event::IntentionLock { count, .. }) = events.inner.first_mut()
                        && *count.get_mut() == seq
                    {
                        events.inner.swap_remove(0);
                        let flags = events.flags.get_mut();
                        *flags &= !LOCK_EVENT_FLAG;
                    }
                }

                // 唤醒下一个等待者
                notify.notify_one();

                return new_entry;
            }

            unreachable!()
        }

        trigger_lock_event(key, entry, entries).await
    }
}

// impl Debug for Events {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         f.debug_struct("Events")
//             .field("inner", &self.inner)
//             .field("flags", &self.flags)
//             .finish()
//     }
// }

// 用户自定义事件时：
//  可以通过outbox.is_disconnected()判断客户端是否已经断开连接
#[derive(Debug)]
pub enum Event {
    ///  读事件完成后会设为None并设置flag，已完成的事件不会被执行
    Read(ReadEvent),

    /// 写事件完成后会直接移除事件
    ///
    /// 回收已完成的读事件：回收操作必须及时执行，否则会导致内存浪费。另外，回收操作
    /// 也应当尽可能高效。由于添加事件必须要获取写锁，获取写锁必须先尝试触发写事件，
    /// 所以我们可以在尝试触发写事件时，同时尝试回收已完成的读事件。这样，在执行回收
    /// 操作之前，不会有新的读事件被添加并且在探测读事件是否已完成时也不必上锁(值本身
    /// 是可变的)。
    Write(WriteEvent),

    /// 锁事件保证锁存在期间目标协程可以第一个获得锁
    ///
    /// 锁事件是唯一的，重复添加锁事件只会覆盖target_id
    ///
    /// 最后一个等待者负责移除锁事件
    ///
    /// 锁事件的唤醒是有序的
    IntentionLock {
        target_id: Id,
        notify: Arc<Notify>,
        count: AtomicUsize,
    },
}

impl Event {
    pub fn event_flag(&self) -> u8 {
        match self {
            Self::Read(_) => READ_EVENT_FLAG,
            Self::Write(_) => WRITE_EVENT_FLAG,
            Self::IntentionLock { .. } => LOCK_EVENT_FLAG,
        }
    }

    // TODO:
    pub fn event_type_id(&self) -> TypeId {
        match self {
            Self::Read(e) => e.type_id(),
            _ => {
                todo!()
            }
        }
    }
}

#[allow(clippy::type_complexity)]
pub enum ReadEvent {
    // 返回Err则终止执行
    FnOnce {
        deadline: Instant,
        callback: Mutex<Option<Box<dyn FnOnce(&ObjectValue, Instant) -> RutinResult<()>>>>,
    },

    // 返回Err则终止并等待下一次执行
    FnMut {
        deadline: Instant,
        callback: Mutex<Option<Box<dyn FnMut(&ObjectValue, Instant) -> RutinResult<()>>>>,
    },
}

impl Debug for ReadEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ReadEvent::FnOnce { .. } => write!(f, "ReadFnOnceTimeOut"),
            ReadEvent::FnMut { .. } => write!(f, "ReadFnMutTimeOut"),
        }
    }
}

#[allow(clippy::type_complexity)]
pub enum WriteEvent {
    // 返回Err则终止执行
    FnOnce {
        deadline: Instant,
        callback: Box<dyn FnOnce(&mut ObjectValue, Instant) -> RutinResult<()>>,
    },

    // 返回Err则终止并等待下一次执行
    FnMut {
        deadline: Instant,
        callback: Box<dyn FnMut(&mut ObjectValue, Instant) -> RutinResult<()>>,
    },
}

impl Debug for WriteEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            WriteEvent::FnOnce { .. } => write!(f, "WriteFnOnceTimeOut"),
            WriteEvent::FnMut { .. } => write!(f, "WriteFnMutTimeOut"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{server::NEVER_EXPIRE, shared::db::Str};
    use std::sync::atomic::AtomicBool;

    #[test]
    fn add_event() {
        let mut obj = Object::new(Str::from("foo"));

        let yes = Arc::new(AtomicBool::new(false));
        obj.events.add_read_event(ReadEvent::FnOnce {
            deadline: *NEVER_EXPIRE,
            callback: Mutex::new(Some(Box::new({
                let yes = yes.clone();
                move |_, _| {
                    yes.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }))),
        });

        assert_eq!(obj.events.len(), 1);

        let count = Arc::new(AtomicUsize::new(0));
        obj.events.add_read_event(ReadEvent::FnMut {
            deadline: *NEVER_EXPIRE,
            callback: Mutex::new(Some(Box::new({
                let count = count.clone();
                move |_, _| {
                    count.fetch_add(1, Ordering::SeqCst);
                    if count.load(Ordering::SeqCst) == 2 {
                        Ok(())
                    } else {
                        Err(RutinError::Null)
                    }
                }
            }))),
        });

        assert_eq!(obj.events.len(), 2);

        Events::try_trigger_read_event(&obj);
        assert!(yes.load(Ordering::SeqCst));
        assert_eq!(count.load(Ordering::SeqCst), 1);

        Events::try_trigger_read_event(&obj);
        assert_eq!(count.load(Ordering::SeqCst), 2);

        let yes = Arc::new(AtomicBool::new(false));
        obj.events.add_write_event(WriteEvent::FnOnce {
            deadline: *NEVER_EXPIRE,
            callback: Box::new({
                let yes = yes.clone();
                move |_, _| {
                    yes.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }),
        });

        assert_eq!(obj.events.len(), 3);

        let count = Arc::new(AtomicUsize::new(0));
        obj.events.add_write_event(WriteEvent::FnMut {
            deadline: *NEVER_EXPIRE,
            callback: Box::new({
                let count = count.clone();
                move |_, _| {
                    count.fetch_add(1, Ordering::SeqCst);
                    if count.load(Ordering::SeqCst) == 2 {
                        Ok(())
                    } else {
                        Err(RutinError::Null)
                    }
                }
            }),
        });

        assert_eq!(obj.events.len(), 4);

        Events::try_trigger_read_and_write_event(&mut obj);
        assert_eq!(obj.events.len(), 1);
        assert!(yes.load(Ordering::SeqCst));
        assert_eq!(count.load(Ordering::SeqCst), 1);

        Events::try_trigger_read_and_write_event(&mut obj);
        assert_eq!(obj.events.len(), 0);
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}
