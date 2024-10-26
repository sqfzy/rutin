use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{AsyncStream, Handler, NEVER_EXPIRE},
    shared::{
        db::{
            str_as_bytes, Key, List, ObjectValue,
            WriteEvent::{self},
        },
        Letter,
    },
    util::atoi,
    Int,
};
use futures::FutureExt;
use itertools::Itertools;
use std::time::Duration;
use tokio::time::{sleep_until, Instant};
use tracing::instrument;

/// # Reply:
///
/// **Bulk string reply:** the element being popped from the source and pushed to the destination.
/// **Null reply:** the operation timed-out
#[derive(Debug)]
pub struct BLMove<A> {
    source: A,
    destination: A,
    wherefrom: Where,
    whereto: Where,
    timeout: u64,
}

impl<A> CmdExecutor<A> for BLMove<A>
where
    A: CmdArg,
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
        let Handler {
            shared, context, ..
        } = handler;
        let db = shared.db();

        let mut source_entry = db.object_entry(&self.source).await?;
        let mut elem = None;

        // 1. 尝试弹出元素
        if source_entry.is_occupied() {
            source_entry = source_entry.update1(|obj| {
                let list = obj.on_list_mut()?;
                elem = match self.wherefrom {
                    Where::Left => list.pop_front(),
                    Where::Right => list.pop_back(),
                };

                Ok(())
            })?;
        }

        // 2. 如果存在弹出元素，推入元素后直接返回
        if let Some(elem) = elem {
            let res = Resp3::new_blob_string(elem.to_bytes());

            db.update_object_force(
                &self.destination,
                || List::default().into(),
                |obj| {
                    let list = obj.on_list_mut()?;
                    match self.whereto {
                        Where::Left => list.push_front(elem),
                        Where::Right => list.push_back(elem),
                    }

                    Ok(())
                },
            )
            .await?;
            // 成功推入元素

            return Ok(Some(res));
        }

        // 3. 如果不存在弹出元素，加入监听事件

        let deadline = if self.timeout == 0 {
            *NEVER_EXPIRE
        } else {
            Instant::now() + Duration::from_secs(self.timeout)
        };

        let (tx, rx) = flume::bounded(1);

        // source_entry的回调事件，如果有timeout则超时后会移除事件，如果客户端断开连接也会移除事件
        let callback = {
            let mut shutdown = context.shutdown.listen();

            move |obj: &mut ObjectValue, _ex: Instant| -> RutinResult<()> {
                // 客户端已经断开连接则事件结束并中断超时任务
                if (&mut shutdown).now_or_never().is_some() {
                    return Ok(());
                }

                let list = obj.on_list_mut()?;
                let elem = match self.wherefrom {
                    Where::Left => list.pop_front(),
                    Where::Right => list.pop_back(),
                };

                if let Some(elem) = elem {
                    // 发送成功则事件结束，发送失败则证明已经超时，事件也结束
                    tx.send(elem).ok();
                    Ok(())
                } else {
                    Err(RutinError::Null)
                }
            }
        };

        source_entry.add_write_event_force(
            || List::default().into(),
            WriteEvent::FnMut {
                deadline,
                callback: Box::new(callback),
            },
        );

        // 4. 处理对客户端的响应

        let shutdown = context.shutdown.listen();
        let outbox = context.mailbox.outbox.clone();
        let dst = self.destination.into_bytes();
        let shared = *shared;

        shared.pool().spawn_pinned(move || async move {
            tokio::select! {
                _ = shutdown => (),
                // 超时，返回Null
                _ = sleep_until(deadline), if deadline != *NEVER_EXPIRE => {
                    outbox.send(Letter::Resp3(Resp3::Null)).ok();
                },
                elem = rx.recv_async() => {
                    let elem = if let Ok(e) = elem {
                        e
                    } else {
                        outbox.send(Letter::Resp3(Resp3::Null)).ok();
                        return;
                    };

                    let res = shared.db().update_object_force::<Bytes>(&dst,|| List::default().into(), |obj| {
                        let list = obj.on_list_mut()?;
                        match self.whereto {
                            Where::Left => list.push_front(elem.clone()),
                            Where::Right => list.push_back(elem.clone()),
                        }

                        Ok(())
                    })
                    .await;

                    match res {
                        Ok(()) => outbox.send(Letter::Resp3(Resp3::new_blob_string(elem.to_bytes()))).ok(),
                        Err(e) => outbox.send(Letter::Resp3(e.try_into().unwrap())).ok()
                    };
                }
            };
        });

        Ok(None)
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 5 {
            return Err(RutinError::WrongArgNum);
        }

        let source = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&source, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let destination = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&destination, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(BLMove {
            source,
            destination,
            wherefrom: Where::try_from(args.next_back().unwrap().into_uppercase::<8>().as_ref())?,
            whereto: Where::try_from(args.next_back().unwrap().into_uppercase::<8>().as_ref())?,
            timeout: atoi::<u64>(args.next_back().unwrap().as_ref())?,
        })
    }
}

/// 从多个列表中的首个非空列表弹出一个元素
/// # Reply:
///
/// **Null reply:** no element could be popped and the timeout expired
/// **Array reply:** the key from which the element was popped and the value of the popped element.
#[derive(Debug)]
pub struct BLPop<A> {
    keys: Vec<A>,
    timeout: u64,
}

impl<A> CmdExecutor<A> for BLPop<A>
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        mut self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let Handler {
            shared, context, ..
        } = handler;
        let db = shared.db();

        for (i, key) in self.keys.iter().enumerate() {
            let source_entry = db.object_entry(key).await?;
            let mut elem = None;

            if source_entry.is_occupied() {
                source_entry.update1(|obj| {
                    let list = obj.on_list_mut()?;
                    elem = list.pop_front();

                    Ok(())
                })?;
            }

            if let Some(elem) = elem {
                return Ok(Some(CheapResp3::new_array(vec![
                    Resp3::new_blob_string(self.keys.swap_remove(i).into_bytes()),
                    Resp3::new_blob_string(elem.to_bytes()),
                ])));
            }
        }

        // 所有列表都没有元素，加入监听事件

        let deadline = if self.timeout == 0 {
            *NEVER_EXPIRE
        } else {
            Instant::now() + Duration::from_secs(self.timeout)
        };

        let keys: Vec<Key> = self.keys.into_iter().map(|k| k.into()).collect_vec();

        let (tx, rx) = flume::bounded(1);

        // 添加多个键的监听事件，接收者只会接收到第一个非空列表的元素
        for key in keys {
            let callback = {
                let tx = tx.clone();
                let mut shutdown = context.shutdown.listen();
                let key = key.clone();

                move |obj: &mut ObjectValue, _ex: Instant| -> RutinResult<()> {
                    if (&mut shutdown).now_or_never().is_some() {
                        return Ok(());
                    }

                    let list = obj.on_list_mut()?;
                    let elem = list.pop_front();

                    if let Some(elem) = elem {
                        tx.send(CheapResp3::new_array(vec![
                            Resp3::new_blob_string(key.clone()),
                            Resp3::new_blob_string(elem.to_bytes()),
                        ]))
                        .ok();
                        Ok(())
                    } else {
                        Err(RutinError::Null)
                    }
                }
            };

            db.object_entry(&key).await?.add_write_event_force(
                || List::default().into(),
                WriteEvent::FnMut {
                    deadline,
                    callback: Box::new(callback),
                },
            );
        }

        let shutdown = context.shutdown.listen();
        let outbox = context.mailbox.outbox.clone();

        shared.pool().spawn_pinned(move || async move {
            tokio::select! {
                _ = shutdown => {},
                // 超时，返回Null
                _ = sleep_until(deadline), if deadline != *NEVER_EXPIRE => {
                    outbox.send(Letter::Resp3(Resp3::Null)).ok();
                },
                // 接受并处理首个消息
                msg = rx.recv_async() => {
                    if let Ok(msg) = msg {
                        outbox.send(Letter::Resp3(msg)).ok();
                    } else {
                        outbox.send(Letter::Resp3(Resp3::Null)).ok();
                    }
                }
            };
        });

        Ok(None)
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 {
            return Err(RutinError::WrongArgNum);
        }

        let timeout = atoi::<u64>(args.next_back().unwrap().as_ref())?;

        let keys = args
            .map(|k| {
                if ac.deny_reading_or_writing_key(&k, Self::CATS_FLAG) {
                    return Err(RutinError::NoPermission);
                }
                Ok(k)
            })
            .try_collect()?;

        Ok(Self { keys, timeout })
    }
}

/// # Reply:
///
/// **Null reply:** if there is no matching element.
/// **Integer reply:** an integer representing the matching element.
/// **Array reply:** If the COUNT option is given, an array of integers representing the matching elements (or an empty array if there are no matches).
#[derive(Debug)]
pub struct LPos<A> {
    key: A,
    element: A,
    rank: Int,
    count: usize,
    max_len: Option<usize>,
}

impl<A> CmdExecutor<A> for LPos<A>
where
    A: CmdArg,
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
        // 找到一个匹配元素，则rank-1(或+1)，当rank为0时，则表明开始收入
        // 一共要收入count个，但最长只能找max_len个元素
        // 如果count == 1，返回Integer
        // 如果count > 1 || count ==0，返回Array
        // 如果没有匹配的，返回Null
        let mut rank = self.rank;
        let count = self.count;

        let mut res = if count == usize::MAX {
            Vec::with_capacity(8)
        } else {
            Vec::with_capacity(count)
        };

        handler
            .shared
            .db()
            .visit_object(self.key.as_ref(), |obj| {
                let list = obj.on_list()?;
                if rank >= 0 {
                    for i in 0..self.max_len.unwrap_or(list.len()) {
                        if self.element.as_ref() != str_as_bytes!(list[i]) {
                            continue;
                        }
                        // 只有当rank减为0时，才开始收入元素
                        if rank > 1 {
                            rank -= 1;
                            continue;
                        }

                        res.push(Resp3::new_integer(i as Int));
                        if res.len() == count {
                            return Ok(());
                        }
                    }
                } else {
                    let list_len = list.len();
                    let lower_bound = list_len.saturating_sub(self.max_len.unwrap_or(list_len));
                    let upper_bound = list_len - 1;
                    for i in (lower_bound..=upper_bound).rev() {
                        if self.element.as_ref() != list[i].to_bytes() {
                            continue;
                        }
                        // 只有当rank增为0时，才开始收入元素
                        if rank > 1 {
                            rank += 1;
                            continue;
                        }

                        res.push(Resp3::new_integer(i as Int));
                        if res.len() == count {
                            return Ok(());
                        }
                    }
                }

                Ok(())
            })
            .await?;

        let count = res.len();
        let res = if count == 0 {
            Resp3::Null
        } else if count == 1 {
            res.pop().unwrap()
        } else {
            Resp3::new_array(res)
        };

        Ok(Some(res))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if !(args.len() == 2 || args.len() == 4 || args.len() == 6 || args.len() == 8) {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let element = args.next().unwrap();

        let mut rank = 1;
        let mut count = 1;
        let mut max_len = None;

        while let Some(opt) = args.next() {
            let len = opt.as_ref().len();
            if len > 6 {
                return Err("ERR invalid option is given".into());
            }

            match opt.into_uppercase::<8>().as_ref() {
                b"RANK" => rank = atoi::<Int>(args.next().unwrap().as_ref())?,
                b"COUNT" => count = atoi::<usize>(args.next().unwrap().as_ref())?,
                b"MAXLEN" => max_len = Some(atoi::<usize>(args.next().unwrap().as_ref())?),
                _ => return Err("ERR invalid option is given".into()),
            }
        }

        let count = if count == 0 { usize::MAX } else { count };

        Ok(Self {
            key,
            element,
            rank,
            count,
            max_len,
        })
    }
}

/// **Integer reply:** the length of the list.
#[derive(Debug)]
pub struct LLen<A> {
    key: A,
}

impl<A> CmdExecutor<A> for LLen<A>
where
    A: CmdArg,
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
        let mut res = None;
        handler
            .shared
            .db()
            .visit_object(self.key.as_ref(), |obj| {
                let list = obj.on_list()?;
                res = Some(Resp3::new_integer(list.len() as Int));

                Ok(())
            })
            .await
            .map_err(|_| RutinError::from(0))?;

        Ok(res)
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(LLen { key })
    }
}

/// # Reply:
///
/// **Null reply:** if the key does not exist.
/// **Bulk string reply:** when called without the count argument, the value of the first element.
/// **Array reply:** when called with the count argument, a list of popped elements.
#[derive(Debug)]
pub struct LPop<A> {
    key: A,
    count: u32,
}

impl<A> CmdExecutor<A> for LPop<A>
where
    A: CmdArg,
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
        let mut res = None;
        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                let list = obj.on_list_mut()?;

                if self.count == 1 {
                    if let Some(value) = list.pop_front() {
                        res = Some(Resp3::new_blob_string(value.to_bytes()));
                    } else {
                        res = Some(Resp3::Null);
                    }
                } else {
                    let mut values = Vec::with_capacity(self.count as usize);
                    for _ in 0..self.count {
                        if let Some(value) = list.pop_front() {
                            values.push(Resp3::new_blob_string(value.to_bytes()));
                        } else {
                            break;
                        }
                    }

                    if values.is_empty() {
                        res = Some(Resp3::Null);
                    } else {
                        res = Some(Resp3::new_array(values));
                    }
                }

                Ok(())
            })
            .await?;

        Ok(res)
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 && args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let count = if let Some(count) = args.next() {
            atoi::<u32>(count.as_ref())?
        } else {
            1
        };

        Ok(Self { key, count })
    }
}

/// # Reply:
///
/// **Integer reply:** the length of the list after the push operation.
#[derive(Debug)]
pub struct LPush<A> {
    key: A,
    values: Vec<A>,
}

impl<A> CmdExecutor<A> for LPush<A>
where
    A: CmdArg,
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
        let mut len = 0;
        handler
            .shared
            .db()
            .update_object_force(
                &self.key,
                || List::default().into(),
                |obj| {
                    let list = obj.on_list_mut()?;

                    for v in self.values {
                        list.push_front(v);
                    }

                    len = list.len();
                    Ok(())
                },
            )
            .await?;

        Ok(Some(Resp3::new_integer(len as Int)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        Ok(Self {
            key,
            values: args.collect(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Where {
    Left,
    Right,
}
impl TryFrom<&[u8]> for Where {
    type Error = RutinError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let len = value.len();
        if len != 3 && len != 4 {
            return Err("ERR invalid wherefrom is given".into());
        }

        match value as &[u8] {
            b"LEFT" => Ok(Where::Left),
            b"RIGHT" => Ok(Where::Right),
            _ => Err("ERR invalid wherefrom is given".into()),
        }
    }
}

#[cfg(test)]
mod cmd_list_tests {
    use super::*;
    use crate::util::{gen_test_handler, test_init};
    use tokio::time::sleep;

    #[tokio::test]
    async fn llen_test() {
        test_init();
        let mut handler = gen_test_handler();

        // Case: 列表不存在
        let _llen_res = LLen::test(&["list"], &mut handler).await.unwrap_err();

        // Case: 插入一个元素
        let lpush_res = LPush::test(&["list", "key1"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lpush_res.into_integer_unchecked(), 1);

        let llen_res = LLen::test(&["list"], &mut handler).await.unwrap().unwrap();
        assert_eq!(llen_res.into_integer_unchecked(), 1);

        // Case: 插入多个元素
        let lpush_res = LPush::test(&["list", "key2", "key3"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lpush_res.into_integer_unchecked(), 3);

        let llen_res = LLen::test(&["list"], &mut handler).await.unwrap().unwrap();
        assert_eq!(llen_res.into_integer_unchecked(), 3);
    }

    #[tokio::test]
    async fn push_pop_test() {
        test_init();
        let mut handler = gen_test_handler();

        // Case: 插入一个元素
        let lpush_res = LPush::test(&["list", "key1"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lpush_res.into_integer_unchecked(), 1);

        // Case: 插入多个元素
        let lpush_res = LPush::test(&["list", "key2", "key3"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lpush_res.into_integer_unchecked(), 3);

        // Case: 弹出一个元素
        let lpop_res = LPop::test(&["list"], &mut handler).await.unwrap().unwrap();
        assert_eq!(lpop_res.into_blob_string_unchecked(), "key3");

        // Case: 弹出多个元素
        let lpop_res = LPop::test(&["list", "2"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            lpop_res.into_array_unchecked(),
            &[
                Resp3::new_blob_string("key2"),
                Resp3::new_blob_string("key1")
            ]
        );

        // Case: 列表为空
        let lpop_res = LPop::test(&["list"], &mut handler).await.unwrap().unwrap();
        assert_eq!(lpop_res, Resp3::Null);
    }

    #[tokio::test]
    async fn blpop_test() {
        test_init();

        // 非阻塞测试
        let mut handler = gen_test_handler();

        let lpush_res = LPush::test(&["l1", "key1a", "key1", "key1c"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lpush_res.into_integer_unchecked(), 3);

        let lpush_res = LPush::test(&["l2", "key2a", "key2", "key2c"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lpush_res.into_integer_unchecked(), 3);

        let blpop_res = BLPop::test(&["l1", "l2", "1"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            blpop_res.into_array_unchecked(),
            &[
                Resp3::new_blob_string("l1"),
                Resp3::new_blob_string("key1c")
            ]
        );

        let blpop_res = BLPop::test(&["l2", "list1", "1"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            blpop_res.into_array_unchecked(),
            &[
                Resp3::new_blob_string("l2"),
                Resp3::new_blob_string("key2c")
            ]
        );

        // 阻塞测试 (无超时)
        let (mut handler, _) = Handler::new_fake();
        let _blpop = BLPop::test(&["l3", "0"], &mut handler).await.unwrap();

        let inbox = handler.context.mailbox.inbox.clone();
        let handle = handler.shared.pool().spawn_pinned(move || async move {
            assert_eq!(
                inbox.recv().unwrap().into_resp3_unchecked(),
                Resp3::new_array(vec![
                    Resp3::new_blob_string("l3"),
                    Resp3::new_blob_string("key")
                ])
            );
        });

        let lpush_res = LPush::test(&["l3", "key"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lpush_res.into_integer_unchecked(), 1);

        handle.await.unwrap();

        // 阻塞测试 (有超时)
        let (mut handler, _) = Handler::new_fake();
        let _blpop = BLPop::test(&["l4", "2"], &mut handler).await.unwrap();

        let inbox = handler.context.mailbox.inbox.clone();
        let handle = handler.shared.pool().spawn_pinned(move || async move {
            assert_eq!(
                inbox.recv().unwrap().into_resp3_unchecked(),
                Resp3::new_array(vec![
                    Resp3::new_blob_string("l4"),
                    Resp3::new_blob_string("key")
                ])
            );
        });

        sleep(Duration::from_millis(500)).await;
        let lpush_res = LPush::test(&["l4", "key"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lpush_res.into_integer_unchecked(), 1);

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn lpos_test() {
        test_init();

        let mut handler = gen_test_handler();
        let _lpush_res = LPush::test(
            &["list", "8", "7", "6", "5", "2", "2", "2", "1", "0"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();

        let lpos_res = LPos::test(&["list", "1"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lpos_res.into_integer_unchecked(), 1);

        let lpos_res = LPos::test(&["list", "2", "count", "0"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            lpos_res.into_array_unchecked(),
            &[
                Resp3::new_integer(2),
                Resp3::new_integer(3),
                Resp3::new_integer(4)
            ]
        );

        let lpos_res = LPos::test(&["list", "2", "rank", "2", "count", "2"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            lpos_res.into_array_unchecked(),
            &[Resp3::new_integer(3), Resp3::new_integer(4)]
        );

        let lpos_res = LPos::test(
            &["list", "2", "rank", "-1", "count", "3", "maxlen", "6"],
            &mut handler,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            lpos_res.into_array_unchecked(),
            &[Resp3::new_integer(4), Resp3::new_integer(3)]
        );
    }
}
