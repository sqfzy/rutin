use super::*;
use crate::{
    cmd::{CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{AsyncStream, Handler, NEVER_EXPIRE},
    shared::db::{
        Object, ObjectValueType,
        WriteEvent::{self},
    },
    util::{atoi, KeyWrapper},
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
pub struct BLMove {
    source: StaticBytes,
    destination: StaticBytes,
    wherefrom: Where,
    whereto: Where,
    timeout: u64,
}

impl CmdExecutor for BLMove {
    #[instrument(level = "debug", skip(handler), ret, err)]
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

            db.update_object_force(&self.destination, ObjectValueType::List, |obj| {
                let list = obj.on_list_mut()?;
                match self.whereto {
                    Where::Left => list.push_front(elem),
                    Where::Right => list.push_back(elem),
                }

                Ok(())
            })
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

            move |obj: &mut Object| -> RutinResult<()> {
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
            ObjectValueType::List,
            WriteEvent::FnMut {
                deadline,
                callback: Box::new(callback),
            },
        );

        // 4. 处理对客户端的响应

        let dst = KeyWrapper::from(self.destination.as_ref());
        let shared = *shared;

        tokio::select! {
            // 超时，返回Null
            _ = sleep_until(deadline), if deadline != *NEVER_EXPIRE => {
                Ok(Some(Resp3::Null))
            },
            elem = rx.recv_async() => {
                let elem = if let Ok(e) = elem {
                    e
                } else {
                    return Ok(Some(Resp3::Null));
                };

                shared.db().update_object_force(&dst, ObjectValueType::List, |obj| {
                    let list = obj.on_list_mut()?;
                    match self.whereto {
                        Where::Left => list.push_front(elem.clone()),
                        Where::Right => list.push_back(elem.clone()),
                    }

                    Ok(())
                })
                .await?;

                Ok(Some(Resp3::new_blob_string(elem.to_bytes())))
            }
        }
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
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
            timeout: atoi::<u64>(&args.next_back().unwrap())?,
        })
    }
}

/// 从多个列表中的首个非空列表弹出一个元素
/// # Reply:
///
/// **Null reply:** no element could be popped and the timeout expired
/// **Array reply:** the key from which the element was popped and the value of the popped element.
#[derive(Debug)]
pub struct BLPop {
    keys: Vec<StaticBytes>,
    timeout: u64,
}

impl CmdExecutor for BLPop {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let Handler {
            shared, context, ..
        } = handler;
        let db = shared.db();

        for key in &self.keys {
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
                return Ok(Some(Resp3::new_array(vec![
                    Resp3::new_blob_string(key.into()),
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

        let keys = self
            .keys
            .into_iter()
            .map(|k| KeyWrapper::from(k.as_ref()))
            .collect_vec();

        let (tx, rx) = flume::bounded(1);

        // 添加多个键的监听事件，接收者只会接收到第一个非空列表的元素
        for key in keys {
            let callback = {
                let tx = tx.clone();
                let mut shutdown = context.shutdown.listen();
                let key = key.clone();

                move |obj: &mut Object| -> RutinResult<()> {
                    if (&mut shutdown).now_or_never().is_some() {
                        return Ok(());
                    }

                    let list = obj.on_list_mut()?;
                    let elem = list.pop_front();

                    if let Some(elem) = elem {
                        tx.send(CheapResp3::new_array(vec![
                            Resp3::new_blob_string(Bytes::from(key.clone())),
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
                ObjectValueType::List,
                WriteEvent::FnMut {
                    deadline,
                    callback: Box::new(callback),
                },
            );
        }

        tokio::select! {
            // 超时，返回Null
            _ = sleep_until(deadline), if deadline != *NEVER_EXPIRE => {
                Ok(Some(Resp3::Null))
            },
            // 接受并处理首个消息
            msg = rx.recv_async() => {
                Ok(Some(msg.unwrap_or_default()))
            }
        }
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() < 2 {
            return Err(RutinError::WrongArgNum);
        }

        let timeout = atoi::<u64>(&args.next_back().unwrap())?;

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
pub struct LPos {
    key: StaticBytes,
    element: StaticBytes,
    rank: Int,
    count: usize,
    max_len: Option<usize>,
}

impl CmdExecutor for LPos {
    #[instrument(level = "debug", skip(handler), ret, err)]
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
            .visit_object(&self.key, |obj| {
                let list = obj.on_list()?;
                if rank >= 0 {
                    for i in 0..self.max_len.unwrap_or(list.len()) {
                        if self.element != list[i].to_bytes() {
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
                        if self.element != list[i].to_bytes() {
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

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
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
            let len = opt.len();
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
pub struct LLen {
    key: StaticBytes,
}

impl CmdExecutor for LLen {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut res = None;
        handler
            .shared
            .db()
            .visit_object(&self.key, |obj| {
                let list = obj.on_list()?;
                res = Some(Resp3::new_integer(list.len() as Int));

                Ok(())
            })
            .await
            .map_err(|_| RutinError::from(0))?;

        Ok(res)
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
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
pub struct LPop {
    key: StaticBytes,
    count: u32,
}

impl CmdExecutor for LPop {
    #[instrument(level = "debug", skip(handler), ret, err)]
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

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 1 && args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let key = args.next().unwrap();
        if ac.deny_reading_or_writing_key(&key, Self::CATS_FLAG) {
            return Err(RutinError::NoPermission);
        }

        let count = if let Some(count) = args.next() {
            atoi::<u32>(&count)?
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
pub struct LPush {
    key: StaticBytes,
    values: Vec<StaticBytes>,
}

impl CmdExecutor for LPush {
    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let mut len = 0;
        handler
            .shared
            .db()
            .update_object_force(&self.key, ObjectValueType::List, |obj| {
                let list = obj.on_list_mut()?;

                for v in self.values {
                    list.push_front(v);
                }

                len = list.len();
                Ok(())
            })
            .await?;

        Ok(Some(Resp3::new_integer(len as Int)))
    }

    fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
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
//
// /// # Reply:
// ///
// /// **Null reply:** no element could be popped and the timeout expired
// /// **Array reply:** the key from which the element was popped and the value of the popped element.
// // TODO: 也许应该返回Resp3::Push?
// #[derive(Debug)]
// pub struct NBLPop {
//     keys: Vec<Key>,
//     timeout: u64,
//     redirect: Id, // 0表示不重定向
// }
//
// impl CmdExecutor for NBLPop {
//     #[instrument(level = "debug", skip(handler), ret, err)]
//     async fn execute(
//         self,
//         handler: &mut Handler<impl AsyncStream>,
//     ) -> RutinResult<Option<CheapResp3>> {
//         let shared = handler.shared;
//
//         match first_round(&self.keys, shared).await {
//             // 弹出成功，直接返回
//             Ok(Some(frame)) => {
//                 return Ok(Some(frame));
//             }
//             // 弹出失败，继续后续操作
//             Ok(None) => {}
//             // 发生错误，直接返回
//             Err(e) => {
//                 return Err(e);
//             }
//         }
//
//         // 加入监听事件
//         let (key_tx, key_rx) = flume::bounded(1);
//         for key in self.keys {
//             shared
//                 .db()
//                 .add_may_update_event(key, key_tx.clone())
//                 .await?;
//         }
//
//         let deadline = if self.timeout == 0 {
//             None
//         } else {
//             Some(Instant::now() + Duration::from_secs(self.timeout))
//         };
//
//         let outbox = if self.redirect != 0 {
//             handler
//                 .shared
//                 .post_office()
//                 .get_outbox(self.redirect)
//                 .ok_or(RutinError::from("ERR The client ID does not exist"))?
//                 .clone()
//         } else {
//             handler.context.outbox.clone()
//         };
//
//         tokio::spawn(async move {
//             let res = match pop_timeout_at(shared, key_tx, key_rx, deadline).await {
//                 Ok(res) => res,
//                 Err(e) => e.try_into().unwrap(),
//             };
//             let _ = outbox.send(Letter::Resp3(res));
//         });
//
//         // 开启后台任务后，向客户端返回的响应不由该函数负责，而是由后台任务负责
//         Ok(None)
//     }
//
//     fn parse(mut args: CmdUnparsed, ac: &AccessControl) -> RutinResult<Self> {
//         if args.len() < 3 {
//             return Err(RutinError::WrongArgNum);
//         }
//
//         let redirect = atoi::<Id>(&args.next_back().unwrap())?;
//         let timeout = atoi::<u64>(&args.next_back().unwrap())?;
//
//         let keys = args
//             .map(|k| {
//                 if ac.deny_reading_or_writing_key(&k, Self::CATS_FLAG) {
//                     return Err(RutinError::NoPermission);
//                 }
//                 Ok(k.into())
//             })
//             .collect::<RutinResult<Vec<Key>>>()?;
//
//         Ok(Self {
//             keys,
//             timeout,
//             redirect,
//         })
//     }
// }
//
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

        let llen = LLen::parse(
            gen_cmdunparsed_test(["list"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        matches!(
            llen.execute(&mut handler).await.unwrap_err(),
            RutinError::ErrCode { code } if code == 0
        );

        let lpush = LPush::parse(
            gen_cmdunparsed_test(["list", "key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(1)),
            lpush.execute(&mut handler).await.unwrap()
        );

        let llen = LLen::parse(
            gen_cmdunparsed_test(["list"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(1)),
            llen.execute(&mut handler).await.unwrap()
        );

        let lpush = LPush::parse(
            gen_cmdunparsed_test(["list", "key2", "key3"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            lpush.execute(&mut handler).await.unwrap()
        );

        let llen = LLen::parse(
            gen_cmdunparsed_test(["list"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            llen.execute(&mut handler).await.unwrap()
        );
    }

    #[tokio::test]
    async fn push_pop_test() {
        test_init();
        let mut handler = gen_test_handler();

        let lpush = LPush::parse(
            gen_cmdunparsed_test(["list", "key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(1)),
            lpush.execute(&mut handler).await.unwrap()
        );
        // key1

        let lpush = LPush::parse(
            gen_cmdunparsed_test(["list", "key2", "key3"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            lpush.execute(&mut handler).await.unwrap()
        );
        // key3 key2 key1

        let lpop = LPop::parse(
            gen_cmdunparsed_test(["list"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::new_blob_string("key3".into()),
            lpop.execute(&mut handler).await.unwrap().unwrap()
        );

        let lpop = LPop::parse(
            gen_cmdunparsed_test(["list", "2"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::new_array(vec![
                Resp3::new_blob_string("key2".into()),
                Resp3::new_blob_string("key1".into())
            ]),
            lpop.execute(&mut handler).await.unwrap().unwrap()
        );

        let lpop = LPop::parse(
            gen_cmdunparsed_test(["list"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::Null,
            lpop.execute(&mut handler).await.unwrap().unwrap()
        );
    }

    #[tokio::test]
    async fn blpop_test() {
        test_init();

        /***************/
        /* 非阻塞测试 */
        /***************/
        let mut handler = gen_test_handler();
        let inbox = handler.context.inbox.clone();

        let lpush = LPush::parse(
            gen_cmdunparsed_test(["l1", "key1a", "key1", "key1c"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            lpush.execute(&mut handler).await.unwrap()
        );
        // l1: key1c key1b key1a

        let lpush = LPush::parse(
            gen_cmdunparsed_test(["l2", "key2a", "key2", "key2c"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            lpush.execute(&mut handler).await.unwrap()
        );
        // l2: key2c key2b key2a

        let blpop = BLPop::parse(
            gen_cmdunparsed_test(["l1", "l2", "1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        blpop.execute(&mut handler).await.unwrap();
        assert_eq!(
            &Resp3::new_array(vec![
                Resp3::new_blob_string("l1".into()),
                Resp3::new_blob_string("key1c".into())
            ]),
            inbox.recv().as_resp3_unchecked()
        );
        // l1: key1b key1a

        let blpop = BLPop::parse(
            gen_cmdunparsed_test(["l2", "list1", "1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        blpop.execute(&mut handler).await.unwrap();
        assert_eq!(
            &Resp3::new_array(vec![
                Resp3::new_blob_string("l2".into()),
                Resp3::new_blob_string("key2c".into())
            ]),
            inbox.recv().as_resp3_unchecked()
        );
        // l2: key2b key2a

        // TODO:
        /************************/
        /* 无超时时间，阻塞测试 */
        /************************/
        let (mut handler2, _) = Handler::new_fake();
        let handle = tokio::runtime::Handle::current();
        std::thread::spawn(move || {
            handle.block_on(async move {
                let blpop = BLPop::parse(
                    gen_cmdunparsed_test(["l3", "0"].as_ref()),
                    &AccessControl::new_loose(),
                )
                .unwrap();
                assert_eq!(
                    Resp3::new_array(vec![
                        Resp3::new_blob_string("l3".into()),
                        Resp3::new_blob_string("key".into())
                    ]),
                    blpop.execute(&mut handler2).await.unwrap().unwrap()
                );
            });
        });

        sleep(Duration::from_millis(500)).await;
        let lpush = LPush::parse(
            gen_cmdunparsed_test(["l3", "key"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::new_integer(1),
            lpush.execute(&mut handler).await.unwrap().unwrap()
        );

        /************************/
        /* 有超时时间，阻塞测试 */
        /************************/
        let (mut handler3, _) = Handler::new_fake();
        let handle = tokio::runtime::Handle::current();
        std::thread::spawn(move || {
            handle.block_on(async move {
                let blpop = BLPop::parse(
                    gen_cmdunparsed_test(["l4", "2"].as_ref()),
                    &AccessControl::new_loose(),
                )
                .unwrap();
                assert_eq!(
                    Resp3::new_array(vec![
                        Resp3::new_blob_string("l4".into()),
                        Resp3::new_blob_string("key".into())
                    ]),
                    blpop.execute(&mut handler3).await.unwrap().unwrap()
                );
            });
        });

        sleep(Duration::from_millis(500)).await;
        let lpush = LPush::parse(
            gen_cmdunparsed_test(["l4", "key"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::new_integer(1),
            lpush.execute(&mut handler).await.unwrap().unwrap()
        );

        /************************/
        /* 有超时时间，超时测试 */
        /************************/
        let blpop = BLPop::parse(
            gen_cmdunparsed_test(["null", "1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::Null,
            blpop.execute(&mut handler).await.unwrap().unwrap()
        );
    }

    // #[tokio::test]
    // async fn nblpop_test() {
    //     test_init();
    //
    //     let mut handler = gen_test_handler();
    //
    //     /************/
    //     /* 普通测试 */
    //     /************/
    //     let lpush = LPush::parse(
    //         gen_cmdunparsed_test(["l1", "key1a", "key1", "key1c"].as_ref()),
    //         &AccessControl::new_loose(),
    //     )
    //     .unwrap();
    //     assert_eq!(
    //         Some(Resp3::new_integer(3)),
    //         lpush.execute(&mut handler).await.unwrap()
    //     );
    //     // l1: key1c key1b key1a
    //
    //     let lpush = LPush::parse(
    //         gen_cmdunparsed_test(["l2", "key2a", "key2", "key2c"].as_ref()),
    //         &AccessControl::new_loose(),
    //     )
    //     .unwrap();
    //     assert_eq!(
    //         Some(Resp3::new_integer(3)),
    //         lpush.execute(&mut handler).await.unwrap()
    //     );
    //     // l2: key2c key2b key2a
    //
    //     /**************/
    //     /* 有超时时间 */
    //     /**************/
    //     let nblpop = NBLPop::parse(
    //         gen_cmdunparsed_test(["list3", "2", "0"].as_ref()),
    //         &AccessControl::new_loose(),
    //     )
    //     .unwrap();
    //     nblpop.execute(&mut handler).await.unwrap();
    //
    //     let ping = Ping::parse(CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();
    //     assert_eq!(
    //         "PONG".to_string(),
    //         ping.execute(&mut handler)
    //             .await
    //             .unwrap()
    //             .unwrap()
    //             .try_simple_string()
    //             .unwrap()
    //             .to_string()
    //     );
    //
    //     sleep(Duration::from_millis(500)).await;
    //     let lpush = LPush::parse(
    //         gen_cmdunparsed_test(["list3", "key"].as_ref()),
    //         &AccessControl::new_loose(),
    //     )
    //     .unwrap();
    //     assert_eq!(
    //         Resp3::new_integer(1),
    //         lpush.execute(&mut handler).await.unwrap().unwrap()
    //     );
    //
    //     assert_eq!(
    //         &Resp3::new_array(vec![
    //             Resp3::new_blob_string("list3".into()),
    //             Resp3::new_blob_string("key".into())
    //         ]),
    //         handler
    //             .context
    //             .inbox
    //             .recv_async()
    //             .await
    //             .as_resp3_unchecked()
    //     );
    //
    //     /************************/
    //     /* 有超时时间，超时测试 */
    //     /************************/
    //     let nblpop = NBLPop::parse(
    //         gen_cmdunparsed_test(["whatever", "1", "0"].as_ref()),
    //         &AccessControl::new_loose(),
    //     )
    //     .unwrap();
    //     nblpop.execute(&mut handler).await.unwrap();
    //     assert_eq!(
    //         &Resp3::new_null(),
    //         handler
    //             .context
    //             .inbox
    //             .recv_async()
    //             .await
    //             .as_resp3_unchecked()
    //     );
    //
    //     /**************/
    //     /* 无超时时间 */
    //     /**************/
    //     let nblpop = NBLPop::parse(
    //         gen_cmdunparsed_test(["list3", "0", "0"].as_ref()),
    //         &AccessControl::new_loose(),
    //     )
    //     .unwrap();
    //     nblpop.execute(&mut handler).await.unwrap();
    //
    //     let ping = Ping::parse(CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();
    //     assert_eq!(
    //         "PONG".to_string(),
    //         ping.execute(&mut handler)
    //             .await
    //             .unwrap()
    //             .unwrap()
    //             .try_simple_string()
    //             .unwrap()
    //             .to_string()
    //     );
    //
    //     sleep(Duration::from_millis(500)).await;
    //     let lpush = LPush::parse(
    //         gen_cmdunparsed_test(["list3", "key"].as_ref()),
    //         &AccessControl::new_loose(),
    //     )
    //     .unwrap();
    //     assert_eq!(
    //         Resp3::new_integer(1),
    //         lpush.execute(&mut handler).await.unwrap().unwrap()
    //     );
    //
    //     assert_eq!(
    //         &Resp3::new_array(vec![
    //             Resp3::new_blob_string("list3".into()),
    //             Resp3::new_blob_string("key".into())
    //         ]),
    //         handler
    //             .context
    //             .inbox
    //             .recv_async()
    //             .await
    //             .as_resp3_unchecked()
    //     );
    // }
    #[tokio::test]
    async fn lpos_test() {
        test_init();

        let mut handler = gen_test_handler();
        let lpush = LPush::parse(
            gen_cmdunparsed_test(["list", "8", "7", "6", "5", "2", "2", "2", "1", "0"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        lpush.execute(&mut handler).await.unwrap().unwrap();

        let lpos = LPos::parse(
            gen_cmdunparsed_test(["list", "1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = lpos.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res.try_integer().unwrap(), 1);

        let lpos = LPos::parse(
            gen_cmdunparsed_test(["list", "2", "count", "0"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = lpos.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(
            res.try_array().unwrap(),
            &[
                CheapResp3::new_integer(2),
                CheapResp3::new_integer(3),
                CheapResp3::new_integer(4)
            ]
        );

        let lpos = LPos::parse(
            gen_cmdunparsed_test(["list", "2", "rank", "2", "count", "2"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = lpos.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(
            res.try_array().unwrap(),
            &[CheapResp3::new_integer(3), CheapResp3::new_integer(4)]
        );

        let lpos = LPos::parse(
            gen_cmdunparsed_test(["list", "2", "rank", "-1", "count", "3", "maxlen", "6"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = lpos.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(
            res.try_array().unwrap(),
            &[CheapResp3::new_integer(4), CheapResp3::new_integer(3)]
        );
    }
}
