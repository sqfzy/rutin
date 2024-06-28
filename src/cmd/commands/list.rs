use super::*;
use crate::{
    cmd::{error::Err, CmdError, CmdExecutor, CmdType, CmdUnparsed},
    conf::AccessControl,
    connection::AsyncStream,
    frame::Resp3,
    server::Handler,
    shared::{db::ObjValueType, Shared},
    util::atoi,
    Id, Int, Key,
};
use bytes::Bytes;
use flume::{Receiver, Sender};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{instrument, trace};

/// # Reply:
///
/// **Bulk string reply:** the element being popped from the source and pushed to the destination.
/// **Null reply:** the operation timed-out
#[derive(Debug)]
pub struct BLMove {
    source: Key,
    destination: Key,
    wherefrom: Where,
    whereto: Where,
    timeout: u64,
}

impl CmdExecutor for BLMove {
    const NAME: &'static str = "BLMOVE";
    const TYPE: CmdType = CmdType::Write;
    const FLAG: CmdFlag = BLMOVE_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let db = handler.shared.db();

        let mut elem = None;
        let update_res = db
            .update_object(&self.source, |obj| {
                let list = obj.on_list_mut()?;
                elem = match self.wherefrom {
                    Where::Left => list.pop_front(),
                    Where::Right => list.pop_back(),
                };

                Ok(())
            })
            .await;

        // 忽略空键的错误
        if !matches!(update_res, Err(CmdError::Null)) {
            update_res?;
        }

        if let Some(elem) = elem {
            // 存在可弹出元素
            let mut res = None;

            db.update_or_create_object(&self.destination, ObjValueType::List, |obj| {
                let list = obj.on_list_mut()?;
                match self.whereto {
                    Where::Left => list.push_front(elem.clone()),
                    Where::Right => list.push_back(elem.clone()),
                }

                res = Some(Resp3::new_blob_string(elem));
                Ok(())
            })
            .await?;

            if let Some(res) = res {
                // 如果确实拿到了弹出的元素，则返回响应
                return Ok(Some(res));
            }
        }
        // 不存在可弹出元素

        let (key_tx, key_rx) = flume::bounded(1);
        // 监听该键的Update事件
        db.add_may_update_event(self.destination, key_tx.clone())
            .await;

        let deadline = if self.timeout == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_secs(self.timeout))
        };

        let res = pop_timeout_at(&handler.shared, key_tx, key_rx, deadline).await?;

        Ok(Some(res))
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 5 {
            return Err(Err::WrongArgNum.into());
        }

        let source = args.next().unwrap();
        if ac.is_forbidden_key(&source, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

        let destination = args.next().unwrap();
        if ac.is_forbidden_key(&destination, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

        Ok(BLMove {
            source,
            destination,
            wherefrom: Where::try_from(args.next_back().unwrap().as_ref())?,
            whereto: Where::try_from(args.next_back().unwrap().as_ref())?,
            timeout: atoi::<u64>(args.next_back().unwrap().as_ref())?,
        })
    }
}

/// # Reply:
///
/// **Null reply:** no element could be popped and the timeout expired
/// **Array reply:** the key from which the element was popped and the value of the popped element.
#[derive(Debug)]
pub struct BLPop {
    keys: Vec<Key>,
    timeout: u64,
}

impl CmdExecutor for BLPop {
    const NAME: &'static str = "BLPOP";
    const TYPE: CmdType = CmdType::Write;
    const FLAG: CmdFlag = BLPOP_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let db = handler.shared.db();

        match first_round(&self.keys, &handler.shared).await {
            // 弹出成功，直接返回
            Ok(Some(frame)) => {
                return Ok(Some(frame));
            }
            // 弹出失败，继续后续操作
            Ok(None) => {}
            // 发生错误，直接返回
            Err(e) => {
                return Err(e);
            }
        }

        // 加入监听事件
        let (key_tx, key_rx) = flume::bounded(1);
        for key in self.keys {
            db.add_may_update_event(key.clone(), key_tx.clone()).await;
        }

        let deadline = if self.timeout == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_secs(self.timeout))
        };

        let res = pop_timeout_at(&handler.shared, key_tx, key_rx, deadline).await?;

        Ok(Some(res))
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        let timeout = atoi::<u64>(&args.next_back().unwrap())?;

        let keys: Vec<_> = args.collect();
        if ac.is_forbidden_keys(&keys, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

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
    key: Key,
    element: Bytes,
    rank: Int,
    count: usize,
    max_len: Option<usize>,
}

impl CmdExecutor for LPos {
    const NAME: &'static str = "LPOS";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = LPOS_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
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
                        if list[i] != self.element {
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
                        if list[i] != self.element {
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

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if !(args.len() == 2 || args.len() == 4 || args.len() == 6 || args.len() == 8) {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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

            let mut buf = [0; 6];
            buf[..len].copy_from_slice(&opt);
            buf[..len].make_ascii_uppercase();
            match &buf[..len] {
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
    key: Key,
}

impl CmdExecutor for LLen {
    const NAME: &'static str = "LLEN";
    const TYPE: CmdType = CmdType::Other;
    const FLAG: CmdFlag = LLEN_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
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
            .map_err(|_| CmdError::from(0))?;

        Ok(res)
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    key: Key,
    count: u32,
}

impl CmdExecutor for LPop {
    const NAME: &'static str = "LPOP";
    const TYPE: CmdType = CmdType::Write;
    const FLAG: CmdFlag = LPOP_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut res = None;
        handler
            .shared
            .db()
            .update_object(&self.key, |obj| {
                let list = obj.on_list_mut()?;

                if self.count == 1 {
                    if let Some(value) = list.pop_front() {
                        res = Some(Resp3::new_blob_string(value));
                    } else {
                        res = Some(Resp3::Null);
                    }
                } else {
                    let mut values = Vec::with_capacity(self.count as usize);
                    for _ in 0..self.count {
                        if let Some(value) = list.pop_front() {
                            values.push(Resp3::new_blob_string(value));
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

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() != 1 && args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
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
    key: Bytes,
    values: Vec<Bytes>,
}

impl CmdExecutor for LPush {
    const NAME: &'static str = "LPUSH";
    const TYPE: CmdType = CmdType::Write;
    const FLAG: CmdFlag = LPUSH_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let mut len = 0;
        handler
            .shared
            .db()
            .update_or_create_object(&self.key, ObjValueType::List, |obj| {
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

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.next().unwrap();
        if ac.is_forbidden_key(&key, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

        Ok(Self {
            key,
            values: args.collect(),
        })
    }
}

/// # Reply:
///
/// **Null reply:** no element could be popped and the timeout expired
/// **Array reply:** the key from which the element was popped and the value of the popped element.
// TODO: 也许应该返回Resp3::Push?
#[derive(Debug)]
pub struct NBLPop {
    keys: Vec<Key>,
    timeout: u64,
    redirect: Id, // 0表示不重定向
}

impl CmdExecutor for NBLPop {
    const NAME: &'static str = "NBLPOP";
    const TYPE: CmdType = CmdType::Write;
    const FLAG: CmdFlag = NBLPOP_FLAG;

    #[instrument(level = "debug", skip(handler), ret, err)]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Resp3>, CmdError> {
        let Handler { shared, .. } = handler;

        match first_round(&self.keys, shared).await {
            // 弹出成功，直接返回
            Ok(Some(frame)) => {
                return Ok(Some(frame));
            }
            // 弹出失败，继续后续操作
            Ok(None) => {}
            // 发生错误，直接返回
            Err(e) => {
                return Err(e);
            }
        }

        // 加入监听事件
        let (key_tx, key_rx) = flume::bounded(1);
        for key in self.keys {
            shared.db().add_may_update_event(key, key_tx.clone()).await;
        }

        let deadline = if self.timeout == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_secs(self.timeout))
        };

        let shared = handler.shared.clone();
        let bg_sender = if self.redirect != 0 {
            shared
                .db()
                .get_client_bg_sender(self.redirect)
                .ok_or("ERR The client ID you want redirect to does not exist")?
        } else {
            handler.bg_task_channel.new_sender()
        };

        tokio::spawn(async move {
            let res = match pop_timeout_at(&shared, key_tx, key_rx, deadline).await {
                Ok(res) => res,
                Err(e) => e.try_into().unwrap(),
            };
            let _ = bg_sender.send(res);
        });

        // 开启后台任务后，向客户端返回的响应不由该函数负责，而是由后台任务负责
        Ok(None)
    }

    fn parse(args: &mut CmdUnparsed, ac: &AccessControl) -> Result<Self, CmdError> {
        if args.len() < 3 {
            return Err(Err::WrongArgNum.into());
        }

        let redirect = atoi::atoi::<Id>(&args.next_back().unwrap()).ok_or(Err::A2IParse)?;
        let timeout = atoi::atoi::<u64>(&args.next_back().unwrap()).ok_or(Err::A2IParse)?;

        let keys: Vec<_> = args.collect();
        if ac.is_forbidden_keys(&keys, Self::TYPE) {
            return Err(Err::NoPermission.into());
        }

        Ok(Self {
            keys,
            timeout,
            redirect,
        })
    }
}

#[derive(Debug)]
pub enum Where {
    Left,
    Right,
}
impl TryFrom<&[u8]> for Where {
    type Error = &'static str;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let len = value.len();
        if len != 3 && len != 4 {
            return Err("ERR invalid wherefrom is given");
        }

        let mut buf = [0; 5];
        buf[..len].copy_from_slice(value);
        buf[..len].make_ascii_uppercase();
        match &buf[..len] {
            b"LEFT" => Ok(Where::Left),
            b"RIGHT" => Ok(Where::Right),
            _ => Err("ERR invalid wherefrom is given"),
        }
    }
}

async fn first_round<S: AsRef<str> + PartialEq>(
    keys: &[Key],
    shared: &Shared,
) -> Result<Option<Resp3<Bytes, S>>, CmdError> {
    let mut res = None;
    // 先尝试一轮pop
    for key in keys.iter() {
        let update_res = shared
            .db()
            .update_object(key, |obj| {
                let list = obj.on_list_mut()?;

                if let Some(value) = list.pop_front() {
                    res = Some(Resp3::new_array(vec![
                        Resp3::new_blob_string(key.clone()),
                        Resp3::new_blob_string(value),
                    ]));
                }

                Ok(())
            })
            .await;

        // pop成功
        if res.is_some() {
            return Ok(res);
        }

        // 忽略空键的错误
        if !matches!(update_res, Err(CmdError::Null)) {
            update_res?;
        }
    }

    Ok(None)
}

async fn pop_timeout_at(
    shared: &Shared,
    key_tx: Sender<Key>,
    key_rx: Receiver<Key>,
    deadline: Option<Instant>,
) -> Result<Resp3, CmdError> {
    let db = shared.db();
    let mut res = None;

    trace!("listening for list keys..., deadline: {deadline:?}");
    loop {
        // 存在超时时间
        if let Some(dl) = deadline {
            match tokio::time::timeout_at(dl, key_rx.recv_async()).await {
                Ok(Ok(key)) => {
                    let update_res = db
                        .update_object(&key, |obj| {
                            let list = obj.on_list_mut()?;

                            if let Some(value) = list.pop_front() {
                                res = Some(Resp3::new_array(vec![
                                    Resp3::new_blob_string(key.clone()),
                                    Resp3::new_blob_string(value),
                                ]));
                            }

                            Ok(())
                        })
                        .await;

                    if let Some(res) = res {
                        // 如果next确实成功了，则退出循环
                        break Ok(res);
                    }

                    // 如果next失败了，则重新加入事件
                    db.add_may_update_event(key.clone(), key_tx.clone()).await;

                    // 忽略空键的错误
                    if !matches!(update_res, Err(CmdError::Null)) {
                        update_res?;
                    }
                }
                // 超时
                Err(_) => break Ok(Resp3::Null),
                _ => continue,
            }
        }

        // 不存在超时时间
        if let Ok(key) = key_rx.recv_async().await {
            let update_res = shared
                .db()
                .update_object(&key, |obj| {
                    let list = obj.on_list_mut()?;

                    if let Some(value) = list.pop_front() {
                        res = Some(Resp3::new_array(vec![
                            Resp3::new_blob_string(key.clone()),
                            Resp3::new_blob_string(value),
                        ]));
                    }

                    Ok(())
                })
                .await;

            if let Some(res) = res {
                // 如果next确实成功了，则退出循环
                break Ok(res);
            }

            db.add_may_update_event(key.clone(), key_tx.clone()).await;

            // 忽略空键的错误
            if !matches!(update_res, Err(CmdError::Null)) {
                update_res?;
            }
        }
    }
}

#[cfg(test)]
mod cmd_list_tests {
    use super::*;
    use crate::{cmd::Ping, util::test_init};
    use tokio::time::sleep;

    #[tokio::test]
    async fn llen_test() {
        test_init();
        let (mut handler, _) = Handler::new_fake();

        let llen = LLen {
            key: Key::from("list"),
        };
        matches!(
            llen.execute(&mut handler).await.unwrap_err(),
            CmdError::ErrorCode { code } if code == 0
        );

        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["list", "key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(1)),
            lpush.execute(&mut handler).await.unwrap()
        );

        let llen = LLen {
            key: Key::from("list"),
        };
        assert_eq!(
            Some(Resp3::new_integer(1)),
            llen.execute(&mut handler).await.unwrap()
        );

        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["list", "key2", "key3"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            lpush.execute(&mut handler).await.unwrap()
        );

        let llen = LLen {
            key: Key::from("list"),
        };
        assert_eq!(
            Some(Resp3::new_integer(3)),
            llen.execute(&mut handler).await.unwrap()
        );
    }

    #[tokio::test]
    async fn push_pop_test() {
        test_init();
        let (mut handler, _) = Handler::new_fake();

        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["list", "key1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(1)),
            lpush.execute(&mut handler).await.unwrap()
        );
        // key1

        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["list", "key2", "key3"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            lpush.execute(&mut handler).await.unwrap()
        );
        // key3 key2 key1

        let lpop = LPop::parse(
            &mut CmdUnparsed::from(["list"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::new_blob_string("key3".into()),
            lpop.execute(&mut handler).await.unwrap().unwrap()
        );

        let lpop = LPop::parse(
            &mut CmdUnparsed::from(["list", "2"].as_ref()),
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
            &mut CmdUnparsed::from(["list"].as_ref()),
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
        let (mut handler, _) = Handler::new_fake();
        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["l1", "key1a", "key1", "key1c"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            lpush.execute(&mut handler).await.unwrap()
        );
        // l1: key1c key1b key1a

        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["l2", "key2a", "key2", "key2c"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            lpush.execute(&mut handler).await.unwrap()
        );
        // l2: key2c key2b key2a

        let blpop = BLPop::parse(
            &mut CmdUnparsed::from(["l1", "l2", "1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::new_array(vec![
                Resp3::new_blob_string("l1".into()),
                Resp3::new_blob_string("key1c".into())
            ]),
            blpop.execute(&mut handler).await.unwrap().unwrap()
        );
        // l1: key1b key1a

        let blpop = BLPop::parse(
            &mut CmdUnparsed::from(["l2", "list1", "1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::new_array(vec![
                Resp3::new_blob_string("l2".into()),
                Resp3::new_blob_string("key2c".into())
            ]),
            blpop.execute(&mut handler).await.unwrap().unwrap()
        );
        // l2: key2b key2a

        /************************/
        /* 无超时时间，阻塞测试 */
        /************************/
        let (mut handler2, _) = Handler::new_fake();
        tokio::spawn(async move {
            let blpop = BLPop::parse(
                &mut CmdUnparsed::from(["l3", "0"].as_ref()),
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

        sleep(Duration::from_millis(500)).await;
        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["l3", "key"].as_ref()),
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
        tokio::spawn(async move {
            let blpop = BLPop::parse(
                &mut CmdUnparsed::from(["l4", "2"].as_ref()),
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

        sleep(Duration::from_millis(500)).await;
        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["l4", "key"].as_ref()),
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
            &mut CmdUnparsed::from(["null", "1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::Null,
            blpop.execute(&mut handler).await.unwrap().unwrap()
        );
    }

    #[tokio::test]
    async fn nblpop_test() {
        test_init();

        let (mut handler, _) = Handler::new_fake();

        /************/
        /* 普通测试 */
        /************/
        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["l1", "key1a", "key1", "key1c"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            lpush.execute(&mut handler).await.unwrap()
        );
        // l1: key1c key1b key1a

        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["l2", "key2a", "key2", "key2c"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Some(Resp3::new_integer(3)),
            lpush.execute(&mut handler).await.unwrap()
        );
        // l2: key2c key2b key2a

        /**************/
        /* 有超时时间 */
        /**************/
        let nblpop = NBLPop::parse(
            &mut CmdUnparsed::from(["list3", "2", "0"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        nblpop.execute(&mut handler).await.unwrap();

        let ping = Ping::parse(&mut CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();
        assert_eq!(
            "PONG".to_string(),
            ping.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap()
                .to_string()
        );

        sleep(Duration::from_millis(500)).await;
        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["list3", "key"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::new_integer(1),
            lpush.execute(&mut handler).await.unwrap().unwrap()
        );

        assert_eq!(
            Resp3::new_array(vec![
                Resp3::new_blob_string("list3".into()),
                Resp3::new_blob_string("key".into())
            ]),
            handler.bg_task_channel.recv_from_bg_task().await
        );

        /************************/
        /* 有超时时间，超时测试 */
        /************************/
        let nblpop = NBLPop::parse(
            &mut CmdUnparsed::from(["whatever", "1", "0"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        nblpop.execute(&mut handler).await.unwrap();
        assert_eq!(
            Resp3::Null,
            handler.bg_task_channel.recv_from_bg_task().await
        );

        /**************/
        /* 无超时时间 */
        /**************/
        let nblpop = NBLPop::parse(
            &mut CmdUnparsed::from(["list3", "0", "0"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        nblpop.execute(&mut handler).await.unwrap();

        let ping = Ping::parse(&mut CmdUnparsed::default(), &AccessControl::new_loose()).unwrap();
        assert_eq!(
            "PONG".to_string(),
            ping.execute(&mut handler)
                .await
                .unwrap()
                .unwrap()
                .try_simple_string()
                .unwrap()
                .to_string()
        );

        sleep(Duration::from_millis(500)).await;
        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["list3", "key"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        assert_eq!(
            Resp3::new_integer(1),
            lpush.execute(&mut handler).await.unwrap().unwrap()
        );

        assert_eq!(
            Resp3::new_array(vec![
                Resp3::new_blob_string("list3".into()),
                Resp3::new_blob_string("key".into())
            ]),
            handler.bg_task_channel.recv_from_bg_task().await
        );
    }

    #[tokio::test]
    async fn lpos_test() {
        test_init();

        let (mut handler, _) = Handler::new_fake();
        let lpush = LPush::parse(
            &mut CmdUnparsed::from(["list", "8", "7", "6", "5", "2", "2", "2", "1", "0"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        lpush.execute(&mut handler).await.unwrap().unwrap();

        let lpos = LPos::parse(
            &mut CmdUnparsed::from(["list", "1"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = lpos.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(res.try_integer().unwrap(), 1);

        let lpos = LPos::parse(
            &mut CmdUnparsed::from(["list", "2", "count", "0"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = lpos.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(
            res.try_array().unwrap().to_vec(),
            vec![
                Resp3::new_integer(2),
                Resp3::new_integer(3),
                Resp3::new_integer(4)
            ]
        );

        let lpos = LPos::parse(
            &mut CmdUnparsed::from(["list", "2", "rank", "2", "count", "2"].as_ref()),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = lpos.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(
            res.try_array().unwrap().to_vec(),
            vec![Resp3::new_integer(3), Resp3::new_integer(4)]
        );

        let lpos = LPos::parse(
            &mut CmdUnparsed::from(
                ["list", "2", "rank", "-1", "count", "3", "maxlen", "6"].as_ref(),
            ),
            &AccessControl::new_loose(),
        )
        .unwrap();
        let res = lpos.execute(&mut handler).await.unwrap().unwrap();
        assert_eq!(
            res.try_array().unwrap().to_vec(),
            vec![Resp3::new_integer(4), Resp3::new_integer(3)]
        );
    }
}
