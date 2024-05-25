use crate::{
    cmd::{error::Err, CmdError, CmdExecutor, CmdType},
    frame::{Bulks, Frame},
    server::Handler,
    shared::{
        db::{EventType, ObjValueType},
        Shared,
    },
    util::{self, atoi},
    Id, Int, Key,
};
use bytes::Bytes;
use flume::{Receiver, Sender};
use std::time::Duration;
use tokio::time::Instant;
use tracing::trace;

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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
        let db = shared.db();

        let mut elem = None;
        let update_res = db.update_object(&self.source, |obj| {
            let list = obj.on_list_mut()?;
            elem = match self.wherefrom {
                Where::Left => list.pop_front(),
                Where::Right => list.pop_back(),
            };

            Ok(())
        });

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

                res = Some(Frame::new_bulk_owned(elem));
                Ok(())
            })?;

            if let Some(res) = res {
                // 如果确实拿到了弹出的元素，则返回响应
                return Ok(Some(res));
            }
        }
        // 不存在可弹出元素

        let (key_tx, key_rx) = flume::bounded(1);
        // 监听该键的Update事件
        db.add_event(self.destination, key_tx.clone(), EventType::Update);

        let deadline = if self.timeout == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_secs(self.timeout))
        };

        let res = pop_timeout_at(shared, key_tx, key_rx, deadline).await?;

        Ok(Some(res))
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.len() != 5 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(BLMove {
            source: args.pop_back().unwrap(),
            destination: args.pop_back().unwrap(),
            wherefrom: Where::try_from(args.pop_back().unwrap().as_ref())?,
            whereto: Where::try_from(args.pop_back().unwrap().as_ref())?,
            timeout: atoi::<u64>(args.pop_back().unwrap().as_ref())?,
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
        let db = shared.db();

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
            db.add_event(key.clone(), key_tx.clone(), EventType::Update);
        }

        let deadline = if self.timeout == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_secs(self.timeout))
        };

        let res = pop_timeout_at(shared, key_tx, key_rx, deadline).await?;

        Ok(Some(res))
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        let timeout = atoi::<u64>(&args.pop_back().unwrap())?;

        Ok(Self {
            keys: args.iter().cloned().collect(),
            timeout,
        })
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
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

        shared.db().visit_object(&self.key, |obj| {
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

                    res.push(Frame::new_integer(i as Int));
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

                    res.push(Frame::new_integer(i as Int));
                    if res.len() == count {
                        return Ok(());
                    }
                }
            }

            Ok(())
        })?;

        let count = res.len();
        let res = if count == 0 {
            Frame::new_null()
        } else if count == 1 {
            res.pop().unwrap()
        } else {
            Frame::new_array(res)
        };

        Ok(Some(res))
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if !(args.len() == 2 || args.len() == 4 || args.len() == 6 || args.len() == 8) {
            return Err(Err::WrongArgNum.into());
        }

        let key = args.pop_front().unwrap();
        let element = args.pop_front().unwrap();

        let mut rank = 1;
        let mut count = 1;
        let mut max_len = None;

        while let Some(opt) = args.pop_front() {
            let len = opt.len();
            if len > 6 {
                return Err("ERR invalid option is given".into());
            }

            let mut buf = [0; 6];
            buf[..len].copy_from_slice(&opt);
            buf[..len].make_ascii_uppercase();
            match &buf[..len] {
                b"RANK" => rank = atoi::<Int>(args.pop_front().unwrap().as_ref())?,
                b"COUNT" => count = atoi::<usize>(args.pop_front().unwrap().as_ref())?,
                b"MAXLEN" => max_len = Some(atoi::<usize>(args.pop_front().unwrap().as_ref())?),
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
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
        let mut res = None;
        shared
            .db()
            .visit_object(&self.key, |obj| {
                let list = obj.on_list()?;
                res = Some(Frame::new_integer(list.len() as Int));

                Ok(())
            })
            .map_err(|_| CmdError::from(0))?;

        Ok(res)
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.len() != 1 {
            return Err(Err::WrongArgNum.into());
        }
        Ok(LLen {
            key: args.pop_front().unwrap(),
        })
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
        let mut res = None;
        shared.db().update_object(&self.key, |obj| {
            let list = obj.on_list_mut()?;

            if self.count == 1 {
                if let Some(value) = list.pop_front() {
                    res = Some(Frame::new_bulk_owned(value));
                } else {
                    res = Some(Frame::new_null());
                }
            } else {
                let mut values = Vec::with_capacity(self.count as usize);
                for _ in 0..self.count {
                    if let Some(value) = list.pop_front() {
                        values.push(Frame::new_bulk_owned(value));
                    } else {
                        break;
                    }
                }

                if values.is_empty() {
                    res = Some(Frame::new_null());
                } else {
                    res = Some(Frame::Array(values));
                }
            }

            Ok(())
        })?;

        Ok(res)
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        let len = args.len();
        if len != 1 && len != 2 {
            return Err(Err::WrongArgNum.into());
        }

        let count = if args.len() == 2 {
            atoi::<u32>(&args.pop_back().unwrap())?
        } else {
            1
        };

        Ok(Self {
            key: args.pop_front().unwrap(),
            count,
        })
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
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
        let mut len = 0;
        shared
            .db()
            .update_or_create_object(&self.key, ObjValueType::List, |obj| {
                let list = obj.on_list_mut()?;

                for v in self.values {
                    list.push_front(v);
                }

                len = list.len();
                Ok(())
            })?;

        Ok(Some(Frame::new_integer(len as Int)))
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Self {
            key: args.pop_front().unwrap(),
            values: args.iter().cloned().collect(),
        })
    }
}

/// # Reply:
///
/// **Null reply:** no element could be popped and the timeout expired
/// **Array reply:** the key from which the element was popped and the value of the popped element.
#[derive(Debug)]
pub struct NBLPop {
    keys: Vec<Key>,
    timeout: u64,
    redirect: Option<Id>,
}

impl CmdExecutor for NBLPop {
    const CMD_TYPE: CmdType = CmdType::Write;

    async fn execute(self, handler: &mut Handler) -> Result<Option<Frame>, CmdError> {
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
            shared
                .db()
                .add_event(key, key_tx.clone(), EventType::Update);
        }

        let deadline = if self.timeout == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_secs(self.timeout))
        };

        let shared = handler.shared.clone();
        let bg_sender = if let Some(redirect) = self.redirect {
            shared
                .db()
                .get_client_bg_sender(redirect)
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

    async fn _execute(self, _shared: &Shared) -> Result<Option<Frame>, CmdError> {
        Ok(None)
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.len() < 2 {
            return Err(Err::WrongArgNum.into());
        }

        let timeout = atoi::atoi::<u64>(&args.pop_back().unwrap()).ok_or(Err::A2IParse)?;
        let redirect = if let Some(redirect) = args.pop_front() {
            Some(util::atoi::<Id>(&redirect)?)
        } else {
            None
        };

        Ok(Self {
            keys: args.iter().cloned().collect(),
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

async fn first_round(keys: &[Key], shared: &Shared) -> Result<Option<Frame>, CmdError> {
    let mut res = None;
    // 先尝试一轮pop
    for key in keys.iter() {
        let update_res = shared.db().update_object(key, |obj| {
            let list = obj.on_list_mut()?;

            if let Some(value) = list.pop_front() {
                res = Some(Frame::new_bulks(&[key.clone(), value]));
            }

            Ok(())
        });

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
    key_tx: Sender<Frame>,
    key_rx: Receiver<Frame>,
    deadline: Option<Instant>,
) -> Result<Frame, CmdError> {
    let db = shared.db();
    let mut res = None;

    trace!("listening for list keys..., deadline: {deadline:?}");
    loop {
        // 存在超时时间
        if let Some(dl) = deadline {
            match tokio::time::timeout_at(dl, key_rx.recv_async()).await {
                Ok(Ok(key)) => {
                    let key = key.to_bulk()?;
                    let update_res = db.update_object(&key, |obj| {
                        let list = obj.on_list_mut()?;

                        if let Some(value) = list.pop_front() {
                            res = Some(Frame::new_bulks(&[key.clone(), value]));
                        }

                        Ok(())
                    });

                    if let Some(res) = res {
                        // 如果pop_front确实成功了，则退出循环
                        break Ok(res);
                    }

                    // 如果pop_front失败了，则重新加入事件
                    db.add_event(key, key_tx.clone(), EventType::Update);

                    // 忽略空键的错误
                    if !matches!(update_res, Err(CmdError::Null)) {
                        update_res?;
                    }
                }
                // 超时
                Err(_) => break Ok(Frame::new_null()),
                _ => continue,
            }
        }

        // 不存在超时时间
        if let Ok(key) = key_rx.recv_async().await {
            let key = key.to_bulk()?;
            let update_res = shared.db().update_object(&key, |obj| {
                let list = obj.on_list_mut()?;

                if let Some(value) = list.pop_front() {
                    res = Some(Frame::new_bulks(&[key.clone(), value]));
                }

                Ok(())
            });

            if let Some(res) = res {
                // 如果pop_front确实成功了，则退出循环
                break Ok(res);
            }

            db.add_event(key, key_tx.clone(), EventType::Update);

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
    use crate::{
        cmd::Ping,
        shared::db::db_tests::get_event,
        util::{create_handler, create_server, test_init},
    };
    use tokio::time::sleep;

    #[tokio::test]
    async fn llen_test() {
        test_init();
        let shared = Shared::default();

        let llen = LLen {
            key: Key::from("list"),
        };
        matches!(
            llen._execute(&shared).await.unwrap_err(),
            CmdError::ErrorCode { code } if code == 0
        );

        let lpush = LPush::parse(&mut Bulks::from(["list", "key1"].as_ref())).unwrap();
        assert_eq!(
            Some(Frame::new_integer(1)),
            lpush._execute(&shared).await.unwrap()
        );

        let llen = LLen {
            key: Key::from("list"),
        };
        assert_eq!(
            Some(Frame::new_integer(1)),
            llen._execute(&shared).await.unwrap()
        );

        let lpush = LPush::parse(&mut Bulks::from(["list", "key2", "key3"].as_ref())).unwrap();
        assert_eq!(
            Some(Frame::new_integer(3)),
            lpush._execute(&shared).await.unwrap()
        );

        let llen = LLen {
            key: Key::from("list"),
        };
        assert_eq!(
            Some(Frame::new_integer(3)),
            llen._execute(&shared).await.unwrap()
        );
    }

    #[tokio::test]
    async fn push_pop_test() {
        test_init();
        let shared = Shared::default();

        let lpush = LPush::parse(&mut Bulks::from(["list", "key1"].as_ref())).unwrap();
        assert_eq!(
            Some(Frame::new_integer(1)),
            lpush._execute(&shared).await.unwrap()
        );
        // key1

        let lpush = LPush::parse(&mut Bulks::from(["list", "key2", "key3"].as_ref())).unwrap();
        assert_eq!(
            Some(Frame::new_integer(3)),
            lpush._execute(&shared).await.unwrap()
        );
        // key3 key2 key1

        let lpop = LPop::parse(&mut Bulks::from(["list"].as_ref())).unwrap();
        assert_eq!(
            Frame::new_bulk_from_static(b"key3"),
            lpop._execute(&shared).await.unwrap().unwrap()
        );

        let lpop = LPop::parse(&mut Bulks::from(["list", "2"].as_ref())).unwrap();
        assert_eq!(
            Frame::new_bulks_from_static(&[b"key2", b"key1"]),
            lpop._execute(&shared).await.unwrap().unwrap()
        );

        let lpop = LPop::parse(&mut Bulks::from(["list"].as_ref())).unwrap();
        assert_eq!(
            Frame::new_null(),
            lpop._execute(&shared).await.unwrap().unwrap()
        );
    }

    #[tokio::test]
    async fn blpop_test() {
        test_init();

        let mut server = create_server().await;
        let handler = create_handler(&mut server).await;

        /***************/
        /* 非阻塞测试 */
        /***************/
        let lpush =
            LPush::parse(&mut Bulks::from(["l1", "key1a", "key1", "key1c"].as_ref())).unwrap();
        assert_eq!(
            Some(Frame::new_integer(3)),
            lpush._execute(&handler.shared).await.unwrap()
        );
        // l1: key1c key1b key1a

        let lpush =
            LPush::parse(&mut Bulks::from(["l2", "key2a", "key2", "key2c"].as_ref())).unwrap();
        assert_eq!(
            Some(Frame::new_integer(3)),
            lpush._execute(&handler.shared).await.unwrap()
        );
        // l2: key2c key2b key2a

        let blpop = BLPop::parse(&mut Bulks::from(["l1", "l2", "1"].as_ref())).unwrap();
        assert_eq!(
            Frame::new_bulks_from_static(&[b"l1", b"key1c"]),
            blpop._execute(&handler.shared).await.unwrap().unwrap()
        );
        // l1: key1b key1a

        let blpop = BLPop::parse(&mut Bulks::from(["l2", "list1", "1"].as_ref())).unwrap();
        assert_eq!(
            Frame::new_bulks_from_static(&[b"l2", b"key2c"]),
            blpop._execute(&handler.shared).await.unwrap().unwrap()
        );
        // l2: key2b key2a

        /************************/
        /* 无超时时间，阻塞测试 */
        /************************/
        let handler2 = create_handler(&mut server).await;
        tokio::spawn(async move {
            let blpop = BLPop::parse(&mut Bulks::from(["l3", "0"].as_ref())).unwrap();
            assert_eq!(
                Frame::new_bulks_from_static(&[b"l3", b"key"]),
                blpop._execute(&handler2.shared).await.unwrap().unwrap()
            );
        });

        sleep(Duration::from_millis(500)).await;
        let lpush = LPush::parse(&mut Bulks::from(["l3", "key"].as_ref())).unwrap();
        assert_eq!(
            Frame::new_integer(1),
            lpush._execute(&handler.shared).await.unwrap().unwrap()
        );

        /************************/
        /* 有超时时间，阻塞测试 */
        /************************/
        let handler3 = create_handler(&mut server).await;
        tokio::spawn(async move {
            let blpop = BLPop::parse(&mut Bulks::from(["l4", "2"].as_ref())).unwrap();
            assert_eq!(
                Frame::new_bulks_from_static(&[b"l4", b"key"]),
                blpop._execute(&handler3.shared).await.unwrap().unwrap()
            );
        });

        sleep(Duration::from_millis(500)).await;
        let lpush = LPush::parse(&mut Bulks::from(["l4", "key"].as_ref())).unwrap();
        assert_eq!(
            Frame::new_integer(1),
            lpush._execute(&handler.shared).await.unwrap().unwrap()
        );

        /************************/
        /* 有超时时间，超时测试 */
        /************************/
        let blpop = BLPop::parse(&mut Bulks::from(["null", "1"].as_ref())).unwrap();
        assert_eq!(
            Frame::new_null(),
            blpop._execute(&handler.shared).await.unwrap().unwrap()
        );
    }

    #[tokio::test]
    async fn nblpop_test() {
        test_init();

        let mut server = create_server().await;
        let mut handler = create_handler(&mut server).await;

        /************/
        /* 普通测试 */
        /************/
        let lpush =
            LPush::parse(&mut Bulks::from(["l1", "key1a", "key1", "key1c"].as_ref())).unwrap();
        assert_eq!(
            Some(Frame::new_integer(3)),
            lpush._execute(&handler.shared).await.unwrap()
        );
        // l1: key1c key1b key1a

        let lpush =
            LPush::parse(&mut Bulks::from(["l2", "key2a", "key2", "key2c"].as_ref())).unwrap();
        assert_eq!(
            Some(Frame::new_integer(3)),
            lpush._execute(&handler.shared).await.unwrap()
        );
        // l2: key2c key2b key2a

        /**************/
        /* 有超时时间 */
        /**************/
        let nblpop = NBLPop::parse(&mut Bulks::from(["list3", "2"].as_ref())).unwrap();
        nblpop.execute(&mut handler).await.unwrap();
        println!("{:?}", get_event(handler.shared.db(), b"list3").unwrap());

        let ping = Ping::parse(&mut Bulks::default()).unwrap();
        assert_eq!(
            Frame::new_simple_borrowed("PONG"),
            ping._execute(&handler.shared).await.unwrap().unwrap()
        );

        sleep(Duration::from_millis(500)).await;
        let lpush = LPush::parse(&mut Bulks::from(["list3", "key"].as_ref())).unwrap();
        assert_eq!(
            Frame::new_integer(1),
            lpush._execute(&handler.shared).await.unwrap().unwrap()
        );

        assert_eq!(
            Frame::new_bulks_from_static(&[b"list3", b"key"]),
            handler.bg_task_channel.recv_from_bg_task().await
        );

        /************************/
        /* 有超时时间，超时测试 */
        /************************/
        let nblpop = NBLPop::parse(&mut Bulks::from(["whatever", "1"].as_ref())).unwrap();
        nblpop.execute(&mut handler).await.unwrap();
        assert_eq!(
            Frame::new_null(),
            handler.bg_task_channel.recv_from_bg_task().await
        );

        /**************/
        /* 无超时时间 */
        /**************/
        let nblpop = NBLPop::parse(&mut Bulks::from(["list3", "0"].as_ref())).unwrap();
        nblpop.execute(&mut handler).await.unwrap();

        let ping = Ping::parse(&mut Bulks::default()).unwrap();
        assert_eq!(
            Frame::new_simple_borrowed("PONG"),
            ping._execute(&handler.shared).await.unwrap().unwrap()
        );

        sleep(Duration::from_millis(500)).await;
        let lpush = LPush::parse(&mut Bulks::from(["list3", "key"].as_ref())).unwrap();
        assert_eq!(
            Frame::new_integer(1),
            lpush._execute(&handler.shared).await.unwrap().unwrap()
        );

        assert_eq!(
            Frame::new_bulks_from_static(&[b"list3", b"key"]),
            handler.bg_task_channel.recv_from_bg_task().await
        );
    }

    #[tokio::test]
    async fn lpos_test() {
        test_init();

        let shared = Shared::default();
        let lpush = LPush::parse(&mut Bulks::from(
            ["list", "8", "7", "6", "5", "2", "2", "2", "1", "0"].as_ref(),
        ))
        .unwrap();
        lpush._execute(&shared).await.unwrap().unwrap();

        let lpos = LPos::parse(&mut Bulks::from(["list", "1"].as_ref())).unwrap();
        let res = lpos._execute(&shared).await.unwrap().unwrap();
        assert_eq!(res.on_integer().unwrap(), 1);

        let lpos = LPos::parse(&mut Bulks::from(["list", "2", "count", "0"].as_ref())).unwrap();
        let res = lpos._execute(&shared).await.unwrap().unwrap();
        assert_eq!(
            res.into_array().unwrap(),
            vec![
                Frame::new_integer(2),
                Frame::new_integer(3),
                Frame::new_integer(4)
            ]
        );

        let lpos = LPos::parse(&mut Bulks::from(
            ["list", "2", "rank", "2", "count", "2"].as_ref(),
        ))
        .unwrap();
        let res = lpos._execute(&shared).await.unwrap().unwrap();
        assert_eq!(
            res.into_array().unwrap(),
            vec![Frame::new_integer(3), Frame::new_integer(4)]
        );

        let lpos = LPos::parse(&mut Bulks::from(
            ["list", "2", "rank", "-1", "count", "3", "maxlen", "6"].as_ref(),
        ))
        .unwrap();
        let res = lpos._execute(&shared).await.unwrap().unwrap();
        assert_eq!(
            res.into_array().unwrap(),
            vec![Frame::new_integer(4), Frame::new_integer(3)]
        );
    }
}
