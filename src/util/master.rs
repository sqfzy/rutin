use crate::{
    conf::{AccessControl, MasterConf},
    error::{RutinError, RutinResult},
    frame::Resp3,
    persist::rdb::rdb_save,
    server::Handler,
    shared::{Letter, Shared, SET_MASTER_ID},
};
use bytes::{Buf, BytesMut};
use event_listener::listener;
use futures::{future::select_all, FutureExt};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpStream, time::error::Elapsed};
use tokio_util::time::FutureExt as _;

#[derive(Debug)]
pub struct BackLog {
    buf: BytesMut,
    cap: u64,
    offset: u64,
}

impl BackLog {
    fn new(cap: u64) -> Self {
        Self {
            buf: BytesMut::zeroed(cap as usize),
            cap,
            offset: 0,
        }
    }

    pub fn push(&mut self, mut wcmd: BytesMut) {
        self.offset += wcmd.len() as u64;

        if wcmd.len() > self.cap as usize {
            self.buf = wcmd.split_off(wcmd.len() - self.cap as usize);
        } else {
            self.buf.advance(wcmd.len());
            self.buf.unsplit(wcmd);
        }

        debug_assert_eq!(self.cap as usize, self.buf.len());
    }

    pub fn get_gap_data(&self, repl_offset: u64) -> Option<&[u8]> {
        let gap = self.offset.saturating_sub(repl_offset);
        if repl_offset <= self.cap {
            self.buf.get(gap as usize..)
        } else {
            None
        }
    }
}

// 执行该函数后，服务应满足：
// 1. 不存在SET_REPLICA任务
// 2. post_office中存在SET_MASTER_ID mailbox以及set_master_outbox
pub async fn set_server_to_master(shared: Shared, master_conf: MasterConf) -> RutinResult<()> {
    let conf = shared.conf();
    let post_office = shared.post_office();

    let set_master_inbox =
        shared
            .post_office()
            .get_inbox(SET_MASTER_ID)
            .ok_or(RutinError::from(
                "SET_MASTER_ID mailbox should exist when master config is set",
            ))?;

    // 记录最近的写命令
    let mut back_log = BackLog::new(master_conf.backlog_size);

    let mut replica_handlers = Vec::<Handler<TcpStream>>::new();
    let replica_handlers_ptr = &mut replica_handlers as *mut Vec<Handler<TcpStream>>;

    let ping_replica_period = Duration::from_secs(master_conf.ping_replica_period);
    let mut interval = tokio::time::interval(ping_replica_period);
    let ping_frame = Resp3::new_array(vec![Resp3::new_blob_string("PING".into())]);

    let timeout = Duration::from_secs(master_conf.timeout);

    // select_all的数组中必须有一个future，否则会panic
    let mut futs = select_all([async {
        Ok::<Result<Option<Resp3>, RutinError>, Elapsed>(Ok(Some(ping_frame.clone())))
    }
    .boxed()]);

    loop {
        tokio::select! {
            letter = set_master_inbox.recv_async() => {
                match letter {
                    Letter::ShutdownServer | Letter::ShutdownTask | Letter::Reset => {
                        return Ok(());
                    }
                    Letter::BlockAll { unblock_event } => {
                        listener!(unblock_event  => listener);
                        listener.await;
                    }
                    Letter::Wcmd(wcmd) => {
                        if replica_handlers.is_empty() {
                            continue;
                        }

                        for handler in replica_handlers.iter_mut() {
                            handler.conn.write_all(&wcmd).await?;
                        }

                        back_log.push(wcmd);
                    }
                    Letter::Psync { mut handle_replica, repl_id, repl_offset } => {
                        if repl_id == conf.server.run_id && let Some(gap_data) = back_log.get_gap_data(repl_offset) {
                            // 如果id匹配且BackLog中有足够的数据，则进行增量复制
                            handle_replica.conn.write_frame(&Resp3::<&[u8], _>::new_simple_string("CONTINUE")).await?;
                            handle_replica.conn.write_all(gap_data).await?;

                            // 允许接收从节点的所有命令
                            handle_replica.context.ac = Arc::new(AccessControl::new_loose());

                            replica_handlers.push(handle_replica);

                            // 丢弃之前的futs，重新构建futs，futs一定不为空
                            futs = select_all(
                                unsafe { &mut *replica_handlers_ptr }
                                    .iter_mut()
                                    .map(|handler| handler.conn.read_frame().timeout(timeout).boxed()),
                            );
                        } else {
                            let repl_id = conf.server.run_id.clone();
                            let repl_offset = back_log.offset;

                            // 全量复制可能非常耗时，因此放到新线程中执行，执行完毕后
                            // 再次发送Psync请求，直到可以进行增量复制。为了避免重复
                            // 执行全量复制，BackLog的大小应该足够大
                            let handle = tokio::runtime::Handle::current();
                            std::thread::spawn(move || {
                                handle.block_on(async move {
                                    full_sync(&mut handle_replica).await.unwrap();

                                    post_office
                                        .get_outbox(SET_MASTER_ID)
                                        .unwrap()
                                        .send(Letter::Psync {
                                            handle_replica,
                                            repl_id,
                                            repl_offset,
                                        })
                                        .ok();
                                });
                            });
                        }

                    }
                    // TODO:
                    // RemoveReplica。如果remove后，replica_handlers为空，则
                    // futs = select_all([async { Ok(ping_frame) }.boxed()]);
                    Letter::Resp3(_) => {}
                }
            },
            (res, i, mut rest) = &mut futs, if !replica_handlers.is_empty() => {
                match res {
                    // 超时，连接出错，或者对方关闭连接，则移除该handler
                    Err(_) | Ok(Err(_)) | Ok(Ok(None)) => {
                        replica_handlers.remove(i);
                    }
                    Ok(Ok(Some(frame))) => {
                        let handler = unsafe { &mut (*replica_handlers_ptr)[i] };

                        handler.context.back_log = Some(back_log); // set back_log

                        if let Some(resp) = handler.dispatch(frame).await? {
                            handler.conn.write_frame(&resp).await?;
                        }

                        back_log = handler.context.back_log.take().unwrap(); // return back back_log

                        rest.insert(i, handler.conn.read_frame().timeout(timeout).boxed());
                    }
                }

                if rest.is_empty() {
                    rest = vec![async {
                         Ok::<Result<Option<Resp3>, RutinError>, Elapsed>(Ok(Some(ping_frame.clone())))
                    }.boxed()];
                }

                futs = select_all(rest);
            }
            _ = interval.tick(), if !replica_handlers.is_empty() => {
                for handler in replica_handlers.iter_mut() {
                    handler.conn.write_frame(&ping_frame).await?;
                }
            }
        }
    }
}

async fn full_sync(handle_replica: &mut Handler<TcpStream>) -> RutinResult<()> {
    let shared = handle_replica.shared;
    let rdb_data = rdb_save(shared.db(), false)
        .await
        .map_err(|e| RutinError::new_server_error(e.to_string()))?;

    let len = rdb_data.len();

    handle_replica
        .conn
        .write_all(&format!("${}\r\n", len).into_bytes())
        .await?;

    handle_replica.conn.write_all(&rdb_data).await?;

    Ok(())
}
