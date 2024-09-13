use crate::{
    cmd::commands::{AUTH_CMD_FLAG, PSYNC_CMD_FLAG, REPLCONF_CMD_FLAG},
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
use std::{future::pending, sync::Arc, time::Duration};
use tokio::{net::TcpStream, time::error::Elapsed};
use tokio_util::time::FutureExt as _;

#[derive(Debug)]
pub struct BackLog {
    pub buf: BytesMut,
    cap: u64,
    pub offset: u64,
}

impl BackLog {
    pub fn new(cap: u64) -> Self {
        Self {
            buf: BytesMut::with_capacity(cap as usize),
            cap,
            offset: 0,
        }
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.offset = 0;
    }

    // 主节点向从节点发送的所有命令都会被记录到BackLog中(包括PING和REPLCONF命令)
    pub fn push(&mut self, wcmd: BytesMut) {
        self.offset = self.offset.wrapping_add(wcmd.len() as u64);

        self.buf.unsplit(wcmd);

        if self.buf.len() > self.cap as usize {
            self.buf.advance(self.buf.len() - self.cap as usize);
        }

        debug_assert!(self.buf.len() <= self.cap as usize);
    }

    pub fn get_gap_data(&self, repl_offset: u64) -> Option<&[u8]> {
        let gap = self.offset.saturating_sub(repl_offset);

        if gap == 0 {
            Some(&[])
        } else if gap <= self.cap {
            self.buf.get((self.buf.len() - gap as usize)..)
        } else {
            None
        }
    }
}

// 执行该函数后，服务应满足：
// 1. 不存在SET_REPLICA任务
// 2. post_office中存在SET_MASTER_ID mailbox以及set_master_outbox
// 3. 服务会定期向从节点发送PING命令，以便从节点知道主节点是否存活
pub async fn set_server_to_master(shared: Shared, master_conf: MasterConf) -> RutinResult<()> {
    let conf = shared.conf();
    let post_office = shared.post_office();

    let set_master_inbox = if let Some(inbox) = post_office.get_inbox(SET_MASTER_ID) {
        inbox
    } else {
        // 创建SET_MASTER_ID mailbox但先不设置set_master_outbox，直到有从节点连接
        post_office.new_mailbox_with_special_id(SET_MASTER_ID).1
    };

    // 记录最近的写命令
    let mut back_log = BackLog::new(master_conf.backlog_size);

    let mut replica_handlers = Vec::<Handler<TcpStream>>::new();
    let replica_handlers_ptr = &mut replica_handlers as *mut Vec<Handler<TcpStream>>;

    let ping_replica_period = Duration::from_secs(master_conf.ping_replica_period);
    let mut ping_interval = tokio::time::interval(ping_replica_period);
    let ping_frame = Resp3::<&[u8; 4], String>::new_array(vec![Resp3::new_blob_string(b"PING")]);
    let ping_frame_encoded = ping_frame.encode();

    let timeout = Duration::from_secs(master_conf.timeout);

    // select_all的数组中必须有一个future，否则会panic，因此放入一个永远不会完成的future
    let mut futs =
        select_all([pending::<Result<Result<Option<Resp3>, RutinError>, Elapsed>>().boxed_local()]);

    loop {
        tokio::select! {
            letter = set_master_inbox.recv_async() => {
                match letter {
                    Letter::ShutdownServer | Letter::Reset => {
                        // 如果set_master_outbox存在，则关闭SetMaster任务时，清除set_master_outbox
                        if post_office.set_master_outbox().is_some() {
                            post_office.set_set_master_outbox(None).await;
                        }

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
                            dbg!(&wcmd);
                            handler.conn.write_all(&wcmd).await?;
                            handler.conn.write_all(&wcmd).await?;
                        }

                        back_log.push(wcmd);
                    }
                    Letter::Psync { mut handle_replica, repl_id, repl_offset } => {
                        let run_id = conf.server.run_id.clone();
                        let master_offset = back_log.offset;

                        // 设置ac
                        handle_replica.context.ac = Arc::new(get_handle_replica_ac());

                        if conf.security.requirepass.is_some() {
                            let auth = handle_replica.conn.read_frame().await?.ok_or(RutinError::from("ERR require auth"))?;
                            if let Some(resp) = handle_replica.dispatch(auth).await? {
                                handle_replica.conn.write_frame(&resp).await?;

                                if resp.is_simple_error() {
                                    // 认证失败
                                    continue;
                                }
                            }
                        }

                        if repl_id == run_id && let Some(gap_data) = back_log.get_gap_data(repl_offset) {
                            // 如果id匹配且BackLog中有足够的数据，则进行增量复制

                            // 返回"+CONTINUE"
                            handle_replica
                                .conn
                                .write_frame(&Resp3::<&[u8], _>::new_simple_string("CONTINUE"))
                                .await?;

                            handle_replica.conn.write_all(gap_data).await?;


                            replica_handlers.push(handle_replica);

                            // 有从节点连接但set_master_outbox不存在，则设置set_master_outbox
                            if post_office.set_master_outbox().is_none() {
                                post_office.set_set_master_outbox(post_office.get_outbox(SET_MASTER_ID)).await;
                            }

                            // 丢弃之前的futs，重新构建futs，futs一定不为空
                            futs = select_all(
                                unsafe { &mut *replica_handlers_ptr }
                                    .iter_mut()
                                    .map(|handler| handler.conn.read_frame().timeout(timeout).boxed_local()),
                            );
                        } else {
                            // 如果id不匹配或者BackLog中没有足够的数据，则进行全量复制

                            // 返回"+FULLRESYNC <run_id> <master_offset>"
                            handle_replica
                                .conn
                                .write_frame(&Resp3::<&[u8], _>::new_simple_string(format!(
                                    "FULLRESYNC {:?} {}",
                                    conf.server.run_id, master_offset
                                )))
                                .await?;

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
                                            repl_id: run_id,
                                            repl_offset: master_offset,
                                        })
                                        .ok();
                                });
                            });
                        }


                    }
                    // TODO:
                    // RemoveReplica。如果remove后，replica_handlers为空，则
                    // futs = select_all([async { Ok(ping_frame) }.boxed_local()]);
                    Letter::Resp3(_) => {}
                }
            },
            // 接收从节点的命令
            (res, i, mut rest) = &mut futs, if !replica_handlers.is_empty() => {
                match res {
                    // 超时，连接出错，或者对方关闭连接，则移除该handler
                    Err(_) | Ok(Err(_)) | Ok(Ok(None)) => {
                        replica_handlers.remove(i);

                        if replica_handlers.is_empty() {
                            // 如果没有从节点但set_master_outbox存在，则清除set_master_outbox
                            if post_office.set_master_outbox().is_some() {
                                post_office.set_set_master_outbox(None).await;
                            }

                            // 重置BackLog
                            back_log.reset();
                        }
                    }
                    Ok(Ok(Some(frame))) => {
                        let handler = unsafe { &mut (*replica_handlers_ptr)[i] };

                        handler.context.back_log = Some(back_log); // set back_log

                        if let Some(resp) = handler.dispatch(frame).await? {
                            handler.conn.write_frame(&resp).await?;
                        }

                        back_log = handler.context.back_log.take().unwrap(); // return back back_log

                        rest.insert(i, handler.conn.read_frame().timeout(timeout).boxed_local());
                    }
                }

                futs = select_all(rest);
            }
            // 向从节点发送PING命令
            _ = ping_interval.tick(), if !replica_handlers.is_empty() => {
                for handler in replica_handlers.iter_mut() {
                    handler.conn.write_frame(&ping_frame).await?;
                    back_log.push(ping_frame_encoded.clone());
                    // 不需要等待响应，因为从节点不会返回响应
                }
            }
        }
    }
}

fn get_handle_replica_ac() -> AccessControl {
    let mut ac = AccessControl::new_strict();

    ac.allow_cmds(AUTH_CMD_FLAG | REPLCONF_CMD_FLAG | PSYNC_CMD_FLAG);

    ac
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
