use crate::{
    cmd::commands::{AUTH_CMD_FLAG, PSYNC_CMD_FLAG, REPLCONF_CMD_FLAG},
    conf::{AccessControl, MasterConf},
    error::{RutinError, RutinResult},
    frame::{Resp3, StaticResp3},
    persist::rdb::encode_rdb,
    server::Handler,
    shared::{Letter, Shared, SET_MASTER_ID},
};
use bytes::{Buf, BytesMut};
use event_listener::{listener, IntoNotification};
use futures::{future::select_all, FutureExt};
use std::{future::pending, sync::Arc, time::Duration};
use tokio::{net::TcpStream, time::error::Elapsed};
use tokio_util::time::FutureExt as _;
use tracing::instrument;

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
// 2. post_office中存在SET_MASTER_ID mailbox以及set_master_outbox字段
// 3. 服务会定期向从节点发送PING命令，以便从节点知道主节点是否存活
pub fn spawn_set_server_to_master(shared: Shared, master_conf: Arc<MasterConf>) {
    #[inline]
    #[instrument(level = "debug", skip(shared), err)]
    pub async fn _set_server_to_master(
        shared: Shared,
        master_conf: Arc<MasterConf>,
    ) -> RutinResult<()> {
        let conf = shared.conf();
        let post_office = shared.post_office();

        // SET_REPLICA_ID 和 SET_MASTER_ID 任务不能同时存在
        conf.update_replica_conf(
            &mut |mut replica_conf| {
                replica_conf = None;
                replica_conf
            },
            shared,
        );

        let mailbox = post_office.register_mailbox(SET_MASTER_ID);

        // 记录最近的写命令
        let mut back_log = BackLog::new(master_conf.backlog_size);

        let mut replica_handlers = Vec::<Handler<TcpStream>>::new();
        let replica_handlers_ptr = &mut replica_handlers as *mut Vec<Handler<TcpStream>>;

        let ping_replica_period = Duration::from_millis(master_conf.ping_replica_period_ms);
        let mut ping_interval = tokio::time::interval(ping_replica_period);
        let ping_frame =
            Resp3::<&[u8; 4], String>::new_array(vec![Resp3::new_blob_string(b"PING")]);
        let ping_frame_encoded: BytesMut = ping_frame.encode();

        let timeout = Duration::from_secs(master_conf.timeout_ms);

        // select_all的数组中必须有一个future，否则会panic，因此放入一个永远不会完成的future
        let mut futs = select_all([pending::<
            Result<Result<Option<StaticResp3>, RutinError>, Elapsed>,
        >()
        .boxed_local()]);

        loop {
            tokio::select! {
                letter = mailbox.recv_async() => {
                    match letter {
                        Letter::Shutdown => {
                            return Ok(());
                        }
                        Letter::Block { unblock_event } => {
                            unblock_event.notify(1.additional());
                            listener!(unblock_event  => listener);
                            listener.await;
                        }
                        Letter::Wcmd(wcmd) => {
                            if replica_handlers.is_empty() {
                                continue;
                            }

                            for handler in replica_handlers.iter_mut() {
                                handler.conn.write_all(&wcmd).await?;
                                handler.conn.write_all(&wcmd).await?;
                            }

                            back_log.push(wcmd);
                        }
                        // 执行Psync命令时，要求db与back_log的状态保持一致，并且db不会被修改
                        Letter::Psync { mut handle_replica, repl_id, repl_offset } => {
                            // 设置ac
                            handle_replica.context.ac = Arc::new(get_handle_replica_ac());

                            if conf.security_conf().requirepass.is_some() {
                                let auth = handle_replica.conn.read_frame().await?.ok_or(RutinError::from("ERR require auth"))?;
                                if let Some(resp) = handle_replica.dispatch(auth).await? {
                                    handle_replica.conn.write_frame(&resp).await?;

                                    if resp.is_simple_error() {
                                        // 认证失败
                                        continue;
                                    }
                                }
                            }

                            // 阻塞所有任务，防止Db被修改，防止送来更多的Wcmd
                            let _guard = post_office.send_block_all(SET_MASTER_ID);

                            // 有可能接收到Psync后仍有Wcmd在其后，这些Wcmd已经作用于Db，
                            // 因此back_log需要反应这一点
                            let mut letter_buf = vec![];
                            while let Ok(letter) = mailbox.inbox.try_recv() {
                                match letter {
                                    // 接收剩余的wcmd，保持back_log与db的状态一致
                                    Letter::Wcmd(wcmd) => {
                                        if replica_handlers.is_empty() {
                                            continue;
                                        }

                                        for handler in replica_handlers.iter_mut() {
                                            handler.conn.write_all(&wcmd).await?;
                                            handler.conn.write_all(&wcmd).await?;
                                        }

                                        back_log.push(wcmd);
                                    }
                                    letter => {
                                        letter_buf.push(letter);
                                    }
                                }
                            }

                            // 重新放回非wcmd的letter
                            for letter in letter_buf {
                                mailbox.send(letter);
                            }

                            let run_id = &conf.server_conf().run_id;
                            let master_offset = back_log.offset;


                            if repl_id == run_id && let Some(gap_data) = back_log.get_gap_data(repl_offset) {
                                // 如果id匹配且BackLog中有足够的数据，则进行增量复制

                                // 返回"+CONTINUE"
                                handle_replica
                                    .conn
                                    .write_frame(&Resp3::<&[u8], String>::new_simple_string("CONTINUE"))
                                    .await?;

                                handle_replica.conn.write_all(gap_data).await?;


                                replica_handlers.push(handle_replica);

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
                                    .write_frame(&Resp3::<&[u8], String>::new_simple_string(format!(
                                        "FULLRESYNC {:?} {}",
                                        run_id, master_offset
                                    )))
                                    .await?;

                                full_sync(&mut handle_replica).await.unwrap();

                                replica_handlers.push(handle_replica);

                                // 丢弃之前的futs，重新构建futs，futs一定不为空
                                futs = select_all(
                                    unsafe { &mut *replica_handlers_ptr }
                                        .iter_mut()
                                        .map(|handler| handler.conn.read_frame().timeout(timeout).boxed_local()),
                                );
                            }
                        }
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

    shared.pool().spawn_pinned(move || async move {
        _set_server_to_master(shared, master_conf).await.ok();
    });
}

fn get_handle_replica_ac() -> AccessControl {
    let mut ac = AccessControl::new_strict();

    ac.allow_cmds(AUTH_CMD_FLAG | REPLCONF_CMD_FLAG | PSYNC_CMD_FLAG);

    ac
}

async fn full_sync(handle_replica: &mut Handler<TcpStream>) -> RutinResult<()> {
    let shared = handle_replica.shared;
    let rdb_data = encode_rdb(shared.db(), false).await;

    let len = rdb_data.len();

    handle_replica
        .conn
        .write_all(&format!("${}\r\n", len).into_bytes())
        .await?;

    handle_replica.conn.write_all(&rdb_data).await?;

    Ok(())
}
