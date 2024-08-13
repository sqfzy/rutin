use crate::{
    error::{RutinError, RutinResult},
    frame::Resp3,
    persist::aof::{Aof, AppendFSync},
    server::Handler,
    shared::{Inbox, Letter, Shared},
};
use bytes::{Buf, BytesMut};
use event_listener::listener;
use futures::{future::select_all, FutureExt};
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpStream};

pub async fn set_server_to_master(shared: &Shared) {
    let conf = shared.conf();

    let mut ms_info = conf.replica.master_info.lock().await;
    if ms_info.is_none() {
        return;
    } else {
        *ms_info = None;
    }

    // 断开Psync中的连接
    shared
        .post_office()
        .send_all(Letter::ShutdownReplicas)
        .await;
}

pub struct BackLog {
    buf: BytesMut,
    cap: u64,
    offset: u64,
}

impl BackLog {
    fn new() -> Self {
        let cap = 1024 * 1024; // 1MB;
        Self {
            buf: BytesMut::zeroed(cap as usize),
            cap,
            offset: 0,
        }
    }

    pub fn push(&mut self, wcmd: BytesMut) {
        self.offset += wcmd.len() as u64;

        self.buf.advance(wcmd.len());
        self.buf.unsplit(wcmd);

        debug_assert_eq!(self.cap as usize, self.buf.len());
    }

    pub fn get_gap_data(&self, gap: u64) -> Option<&[u8]> {
        if gap <= self.cap {
            self.buf.get(gap as usize..)
        } else {
            None
        }
    }
}

struct SendWrapper(*mut Vec<Handler<TcpStream>>);

unsafe impl Send for SendWrapper {}

pub async fn propagate_wcmd_to_replicas(wcmd_inbox: Inbox) -> RutinResult<()> {
    // 记录最近的写命令
    let mut back_log: BackLog = BackLog::new();

    let mut replica_handlers = Vec::<Handler<TcpStream>>::new();
    let replica_handlers_ptr = SendWrapper(&mut replica_handlers as *mut Vec<Handler<TcpStream>>);

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let ping_frame = Resp3::new_array(vec![Resp3::new_blob_string("PING".into())]);

    // select_all的数组中必须有一个future，否则会panic
    let mut futs =
        select_all([async { Ok::<Option<Resp3>, RutinError>(Some(ping_frame.clone())) }.boxed()]);

    loop {
        tokio::select! {
            letter = wcmd_inbox.recv_async() => {
                match letter {
                    Letter::ShutdownServer => {
                        return Ok(());
                    }
                    Letter::BlockAll { unblock_event } => {
                        listener!(unblock_event  => listener);
                        listener.await;
                    }
                    Letter::Wcmd(wcmd) => {
                        for handler in replica_handlers.iter_mut() {
                            handler.conn.write_all(&wcmd).await?;
                        }
                        back_log.push(wcmd);
                    }
                    Letter::AddReplica(handler) => {
                        replica_handlers.push(handler);

                        // 丢弃之前的futs，重新构建futs，futs一定不为空
                        futs = unsafe {
                            select_all(
                                (*replica_handlers_ptr.0)
                                    .iter_mut()
                                    .map(|handler| handler.conn.read_frame().boxed()),
                            )
                        };
                    }
                    // TODO:
                    // RemoveReplica。如果remove后，replica_handlers为空，则
                    // futs = select_all([async { Ok(ping_frame) }.boxed()]);
                    _ => {}
                }
            },
            (res, i, mut rest) = &mut futs, if !replica_handlers.is_empty() => {
                if let Some(frame) = res? {
                    let handler = unsafe { &mut (*replica_handlers_ptr.0)[i] };

                    handler.back_log = Some(back_log); // set back_log

                    if let Some(resp) = handler.dispatch(frame).await? {
                        handler.conn.write_frame(&resp).await?;
                    }

                    back_log = handler.back_log.take().unwrap(); // return back back_log

                    rest.insert(i, handler.conn.read_frame().boxed());
                } else {
                    replica_handlers.remove(i);
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

// 为了避免在shutdown的时候，还有数据没有写入到文件中，shutdown时必须等待该函数执行完毕
pub async fn save_and_propagate_wcmd_to_replicas(
    aof: &mut Aof,
    wcmd_inbox: Inbox,
) -> anyhow::Result<()> {
    let aof_conf = aof.shared.conf().aof.as_ref().unwrap();

    let mut curr_aof_size = 0_u128; // 单位为byte
    let auto_aof_rewrite_min_size = (aof_conf.auto_aof_rewrite_min_size as u128) << 20;

    let mut back_log: BackLog = BackLog::new();

    let mut replica_handlers = Vec::<Handler<TcpStream>>::new();
    let replica_handlers_ptr = SendWrapper(&mut replica_handlers as *mut Vec<Handler<TcpStream>>);

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let ping_frame = Resp3::new_array(vec![Resp3::new_blob_string("PING".into())]);

    // select_all的数组中必须有一个future，否则会panic
    let mut futs =
        select_all([async { Ok::<Option<Resp3>, RutinError>(Some(ping_frame.clone())) }.boxed()]);

    match aof_conf.append_fsync {
        AppendFSync::Always => loop {
            tokio::select! {
                biased;

                letter = wcmd_inbox.recv_async() => match letter {
                    Letter::ShutdownServer => {
                        break;
                    }
                    Letter::BlockAll { unblock_event } => {
                        listener!(unblock_event  => listener);
                        listener.await;
                    }
                    Letter::Wcmd(wcmd) => {
                        curr_aof_size += wcmd.len() as u128;

                        aof.file.write_all(&wcmd).await?;
                        aof.file.sync_data().await?;

                        if !replica_handlers.is_empty() {
                            for handler in replica_handlers.iter_mut() {
                                handler.conn.write_all(&wcmd).await?;
                            }
                        }

                        if curr_aof_size >= auto_aof_rewrite_min_size {
                            aof.rewrite().await?;
                            curr_aof_size = 0;
                        }

                        back_log.push(wcmd);
                    }
                    Letter::AddReplica(handler) => {
                        replica_handlers.push(handler);

                        // 丢弃之前的futs，重新构建futs，futs一定不为空
                        futs = unsafe {
                            select_all(
                                (*replica_handlers_ptr.0)
                                    .iter_mut()
                                    .map(|handler| handler.conn.read_frame().boxed()),
                            )
                        };
                    }
                    _ => {}
                },
                (res, i, mut rest) = &mut futs, if !replica_handlers.is_empty() => {
                    if let Some(frame) = res? {
                        let handler = unsafe { &mut (*replica_handlers_ptr.0)[i] };

                        handler.back_log = Some(back_log); // set back_log

                        if let Some(resp) = handler.dispatch(frame).await? {
                            handler.conn.write_frame(&resp).await?;
                        }

                        back_log = handler.back_log.take().unwrap(); // return back back_log

                        rest.insert(i, handler.conn.read_frame().boxed());
                    } else {
                        replica_handlers.remove(i);
                    }

                    futs = select_all(rest);
                }
                _ = interval.tick(), if !replica_handlers.is_empty() => {
                    for handler in replica_handlers.iter_mut() {
                        handler.conn.write_frame(&ping_frame).await?;
                    }
                }
            }
        },

        AppendFSync::EverySec => loop {
            tokio::select! {
                biased;

                letter = wcmd_inbox.recv_async() => match letter {
                    Letter::ShutdownServer => {
                        break;
                    }
                    Letter::BlockAll { unblock_event } => {
                        listener!(unblock_event  => listener);
                        listener.await;
                    }
                    Letter::Wcmd(wcmd) => {
                        curr_aof_size += wcmd.len() as u128;

                        aof.file.write_all(&wcmd).await?;

                        if !replica_handlers.is_empty() {
                            for handler in replica_handlers.iter_mut() {
                                handler.conn.write_all(&wcmd).await?;
                            }
                        }

                        if curr_aof_size >= auto_aof_rewrite_min_size {
                            aof.rewrite().await?;
                            curr_aof_size = 0;
                        }

                        back_log.push(wcmd);
                    }
                    Letter::AddReplica(handler) => {
                        replica_handlers.push(handler);

                        // 丢弃之前的futs，重新构建futs，futs一定不为空
                        futs = unsafe {
                            select_all(
                                (*replica_handlers_ptr.0)
                                    .iter_mut()
                                    .map(|handler| handler.conn.read_frame().boxed()),
                            )
                        };
                    }
                    _ => {}
                },
                (res, i, mut rest) = &mut futs, if !replica_handlers.is_empty() => {
                    if let Some(frame) = res? {
                        let handler = unsafe { &mut (*replica_handlers_ptr.0)[i] };

                        handler.back_log = Some(back_log); // set back_log

                        if let Some(resp) = handler.dispatch(frame).await? {
                            handler.conn.write_frame(&resp).await?;
                        }

                        back_log = handler.back_log.take().unwrap(); // return back back_log

                        rest.insert(i, handler.conn.read_frame().boxed());
                    } else {
                        replica_handlers.remove(i);
                    }

                    futs = select_all(rest);
                }
                // 每隔一秒，同步文件
                _ = interval.tick() => {
                    aof.file.sync_data().await?;

                    if !replica_handlers.is_empty() {
                        for handler in replica_handlers.iter_mut() {
                            handler.conn.write_frame(&ping_frame).await?;
                        }
                    }
                }
            }
        },

        AppendFSync::No => loop {
            tokio::select! {
                biased;

                letter = wcmd_inbox.recv_async() => match letter {
                    Letter::ShutdownServer => {
                        break;
                    }
                    Letter::BlockAll { unblock_event } => {
                        listener!(unblock_event  => listener);
                        listener.await;
                    }
                    Letter::Wcmd(wcmd) => {
                        curr_aof_size += wcmd.len() as u128;

                        aof.file.write_all(&wcmd).await?;

                        if !replica_handlers.is_empty() {
                            for handler in replica_handlers.iter_mut() {
                                handler.conn.write_all(&wcmd).await?;
                            }
                        }

                        if curr_aof_size >= auto_aof_rewrite_min_size {
                            aof.rewrite().await?;
                            curr_aof_size = 0;
                        }

                        back_log.push(wcmd);
                    }
                    Letter::AddReplica(handler) => {
                        replica_handlers.push(handler);

                        // 丢弃之前的futs，重新构建futs，futs一定不为空
                        futs = unsafe {
                            select_all(
                                (*replica_handlers_ptr.0)
                                    .iter_mut()
                                    .map(|handler| handler.conn.read_frame().boxed()),
                            )
                        };
                    }
                    Letter::Resp3(_) | Letter::BlockServer { .. } | Letter::ShutdownClient | Letter::ShutdownReplicas => { }
                },
                (res, i, mut rest) = &mut futs, if !replica_handlers.is_empty() => {
                    if let Some(frame) = res? {
                        let handler = unsafe { &mut (*replica_handlers_ptr.0)[i] };

                        handler.back_log = Some(back_log); // set back_log

                        if let Some(resp) = handler.dispatch(frame).await? {
                            handler.conn.write_frame(&resp).await?;
                        }

                        back_log = handler.back_log.take().unwrap(); // return back back_log

                        rest.insert(i, handler.conn.read_frame().boxed());
                    } else {
                        replica_handlers.remove(i);
                    }

                    futs = select_all(rest);
                }
                _ = interval.tick(), if !replica_handlers.is_empty() => {
                    for handler in replica_handlers.iter_mut() {
                        handler.conn.write_frame(&ping_frame).await?;
                    }
                }
            }
        },
    }

    while let Ok(Letter::Wcmd(wcmd)) = wcmd_inbox.inner.try_recv() {
        aof.file.write_all(&wcmd).await?;
    }

    aof.file.sync_data().await?;
    aof.rewrite().await?; // 最后再重写一次
    tracing::info!("AOF file rewrited.");
    Ok(())
}
