use crate::{
    error::{RutinError, RutinResult},
    frame::Resp3,
    persist::aof::{Aof, AppendFSync},
    server::Handler,
    shared::{Letter, Shared, WCMD_PROPAGATE_ID},
};
use bytes::{Buf, BytesMut};
use event_listener::listener;
use futures::{future::select_all, FutureExt};
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpStream, time::error::Elapsed};
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

    pub fn push(&mut self, wcmd: BytesMut) {
        self.offset += wcmd.len() as u64;

        self.buf.advance(wcmd.len());
        self.buf.unsplit(wcmd);

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

struct SendWrapper(*mut Vec<Handler<TcpStream>>);

// // Safety: SendWrapper不允许跨task使用
// unsafe impl Send for SendWrapper {}

pub async fn propagate_wcmd_to_replicas(shared: Shared) -> RutinResult<()> {
    let replica_conf = &shared.conf().replica;

    // 记录最近的写命令
    let mut back_log: BackLog = BackLog::new(replica_conf.repl_backlog_size);

    let wcmd_inbox = shared
        .post_office()
        .get_inbox(WCMD_PROPAGATE_ID)
        .ok_or(RutinError::from(
            "wcmd mailbox should exist when standalone is false",
        ))?;

    let mut replica_handlers = Vec::<Handler<TcpStream>>::new();
    let replica_handlers_ptr = SendWrapper(&mut replica_handlers as *mut Vec<Handler<TcpStream>>);

    let repl_ping_replica_period = Duration::from_secs(replica_conf.repl_ping_replica_period);
    let mut interval = tokio::time::interval(repl_ping_replica_period);
    let ping_frame = Resp3::new_array(vec![Resp3::new_blob_string("PING".into())]);

    let repl_timeout = Duration::from_secs(replica_conf.repl_timeout);

    // select_all的数组中必须有一个future，否则会panic
    let mut futs = select_all([async {
        Ok::<Result<Option<Resp3>, RutinError>, Elapsed>(Ok(Some(ping_frame.clone())))
    }
    .boxed()]);

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
                    Letter::Psync { handle_replica, repl_id, repl_offset } => {
                        todo!()
                        // replica_handlers.push(handler);
                        //
                        // // 丢弃之前的futs，重新构建futs，futs一定不为空
                        // futs = unsafe {
                        //     select_all(
                        //         (*replica_handlers_ptr.0)
                        //             .iter_mut()
                        //             .map(|handler| handler.conn.read_frame().timeout(repl_timeout).boxed()),
                        //     )
                        // };
                    }
                    // TODO:
                    // RemoveReplica。如果remove后，replica_handlers为空，则
                    // futs = select_all([async { Ok(ping_frame) }.boxed()]);
                    _ => {}
                }
            },
            (res, i, mut rest) = &mut futs, if !replica_handlers.is_empty() => {
                match res {
                    // 超时，连接出错，或者对方关闭连接，则移除该handler
                    Err(_) | Ok(Err(_)) | Ok(Ok(None)) => {
                        replica_handlers.remove(i);
                    }
                    Ok(Ok(Some(frame))) => {
                        let handler = unsafe { &mut (*replica_handlers_ptr.0)[i] };

                        handler.context.back_log = Some(back_log); // set back_log

                        if let Some(resp) = handler.dispatch(frame).await? {
                            handler.conn.write_frame(&resp).await?;
                        }

                        back_log = handler.context.back_log.take().unwrap(); // return back back_log

                        rest.insert(i, handler.conn.read_frame().timeout(repl_timeout).boxed());
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

// 为了避免在shutdown的时候，还有数据没有写入到文件中，shutdown时必须等待该函数执行完毕
pub async fn save_and_propagate_wcmd_to_replicas(
    aof: &mut Aof,
    shared: Shared,
) -> anyhow::Result<()> {
    let wcmd_inbox = shared
        .post_office()
        .get_inbox(WCMD_PROPAGATE_ID)
        .ok_or(anyhow::anyhow!(
            "wcmd mailbox should exist when aof is enabled",
        ))?;

    let aof_conf = aof.shared.conf().aof.as_ref().unwrap();
    let replica_conf = &shared.conf().replica;

    let mut curr_aof_size = 0_u128; // 单位为byte
    let auto_aof_rewrite_min_size = aof_conf.auto_aof_rewrite_min_size;

    let mut back_log: BackLog = BackLog::new(replica_conf.repl_backlog_size);

    let mut replica_handlers = Vec::<Handler<TcpStream>>::new();
    let replica_handlers_ptr = SendWrapper(&mut replica_handlers as *mut Vec<Handler<TcpStream>>);

    let repl_ping_replica_period = Duration::from_secs(replica_conf.repl_ping_replica_period);
    let mut interval = tokio::time::interval(repl_ping_replica_period);
    let ping_frame = Resp3::new_array(vec![Resp3::new_blob_string("PING".into())]);

    let repl_timeout = Duration::from_secs(replica_conf.repl_timeout);

    // select_all的数组中必须有一个future，否则会panic
    let mut futs = select_all([async {
        Ok::<Result<Option<Resp3>, RutinError>, Elapsed>(Ok(Some(ping_frame.clone())))
    }
    .boxed()]);

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
                    Letter::Psync { handle_replica, repl_id, repl_offset } => {
                        // if repl_id == shared.conf().server.run_id && let Some(gap_data) = back_log.get_gap_data(repl_offset) {
                        //     // id匹配且缺少的数据正好在back_log中，则进行增量复制
                        //     handle_replica.conn.write_all(gap_data).await?;
                        // } else {
                        //     // 进行全量复制
                        // }
                        //
                        // replica_handlers.push(handle_replica);
                        //
                        // // 丢弃之前的futs，重新构建futs，futs一定不为空
                        // futs = unsafe {
                        //     select_all(
                        //         (*replica_handlers_ptr.0)
                        //             .iter_mut()
                        //             .map(|handler| handler.conn.read_frame().timeout(repl_timeout).boxed()),
                        //     )
                        // };
                    }
                    _ => {}
                },
                (res, i, mut rest) = &mut futs, if !replica_handlers.is_empty() => {
                    match res {
                        // 超时，连接出错，或者对方关闭连接，则移除该handler
                        Err(_) | Ok(Err(_)) | Ok(Ok(None)) => {
                            replica_handlers.remove(i);
                        }
                        Ok(Ok(Some(frame))) => {
                            let handler = unsafe { &mut (*replica_handlers_ptr.0)[i] };

                            handler.context.back_log = Some(back_log); // set back_log

                            if let Some(resp) = handler.dispatch(frame).await? {
                                handler.conn.write_frame(&resp).await?;
                            }

                            back_log = handler.context.back_log.take().unwrap(); // return back back_log

                            rest.insert(i, handler.conn.read_frame().timeout(repl_timeout).boxed());
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
                    Letter::Psync { handle_replica, repl_id, repl_offset } => {
                        // replica_handlers.push(handler);
                        //
                        // // 丢弃之前的futs，重新构建futs，futs一定不为空
                        // futs = unsafe {
                        //     select_all(
                        //         (*replica_handlers_ptr.0)
                        //             .iter_mut()
                        //             .map(|handler| handler.conn.read_frame().timeout(repl_timeout).boxed()),
                        //     )
                        // };
                    }
                    _ => {}
                },
                (res, i, mut rest) = &mut futs, if !replica_handlers.is_empty() => {
                    match res {
                        // 超时，连接出错，或者对方关闭连接，则移除该handler
                        Err(_) | Ok(Err(_)) | Ok(Ok(None)) => {
                            replica_handlers.remove(i);
                        }
                        Ok(Ok(Some(frame))) => {
                            let handler = unsafe { &mut (*replica_handlers_ptr.0)[i] };

                            handler.context.back_log = Some(back_log); // set back_log

                            if let Some(resp) = handler.dispatch(frame).await? {
                                handler.conn.write_frame(&resp).await?;
                            }

                            back_log = handler.context.back_log.take().unwrap(); // return back back_log

                            rest.insert(i, handler.conn.read_frame().timeout(repl_timeout).boxed());
                        }
                    }

                    if rest.is_empty() {
                        rest = vec![async {
                             Ok::<Result<Option<Resp3>, RutinError>, Elapsed>(Ok(Some(ping_frame.clone())))
                        }.boxed()];
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
                    Letter::Psync { handle_replica, repl_id, repl_offset } => {
                        // replica_handlers.push(handler);
                        //
                        // // 丢弃之前的futs，重新构建futs，futs一定不为空
                        // futs = unsafe {
                        //     select_all(
                        //         (*replica_handlers_ptr.0)
                        //             .iter_mut()
                        //             .map(|handler| handler.conn.read_frame().timeout(repl_timeout).boxed()),
                        //     )
                        // };
                    }
                    Letter::Resp3(_) | Letter::BlockServer { .. } | Letter::ShutdownClient | Letter::ShutdownReplicas => { }
                },
                (res, i, mut rest) = &mut futs, if !replica_handlers.is_empty() => {
                    match res {
                        // 超时，连接出错，或者对方关闭连接，则移除该handler
                        Err(_) | Ok(Err(_)) | Ok(Ok(None)) => {
                            replica_handlers.remove(i);
                        }
                        Ok(Ok(Some(frame))) => {
                            let handler = unsafe { &mut (*replica_handlers_ptr.0)[i] };

                            handler.context.back_log = Some(back_log); // set back_log

                            if let Some(resp) = handler.dispatch(frame).await? {
                                handler.conn.write_frame(&resp).await?;
                            }

                            back_log = handler.context.back_log.take().unwrap(); // return back back_log

                            rest.insert(i, handler.conn.read_frame().timeout(repl_timeout).boxed());
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
