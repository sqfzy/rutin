use crate::{
    error::RutinResult,
    persist::aof::{Aof, AppendFSync},
    server::Handler,
    shared::{Inbox, Letter, Shared},
};
use bytes::{Buf, BytesMut};
use event_listener::listener;
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

unsafe impl Send for BackLog {}
unsafe impl Sync for BackLog {}

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

pub async fn propagate_wcmd_to_replicas(wcmd_inbox: Inbox) -> RutinResult<()> {
    // 记录最近的写命令
    let mut back_log: BackLog = BackLog::new();

    let mut replica_handlers: Vec<Handler<TcpStream>> = Vec::new();

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        if replica_handlers.is_empty() {
            match wcmd_inbox.recv_async().await {
                Letter::ShutdownServer => {
                    break;
                }
                Letter::BlockAll { event } => {
                    listener!(event => listener);
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
                }
                _ => {}
            }
        } else {
            tokio::select! {
                biased;

                letter = wcmd_inbox.recv_async() => match letter {
                    Letter::ShutdownServer => {
                        break;
                    }
                    Letter::BlockAll { event } => {
                        listener!(event => listener);
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
                    }
                    _ => {}
                },
                _ = interval.tick() => {
                    for handler in &mut replica_handlers {
                        if let Ok(frame) = handler.conn.try_read_frame().await {
                            if let Some(f) = frame {
                                handler.back_log = Some(back_log); // set back_log
                                if let Some(resp) = handler.dispatch(f).await? {
                                    handler.conn.write_frame(&resp).await?;
                                }
                                back_log = handler.back_log.take().unwrap(); // return back back_log
                            }
                        } else {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    Ok(())
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

    let mut replica_handlers: Vec<Handler<TcpStream>> = Vec::new();

    match aof_conf.append_fsync {
        AppendFSync::Always => {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    biased;

                    letter = wcmd_inbox.recv_async() => match letter {
                        Letter::ShutdownServer => {
                            break;
                        }
                        Letter::BlockAll { event } => {
                            listener!(event => listener);
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
                        }
                        _ => {}
                    },
                    _ = interval.tick() => {
                        if replica_handlers.is_empty() {
                            continue;
                        }

                        for handler in &mut replica_handlers {
                            if let Ok(frame) = handler.conn.try_read_frame().await {
                                if let Some(f) = frame {
                                    handler.back_log = Some(back_log); // set back_log
                                    if let Some(resp) = handler.dispatch(f).await? {
                                        handler.conn.write_frame(&resp).await?;
                                    }
                                    back_log = handler.back_log.take().unwrap(); // return back back_log
                                }
                            } else {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
        AppendFSync::EverySec => {
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    biased;

                    letter = wcmd_inbox.recv_async() => match letter {
                        Letter::ShutdownServer => {
                            break;
                        }
                        Letter::BlockAll { event } => {
                            listener!(event => listener);
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
                        }
                        _ => {}
                    },
                    // 每隔一秒，同步文件
                    _ = interval.tick() => {
                        aof.file.sync_data().await?;

                        if replica_handlers.is_empty() {
                            continue;
                        }

                        for handler in &mut replica_handlers {
                            if let Ok(frame) = handler.conn.try_read_frame().await {
                                if let Some(f) = frame {
                                    handler.back_log = Some(back_log); // set back_log
                                    if let Some(resp) = handler.dispatch(f).await? {
                                        handler.conn.write_frame(&resp).await?;
                                    }
                                    back_log = handler.back_log.take().unwrap(); // return back back_log
                                }
                            } else {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
        AppendFSync::No => {
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    biased;

                    letter = wcmd_inbox.recv_async() => match letter {
                        Letter::ShutdownServer => {
                            break;
                        }
                        Letter::BlockAll { event } => {
                            listener!(event => listener);
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
                        }
                        Letter::Resp3(_) | Letter::BlockServer { .. } | Letter::ShutdownClient | Letter::ShutdownReplicas => { }
                    },
                    _ = interval.tick() => {
                        if replica_handlers.is_empty() {
                            continue;
                        }

                        for handler in &mut replica_handlers {
                            if let Ok(frame) = handler.conn.try_read_frame().await {
                                if let Some(f) = frame {
                                    handler.back_log = Some(back_log); // set back_log
                                    if let Some(resp) = handler.dispatch(f).await? {
                                        handler.conn.write_frame(&resp).await?;
                                    }
                                    back_log = handler.back_log.take().unwrap(); // return back back_log
                                }
                            } else {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    while let Ok(Letter::Wcmd(wcmd)) = wcmd_inbox.inner.try_recv() {
        aof.file.write_all(&wcmd).await?;
    }

    aof.file.sync_data().await?;
    aof.rewrite().await?; // 最后再重写一次
    tracing::info!("AOF file rewrited.");
    Ok(())
}
