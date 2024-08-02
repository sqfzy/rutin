use std::sync::{atomic::AtomicI32, Arc};

use crate::{
    conf::{AccessControl, MasterInfo},
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{Handler, RESET_SIGNAL},
    shared::{Message, Shared, SignalManager},
    Id,
};
use async_shutdown::ShutdownManager;
use bytes::Bytes;
use bytestring::ByteString;
use crossbeam::queue::ArrayQueue;
use flume::Receiver;
use tokio::{net::TcpStream, sync::Semaphore};

pub fn set_server_to_master(shared: &Shared) {
    let conf = shared.conf();

    // if conf.replica.master_addr.load().is_none() {
    //     return;
    // }
    //
    // conf.replica.master_addr.store(None);
}

// 该函数是互斥函数，只允许一个异步任务进入。
pub async fn set_server_to_replica(
    shared: &Shared,
    master_host: ByteString,
    master_port: u16,
) -> RutinResult<()> {
    let conf = shared.conf();

    // 该函数需要通知所有Handler结束事件循环，因此该函数不能造成阻塞，否则会导致Handler无法返回
    let ms_info = conf
        .replica
        .master_info
        .try_lock()
        .map_err(|_| "ERR another replica operation is in progress, please try again later")?;

    // 主节点地址未变化
    if let Some(ms_info) = ms_info.as_ref()
        && master_host == ms_info.host
        && master_port == ms_info.port
    {
        return Ok(());
    }

    /* 与主节点握手建立连接 */

    let to_master = TcpStream::connect((master_host.as_ref(), master_port)).await?;
    let mut to_master_handler = Handler::new(shared.clone(), to_master);

    // 发送PING测试主节点是否可达
    to_master_handler
        .conn
        .write_frame_force(&Resp3::<_, String>::new_blob_string(b"PING"))
        .await?;

    if !to_master_handler
        .conn
        .read_frame()
        .await?
        .is_some_and(|frame| {
            frame
                .try_simple_string()
                .is_some_and(|frame| frame == "PONG")
        })
    {
        return Err(RutinError::from(format!(
            "ERR master {}:{} is unreachable or busy",
            master_host, master_port
        )));
    };

    // 身份验证(可选)
    if let Some(password) = conf.replica.master_auth.as_ref() {
        to_master_handler
            .conn
            .write_frame_force(&Resp3::<_, String>::new_array(vec![
                Resp3::new_blob_string(b"AUTH".to_vec()),
                Resp3::new_blob_string(password.clone().into_bytes()),
            ]))
            .await?;

        if to_master_handler
            .conn
            .read_frame()
            .await?
            .is_some_and(|frame| frame.is_simple_error())
        {
            return Err(RutinError::from("ERR master authentication failed"));
        }
    }

    // 发送端口信息
    to_master_handler
        .conn
        .write_frame_force(&Resp3::<_, String>::new_array(vec![
            Resp3::new_blob_string(b"REPLCONF".to_vec()),
            Resp3::new_blob_string(b"listening-port".to_vec()),
            Resp3::new_blob_string(conf.server.port.to_string().into_bytes()),
        ]))
        .await?;

    // 发送PSYNC命令
    if let Some(ms_info) = ms_info.as_ref() {
        to_master_handler
            .conn
            .write_frame_force(&Resp3::<_, String>::new_array(vec![
                Resp3::new_blob_string("PSYNC".into()),
                Resp3::new_blob_string(ms_info.run_id.clone().into_bytes()),
                Resp3::new_blob_string(shared.offset().to_string().into()),
            ]))
            .await?;

        let response = to_master_handler
            .conn
            .read_frame()
            .await?
            .ok_or_else(|| RutinError::from("ERR master response is empty"))?;
    } else {
        to_master_handler
            .conn
            .write_frame_force(&Resp3::<_, String>::new_array(vec![
                Resp3::new_blob_string(b"PSYNC".as_ref()),
                Resp3::new_blob_string(b"?".as_ref()),
                Resp3::new_blob_string(b"-1".as_ref()),
            ]))
            .await?;

        // TODO: 全量同步
    };

    // 成功连接主节点

    /* step5: 所有ac都只允许读命令 */

    if let Some(acl) = &conf.security.acl {
        for mut ac in acl.iter_mut() {
            ac.set_only_read();
        }
    }

    conf.security.default_ac.rcu(|ac| {
        let mut ac = AccessControl::clone(ac);
        ac.set_only_read();
        ac
    });

    /* step6: 设置主节点地址 */

    // conf.replica
    //     .master_addr
    //     .store(Some(Arc::new(master_addr.clone())));

    /* step7: 重置服务(释放所有连接和资源) */

    shared.signal_manager().trigger_shutdown(RESET_SIGNAL).ok();

    todo!()
}

fn full_sync(shared: &Shared) -> RutinResult<()> {
    todo!()
}

pub async fn propagate_wcmd_to_replicas(
    wcmd_rx: Receiver<Message>,
    signal_manager: ShutdownManager<Id>,
) -> RutinResult<()> {
    let mut handlers: Vec<Handler<TcpStream>> = Vec::new();

    loop {
        tokio::select! {
            _ = signal_manager.wait_special_signal() => break,
            msg = wcmd_rx.recv_async() => match msg.expect("wcmd_propagator should never close") {
                Message::Wcmd(wcmd) => {
                    if !handlers.is_empty() {
                        for handler in handlers.iter_mut() {
                            handler.conn.write_all(&wcmd).await?;
                        }
                    }
                }
                Message::AddReplica(handler) => {
                    handlers.push(handler);
                }
            }
        }
    }

    Ok(())
}
