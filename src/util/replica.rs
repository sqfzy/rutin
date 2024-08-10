use std::sync::Arc;

use crate::{
    conf::{AccessControl, MasterInfo, DEFAULT_USER},
    error::{RutinError, RutinResult},
    frame::Resp3,
    persist::rdb::rdb_load,
    server::{Handler, HandlerContext},
    shared::{Letter, Shared},
};
use bytes::BytesMut;
use bytestring::ByteString;
use event_listener::Event;
use tokio::{net::TcpStream, sync::Mutex};
use tracing::error;

// 该函数在run_replica()之前，同一时刻只允许一个异步任务执行
//
// 如果server本身已经是从节点，则会发送PSYNC <master_run_id>
// <offset>，尝试增量同步，如果失败则同步offset后，执行全量同步
//
// 如果server本身是主节点，则会发送PSYNC ? -1，同步offset后，执
// 行全量同步
pub async fn set_server_to_replica(
    shared: &Shared,
    master_host: ByteString,
    master_port: u16,
) -> RutinResult<()> {
    // 记录replica的offset。运行run_replica()时，会一直持有该锁，直到trigger_shutdown时释放
    static OFFSET: Mutex<u64> = Mutex::const_new(0);

    let conf = shared.conf();

    // 如果已经有一个异步任务在执行该函数(未执行到run_replica())，则直接返回错误
    let mut ms_info = conf
        .replica
        .master_info
        .try_lock()
        .map_err(|_| "ERR another replica operation is in progress, please try again later")?;

    /* step1: 阻塞服务，断开所有连接 */
    let event = Arc::new(Event::new());
    shared
        .post_office()
        .send_all(Letter::BlockServer {
            event: event.clone(),
        })
        .await;

    /* step2: 与主节点握手建立连接 */

    let to_master = TcpStream::connect((master_host.as_ref(), master_port)).await?;

    // 接收主节点的所有命令
    let ac = AccessControl::new_loose();

    let mut handle_master = Handler::with_cx(
        shared.clone(),
        to_master,
        HandlerContext::with_ac(shared, Arc::new(ac), DEFAULT_USER),
    );

    // 发送PING测试主节点是否可达
    handle_master
        .conn
        .write_frame(&Resp3::<_, String>::new_blob_string(b"PING"))
        .await?;

    if !handle_master.conn.read_frame().await?.is_some_and(|frame| {
        frame
            .try_simple_string()
            .is_some_and(|frame| frame == "PONG")
    }) {
        return Err(RutinError::from(format!(
            "ERR master {}:{} is unreachable or busy",
            master_host, master_port
        )));
    };

    // 身份验证(可选)
    if let Some(password) = conf.replica.master_auth.as_ref() {
        handle_master
            .conn
            .write_frame(&Resp3::<_, String>::new_array(vec![
                Resp3::new_blob_string(b"AUTH".to_vec()),
                Resp3::new_blob_string(password.clone().into_bytes()),
            ]))
            .await?;

        if handle_master
            .conn
            .read_frame()
            .await?
            .is_some_and(|frame| frame.is_simple_error())
        {
            return Err(RutinError::from("ERR master authentication failed"));
        }
    }

    // 发送端口信息
    handle_master
        .conn
        .write_frame(&Resp3::<_, String>::new_array(vec![
            Resp3::new_blob_string(b"REPLCONF".to_vec()),
            Resp3::new_blob_string(b"listening-port".to_vec()),
            Resp3::new_blob_string(conf.server.port.to_string().into_bytes()),
        ]))
        .await?;

    // 等待旧的run_replica()任务结束并获取offset
    let mut offset = OFFSET.lock().await;

    // 发送PSYNC命令
    if let Some(ms_info) = ms_info.as_mut() {
        // PSYNC <run_id> <offset>
        let array = vec![
            Resp3::new_blob_string("PSYNC".into()),
            Resp3::new_blob_string(ms_info.run_id.clone()),
            Resp3::new_blob_string((*offset).to_string().into()),
        ];
        handle_master
            .conn
            .write_frame(&Resp3::<_, String>::new_array(array))
            .await?;
    } else {
        // PSYNC ? -1
        handle_master
            .conn
            .write_frame(&Resp3::<_, String>::new_array(vec![
                Resp3::new_blob_string(b"PSYNC".as_ref()),
                Resp3::new_blob_string(b"?".as_ref()),
                Resp3::new_blob_string(b"-1".as_ref()),
            ]))
            .await?;
    };

    let response = handle_master.conn.read_frame_force().await?;

    let shared = shared.clone();
    tokio::spawn(async move {
        // read_only模式下，所有ac都只允许读命令
        if conf.replica.read_only {
            if let Some(acl) = &conf.security.acl {
                for mut ac in acl.iter_mut() {
                    ac.allow_only_read();
                }
            }

            conf.security.default_ac.rcu(|ac| {
                let mut ac = AccessControl::clone(ac);
                ac.allow_only_read();
                ac
            });
        }

        /* step3: 进行同步 */

        // 根据响应决定是否需要全量同步
        if let Resp3::Array { inner: mut arr, .. } = response {
            // 响应为+FULLRESYNC run_id offset，执行全量同步
            if arr.len() == 3 {
                let master_offset = arr
                    .pop()
                    .unwrap()
                    .into_integer()
                    .expect("invalid offset from master")
                    .try_into()
                    .expect("invalid offset from master");

                let run_id = arr
                    .pop()
                    .unwrap()
                    .into_blob_string()
                    .expect("invalid run_id from master");

                *ms_info = Some(MasterInfo {
                    host: master_host,
                    port: master_port,
                    run_id,
                });
                *offset = master_offset;

                // 清除旧数据并执行全量同步
                shared.clear();
                full_sync(&mut handle_master).await.unwrap();
            }
            // 响应为+CONTINUE，执行增量同步(什么都不做，等之后向主节点发送REPLCONF ACK
            // <offset>时会进行同步)
        }

        /* step4: 解除阻塞 */
        event.notify(usize::MAX);

        // 释放锁，重新允许执行set_server_to_replica函数
        drop(ms_info);

        /* step5: 接收并处理传播的写命令 */
        if let Err(e) = handle_master.run_replica(offset).await {
            error!(cause = %e, "replica run error");
        }
    });

    Ok(())
}

async fn full_sync(handler: &mut Handler<TcpStream>) -> RutinResult<()> {
    // 接收RDB文件
    let len = handler
        .conn
        .read_frame_force()
        .await?
        .try_integer()
        .ok_or_else(|| RutinError::new_server_error("ERR invalid rdb data length"))?;

    let mut buf = BytesMut::with_capacity(len as usize);
    handler.conn.read_exact(&mut buf).await?;

    rdb_load(&mut buf, handler.shared.db(), false)
        .await
        .map_err(|e| RutinError::new_server_error(e.to_string()))?;

    Ok(())
}
