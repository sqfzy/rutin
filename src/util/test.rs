use crate::{
    conf::Conf,
    server::{BgTaskChannel, Handler, HandlerContext, Listener},
    shared::Shared,
    util::fake_cs::FakeStream,
    Connection, Int,
};
use async_shutdown::ShutdownManager;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::Semaphore, time::sleep};
use tracing::Level;

pub fn test_init() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();
}

// pub async fn create_server() -> Listener {
//     let random_port = rand::random::<u16>() % 10000 + rand::random::<u16>() % 10000;
//     let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{:?}", random_port))
//         .await
//         .unwrap();
//     let mut conf = Conf::default();
//     conf.server.port = random_port;
//
//     let shutdown_manager = ShutdownManager::new();
//     Listener {
//         shared: Shared::default(),
//         listener,
//         tls_acceptor: None,
//         limit_connections: Arc::new(Semaphore::new(1000)),
//         delay_token: shutdown_manager.delay_shutdown_token().unwrap(),
//         shutdown_manager,
//         conf: Arc::new(conf),
//     }
// }
//
// pub async fn create_handler(server: &mut Listener) -> Handler<FakeStream> {
//     let port = server.conf.server.port;
//     tokio::spawn(async move {
//         loop {
//             if TcpStream::connect(format!("127.0.0.1:{:?}", port))
//                 .await
//                 .is_ok()
//             {
//                 break;
//             }
//             sleep(Duration::from_millis(500)).await;
//         }
//     });
//
//     let shared = server.shared.clone();
//     let conn = Connection::from(server.listener.accept().await.unwrap().0);
//     let shutdown_manager = server.shutdown_manager.clone();
//     let conf = server.conf.clone();
//
//     // Handler {
//     //     shared,
//     //     conn,
//     //     shutdown_manager,
//     //     bg_task_channel: BgTaskChannel::default(),
//     //     conf,
//     //     context: HandlerContext::new(rand::random::<u128>()),
//     // }
//     // TODO:
//     todo!()
// }

pub fn to_valid_range(start: Int, end: Int, len: usize) -> Option<(usize, usize)> {
    if start == 0 || end == 0 {
        return None;
    }

    let len = len as Int;

    // 将正负索引统一为有效的正索引
    let start_index = if start > 0 {
        if start > len {
            return None;
        }
        start - 1
    } else if start < -len {
        0
    } else {
        len + start
    };

    let end_index = if end < 0 {
        if end < -len {
            return None;
        }
        len + end + 1
    } else if end > len {
        len
    } else {
        end
    };

    if start_index < 0 || end_index < 0 || start_index > end_index {
        return None;
    }

    Some((start_index as usize, end_index as usize - 1))
}

#[test]
fn to_valid_range_test() {
    // 索引存在0则返回None
    assert!(to_valid_range(0, 1, 6).is_none());
    assert!(to_valid_range(1, 0, 6).is_none());
    assert!(to_valid_range(-1, 0, 6).is_none());
    assert!(to_valid_range(0, -1, 6).is_none());

    // 测试正索引
    assert_eq!(to_valid_range(1, 3, 6).unwrap(), (0, 2));
    assert_eq!(to_valid_range(1, 6, 6).unwrap(), (0, 5));
    assert_eq!(to_valid_range(1, 7, 6).unwrap(), (0, 5));
    assert_eq!(to_valid_range(3, 3, 6).unwrap(), (2, 2));
    assert_eq!(to_valid_range(3, 5, 6).unwrap(), (2, 4));
    assert!(to_valid_range(5, 3, 6).is_none());

    // 测试负索引
    assert_eq!(to_valid_range(-6, -4, 6).unwrap(), (0, 2));
    assert_eq!(to_valid_range(-6, -1, 6).unwrap(), (0, 5));
    assert_eq!(to_valid_range(-7, -1, 6).unwrap(), (0, 5));
    assert_eq!(to_valid_range(-4, -4, 6).unwrap(), (2, 2));
    assert_eq!(to_valid_range(-4, -2, 6).unwrap(), (2, 4));
    assert!(to_valid_range(-2, -4, 6).is_none());

    // 测试正负索引混合
    assert_eq!(to_valid_range(1, -4, 6).unwrap(), (0, 2));
    assert_eq!(to_valid_range(-6, 6, 6).unwrap(), (0, 5));
    assert_eq!(to_valid_range(-7, 7, 6).unwrap(), (0, 5));
    assert_eq!(to_valid_range(3, -4, 6).unwrap(), (2, 2));
    assert_eq!(to_valid_range(3, -2, 6).unwrap(), (2, 4));
    assert!(to_valid_range(7, -7, 6).is_none());
}
