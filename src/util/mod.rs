mod replica;
mod test;

pub use replica::*;
pub use test::*;

use bytes::Bytes;
use snafu::OptionExt;

use crate::{
    error::{A2IParseSnafu, RutinError, RutinResult},
    Int,
};
use atoi::FromRadix10SignedChecked;
use std::{ops::Deref, sync::OnceLock, time::SystemTime};
use tokio::time::Instant;

pub static UNIX_EPOCH: UnixEpoch = UnixEpoch::new();

pub struct UnixEpoch(OnceLock<Instant>);

impl UnixEpoch {
    #[allow(clippy::new_without_default)]
    pub const fn new() -> Self {
        Self(OnceLock::new())
    }
}

impl Deref for UnixEpoch {
    type Target = Instant;

    fn deref(&self) -> &Self::Target {
        self.0.get_or_init(|| {
            Instant::now()
                - SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
        })
    }
}

// 模拟服务端，接收客户端的命令并打印
#[cfg(feature = "debug_server")]
#[allow(dead_code)]
pub async fn debug_server() {
    use crate::{conf::Conf, util::test_init};
    use std::sync::Arc;

    test_init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379")
        .await
        .unwrap();

    let stream = listener.accept().await.unwrap().0;
    let mut handler =
        crate::server::Handler::new(Default::default(), stream, Arc::new(Conf::new().unwrap()));

    loop {
        let cmd_frame = if let Some(f) = handler.conn.read_frame().await.unwrap() {
            f
        } else {
            return;
        };

        if let Some(res) = crate::cmd::dispatch(cmd_frame, &mut handler).await.unwrap() {
            handler.conn.write_frame(&res).await.unwrap();
        }
    }
}

// 模拟客户端，发送命令给服务端并打印响应
#[cfg(feature = "debug_client")]
#[allow(dead_code)]
pub async fn debug_client() {
    use crate::frame::Resp3;
    use bytes::BytesMut;
    use clap::Parser;
    use either::Either::Right;
    use tokio::net::TcpStream;

    crate::util::test_init();

    #[derive(clap::Parser)]
    struct RequireCmd {
        #[clap(name = "cmd", required = true)]
        inner: Vec<String>,
    }

    let cmd = RequireCmd::parse();

    let stream = TcpStream::connect("127.0.0.1:6379".to_string())
        .await
        .unwrap();
    let mut conn = crate::Connection::new(stream, 0);

    let cmd = cmd
        .inner
        .into_iter()
        .map(|s| RESP3::Bulk(Right(BytesMut::from(s.as_str()))))
        .collect::<Vec<_>>();

    let cmd = RESP3::Array(cmd);
    conn.write_frame(&cmd).await.unwrap();

    let res = conn.read_frame().await.unwrap();

    if let Some(res) = res {
        println!("{:?}", res);
    }
}

pub fn atoi<I: FromRadix10SignedChecked>(text: &[u8]) -> RutinResult<I> {
    atoi::atoi(text).with_context(|| A2IParseSnafu {
        invalid: Bytes::copy_from_slice(text),
    })
}

pub fn atof(text: &[u8]) -> RutinResult<f64> {
    std::str::from_utf8(text)?
        .parse()
        .map_err(|_| RutinError::A2IParse {
            invalid: Bytes::copy_from_slice(text),
        })
}

pub fn uppercase(src: &[u8], buf: &mut [u8]) -> RutinResult<usize> {
    let len = src.len();
    if len > buf.len() {
        return Err(RutinError::ServerErr {
            msg: "buffer is too small".into(),
        });
    }

    buf[..len].copy_from_slice(src);
    buf[..len].make_ascii_uppercase();

    Ok(len)
}

pub fn get_uppercase<'a>(src: &[u8], buf: &'a mut [u8]) -> RutinResult<&'a [u8]> {
    let len = src.len();
    if len > buf.len() {
        return Err(RutinError::ServerErr {
            msg: "buffer is too small".into(),
        });
    }

    buf[..len].copy_from_slice(src);
    buf[..len].make_ascii_uppercase();

    Ok(&buf[..len])
}

pub fn get_lowercase<'a>(src: &[u8], buf: &'a mut [u8]) -> RutinResult<&'a [u8]> {
    let len = src.len();
    if len > buf.len() {
        return Err(RutinError::ServerErr {
            msg: "buffer is too small".into(),
        });
    }

    buf[..len].copy_from_slice(src);
    buf[..len].make_ascii_lowercase();

    Ok(&buf[..len])
}

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
