mod fake_cs;
mod test;

pub use fake_cs::*;
pub use test::*;

use anyhow::anyhow;
use atoi::FromRadix10SignedChecked;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::Int;

// 测试客户端，向服务端发送指定命令
#[allow(dead_code)]
pub async fn client_test(cmd: &[u8]) {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    stream.write_all(cmd).await.unwrap();
    let mut buf = [0u8; 1024];
    loop {
        let n = stream.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        print!("{:?}", String::from_utf8(buf[0..n].to_vec()).unwrap());
    }
}

// 测试服务端，接收客户端的命令并打印
#[allow(dead_code)]
pub async fn server_test() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379")
        .await
        .unwrap();

    let mut stream = listener.accept().await.unwrap().0;

    loop {
        let mut buf = vec![0; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        println!("read: {n}.\n{:?}", String::from_utf8(buf.clone()).unwrap());
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub fn atoi<I: FromRadix10SignedChecked>(text: &[u8]) -> Result<I, &str> {
    atoi::atoi(text).ok_or("failed to parse number")
}

pub fn upper_case(src: &[u8], buf: &mut [u8]) -> anyhow::Result<usize> {
    let len = src.len();
    if len > buf.len() {
        return Err(anyhow!("buffer is too small"));
    }

    buf[..len].copy_from_slice(src);
    buf[..len].make_ascii_uppercase();

    Ok(len)
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
