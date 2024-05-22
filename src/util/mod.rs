mod test;

use anyhow::anyhow;
use atoi::FromRadix10SignedChecked;
pub use test::*;

use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

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
