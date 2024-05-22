use crate::frame::Frame;
use bytes::BytesMut;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};
use tracing::instrument;

#[derive(Debug)]
pub struct Connection {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    /// stream读取的数据会先写入buf，然后再从buf中读取数据。
    /// buf读取数据的行为：使用游标从buf中读取数据时，如果游标到达末尾，则从stream中读取数据到buf，再次尝试。
    /// 从buf中读数据时，有两种错误情况：
    /// 1. 读取时发现内容错误，证明frame格式错误。
    /// 2. 读取时发现游标到达末尾，但stream中也没有数据，证明frame不完整
    read_buf: BytesMut,
    write_buf: BytesMut,

    /// 支持批处理
    pub count: usize,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        let (r, w) = socket.into_split();
        Self {
            reader: r,
            writer: w,
            read_buf: BytesMut::with_capacity(4 * 1024),
            write_buf: BytesMut::with_capacity(1024),
            count: 0,
            // authed: AtomicCell::new(false),
        }
    }

    #[inline]
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.writer.shutdown().await
    }

    // 尝试读取多个frame，直到buffer和stream都为空
    #[inline]
    #[instrument(level = "trace", skip(self), ret, err)]
    pub async fn read_frames<'a>(&mut self) -> anyhow::Result<Option<Vec<Frame<'static>>>> {
        // 如果buffer为空，则尝试从stream中读入数据到buffer
        if self.read_buf.is_empty() && self.reader.read_buf(&mut self.read_buf).await? == 0 {
            return Ok(None);
        }

        let mut frames = Vec::with_capacity(32);
        loop {
            frames.push(Frame::parse_frame(&mut self.reader, &mut self.read_buf).await?);
            self.count += 1;

            if self.read_buf.is_empty()
                && self.reader.try_read_buf(&mut self.read_buf).unwrap_or(0) == 0
            {
                break;
            }
        }

        Ok(Some(frames))
    }

    #[inline]
    #[instrument(level = "trace", skip(self), err)]
    pub async fn write_frame(&mut self, frame: &Frame<'static>) -> io::Result<()> {
        frame.to_raw_in_buf(&mut self.write_buf);

        if self.count >= 1 {
            self.count -= 1;
        }

        if self.count == 0 {
            self.writer.write_buf(&mut self.write_buf).await?;
            self.writer.flush().await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod conn_tests {
    use super::*;
    use crate::util::test_init;

    #[tokio::test]
    async fn test_read_frames() {
        test_init();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (socket, _addr) = listener.accept().await.unwrap();
            let mut server_conn = Connection::new(socket);

            // 测试简单字符串
            let _ = server_conn
                .write_frame(&Frame::new_simple_borrowed("OK"))
                .await;
            // 测试错误消息
            let _ = server_conn
                .write_frame(&Frame::new_error_borrowed("Error message"))
                .await;
            // 测试整数
            let _ = server_conn.write_frame(&Frame::Integer(1000)).await;
            // 测试大容量字符串
            let _ = server_conn
                .write_frame(&Frame::new_bulk_by_copying(b"foobar"))
                .await;
            // 测试空字符串
            let _ = server_conn
                .write_frame(&Frame::new_bulk_by_copying(b""))
                .await;
            // 测试空值
            let _ = server_conn.write_frame(&Frame::Null).await;
            // 测试数组
            let _ = server_conn
                .write_frame(&Frame::Array(vec![
                    Frame::new_simple_borrowed("simple"),
                    Frame::new_error_borrowed("error"),
                    Frame::Integer(1000),
                    Frame::new_bulk_by_copying(b"bulk"),
                    Frame::Null,
                    Frame::Array(vec![
                        Frame::new_bulk_by_copying(b"foo"),
                        Frame::new_bulk_by_copying(b"bar"),
                    ]),
                ]))
                .await;
            // 测试空数组
            let _ = server_conn.write_frame(&Frame::Array(vec![])).await;

            tx.send(()).unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = Connection::new(stream);

        rx.await.unwrap();

        let mut res = vec![];
        while let Some(frames) = conn.read_frames().await.unwrap() {
            res.extend(frames);
        }

        assert_eq!(
            res,
            vec![
                Frame::new_simple_borrowed("OK"),
                Frame::new_error_borrowed("Error message"),
                Frame::Integer(1000),
                Frame::new_bulk_by_copying(b"foobar"),
                Frame::new_bulk_by_copying(b""),
                Frame::Null,
                Frame::Array(vec![
                    Frame::new_simple_borrowed("simple"),
                    Frame::new_error_borrowed("error"),
                    Frame::Integer(1000),
                    Frame::new_bulk_by_copying(b"bulk"),
                    Frame::Null,
                    Frame::Array(vec![
                        Frame::new_bulk_by_copying(b"foo"),
                        Frame::new_bulk_by_copying(b"bar"),
                    ]),
                ]),
                Frame::Array(vec![]),
            ]
        );
    }

    #[tokio::test]
    async fn test_read_frame() {
        test_init();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel();
        // 模拟服务器
        tokio::spawn(async move {
            let (socket, _addr) = listener.accept().await.unwrap();
            let mut server_conn = Connection::new(socket);

            // 测试简单字符串
            let _ = server_conn
                .write_frame(&Frame::new_simple_borrowed("OK"))
                .await;
            // 测试错误消息
            let _ = server_conn
                .write_frame(&Frame::new_error_borrowed("Error message"))
                .await;
            // 测试整数
            let _ = server_conn.write_frame(&Frame::Integer(1000)).await;
            // 测试大容量字符串
            let _ = server_conn
                .write_frame(&Frame::new_bulk_by_copying(b"foobar"))
                .await;
            // 测试空字符串
            let _ = server_conn
                .write_frame(&Frame::new_bulk_by_copying(b""))
                .await;
            // 测试空值
            let _ = server_conn.write_frame(&Frame::Null).await;
            // 测试数组
            let _ = server_conn
                .write_frame(&Frame::Array(vec![
                    Frame::new_simple_borrowed("simple"),
                    Frame::new_error_borrowed("error"),
                    Frame::Integer(1000),
                    Frame::new_bulk_by_copying(b"bulk"),
                    Frame::Null,
                    Frame::Array(vec![
                        Frame::new_bulk_by_copying(b"foo"),
                        Frame::new_bulk_by_copying(b"bar"),
                    ]),
                ]))
                .await;
            // 测试空数组
            let _ = server_conn.write_frame(&Frame::Array(vec![])).await;
            tx.send(()).unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = Connection::new(stream);

        rx.await.unwrap();

        let mut res = vec![];
        while res.len() != 8 {
            let frame = conn.read_frames().await.unwrap().unwrap();
            res.extend(frame);
        }

        assert_eq!(
            res,
            vec![
                Frame::new_simple_borrowed("OK"),
                Frame::new_error_borrowed("Error message"),
                Frame::Integer(1000),
                Frame::new_bulk_by_copying(b"foobar"),
                Frame::new_bulk_by_copying(b""),
                Frame::Null,
                Frame::Array(vec![
                    Frame::new_simple_borrowed("simple"),
                    Frame::new_error_borrowed("error"),
                    Frame::Integer(1000),
                    Frame::new_bulk_by_copying(b"bulk"),
                    Frame::Null,
                    Frame::Array(vec![
                        Frame::new_bulk_by_copying(b"foo"),
                        Frame::new_bulk_by_copying(b"bar"),
                    ]),
                ]),
                Frame::Array(vec![])
            ]
        )
    }
}
