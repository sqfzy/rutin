use crate::{
    cmd::{
        error::{CmdError, Err},
        CmdExecutor, CmdType, ServerErrSnafu,
    },
    connection::AsyncStream,
    frame::{Bulks, Frame},
    server::Handler,
    shared::Shared,
    Int, Key,
};
use bytes::Bytes;
use snafu::ResultExt;

/// # Reply:
///
/// Integer reply: the number of clients that received the message.
/// Note that in a Redis Cluster, only clients that are connected
/// to the same node as the publishing client are included in the count.
#[derive(Debug)]
pub struct Publish {
    topic: Key,
    msg: Bytes,
}

impl CmdExecutor for Publish {
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn _execute(self, shared: &Shared) -> Result<Option<Frame>, CmdError> {
        // 获取正在监听的订阅者
        let listeners = shared
            .db()
            .get_channel_all_listener(&self.topic)
            .ok_or(CmdError::from(0))?;

        let mut count = 0;
        // 理论上一定会发送成功，因为Db中保存的发布者与订阅者是一一对应的
        for listener in listeners {
            let res = listener
                .send_async(Frame::new_bulks(&[
                    "message".into(),
                    self.topic.clone(),
                    self.msg.clone(),
                ]))
                .await;

            // 如果发送失败，证明订阅者已经关闭连接，此时应该从Db中移除该订阅者
            if res.is_err() {
                shared.db().remove_channel_listener(&self.topic, &listener);
            } else {
                count += 1;
            }
        }

        Ok(Some(Frame::new_integer(count)))
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.len() != 2 {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Publish {
            topic: args.pop_front().unwrap(),
            msg: args.pop_front().unwrap(),
        })
    }
}

#[derive(Debug)]
pub struct Subscribe {
    topics: Vec<Key>,
}

impl CmdExecutor for Subscribe {
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Frame>, CmdError> {
        let Handler {
            shared,
            conn,
            context,
            bg_task_channel,
            ..
        } = handler;

        let subscribed_channels = if context.subscribed_channels.is_none() {
            // subscribed_channels为None，表明从未订阅过频道。创建一个Vec存储订阅的频道的名称
            context.subscribed_channels = Some(Vec::with_capacity(8));
            context.subscribed_channels.as_mut().unwrap()
        } else {
            context.subscribed_channels.as_mut().unwrap()
        };

        for topic in self.topics {
            if !subscribed_channels.contains(&topic) {
                // 没有订阅过，则将该频道加入订阅列表
                subscribed_channels.push(topic.clone());
                shared
                    .db()
                    .add_channel_listener(topic.clone(), bg_task_channel.new_sender());
            }

            conn.write_frame(&Frame::new_array(vec![
                Frame::new_bulk_by_copying(b"subscribe"),
                Frame::new_bulk_owned(topic),
                Frame::new_integer(subscribed_channels.len() as Int), // 当前客户端订阅的频道数
            ]))
            .await
            .context(ServerErrSnafu)?;
        }

        Ok(None)
    }

    async fn _execute(self, _shared: &Shared) -> Result<Option<Frame>, CmdError> {
        Ok(None)
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.is_empty() {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Subscribe {
            topics: args.iter().cloned().collect(),
        })
    }
}

/// # Reply:
///
/// When successful, this command doesn't return anything. Instead, for each channel,
/// one message with the first element being the string unsubscribe is pushed as a
/// confirmation that the command succeeded.
#[derive(Debug)]
pub struct Unsubscribe {
    topics: Vec<Key>,
}

impl CmdExecutor for Unsubscribe {
    const CMD_TYPE: CmdType = CmdType::Other;

    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> Result<Option<Frame>, CmdError> {
        let Handler {
            shared,
            conn,
            context,
            bg_task_channel,
            ..
        } = handler;

        let subscribed_channels = if let Some(sub) = &mut context.subscribed_channels {
            sub
        } else {
            for topic in self.topics {
                conn.write_frame(&Frame::new_array(vec![
                    Frame::new_bulk_by_copying(b"unsubscribe"),
                    Frame::new_bulk_owned(topic),
                    Frame::new_integer(0),
                ]))
                .await
                .context(ServerErrSnafu)?;
            }
            return Ok(None);
        };

        for topic in self.topics {
            // 订阅了该频道，需要从订阅列表移除，并且移除Db中的监听器
            if let Some(i) = subscribed_channels.iter().position(|t| *t == topic) {
                subscribed_channels.swap_remove(i);
                shared
                    .db()
                    .remove_channel_listener(&topic, bg_task_channel.get_sender());
            }

            conn.write_frame(&Frame::new_array(vec![
                Frame::new_bulk_by_copying(b"unsubscribe"),
                Frame::new_bulk_owned(topic),
                Frame::new_integer(subscribed_channels.len() as Int),
            ]))
            .await
            .context(ServerErrSnafu)?;
        }

        Ok(None)
    }

    async fn _execute(self, _shared: &Shared) -> Result<Option<Frame>, CmdError> {
        Ok(None)
    }

    fn parse(args: &mut Bulks) -> Result<Self, CmdError> {
        if args.is_empty() {
            return Err(Err::WrongArgNum.into());
        }

        Ok(Unsubscribe {
            topics: args.iter().cloned().collect(),
        })
    }
}

#[cfg(test)]
mod cmd_pub_sub_tests {
    use super::*;
    use crate::util::test_init;

    #[tokio::test]
    async fn sub_pub_unsub_test() {
        test_init();

        let mut handler = Handler::default();

        // 订阅channel1和channel2
        let subscribe =
            Subscribe::parse(&mut Bulks::from(["channel1", "channel2"].as_ref())).unwrap();
        subscribe.execute(&mut handler).await.unwrap();

        assert!(handler
            .shared
            .db()
            .get_channel_all_listener(b"channel1")
            .is_some());
        assert!(handler
            .shared
            .db()
            .get_channel_all_listener(b"channel2")
            .is_some());

        assert_eq!(
            2,
            handler.context.subscribed_channels.as_ref().unwrap().len()
        );

        // 订阅channel3
        let subscribe = Subscribe::parse(&mut Bulks::from(["channel3"].as_ref())).unwrap();
        subscribe.execute(&mut handler).await.unwrap();

        assert!(handler
            .shared
            .db()
            .get_channel_all_listener(b"channel3")
            .is_some());

        assert_eq!(
            3,
            handler.context.subscribed_channels.as_ref().unwrap().len()
        );

        // 向channel1发布消息
        let publish = Publish::parse(&mut Bulks::from(["channel1", "hello"].as_ref())).unwrap();
        let res = publish
            ._execute(&handler.shared)
            .await
            .unwrap()
            .unwrap()
            .on_integer()
            .unwrap();
        assert_eq!(res, 1);

        let msg = handler
            .bg_task_channel
            .recv_from_bg_task()
            .await
            .into_array()
            .unwrap();
        matches!(msg.first().unwrap(), Frame::Bulk(x) if x.as_ref() == b"message");
        matches!(msg.get(1).unwrap(), Frame::Bulk(x) if x.as_ref() == b"channel1");
        matches!(msg.get(2).unwrap(), Frame::Bulk(x) if x.as_ref() == b"hello");

        // 向channel2发布消息
        let publish = Publish::parse(&mut Bulks::from(["channel2", "world"].as_ref())).unwrap();
        let res = publish
            ._execute(&handler.shared)
            .await
            .unwrap()
            .unwrap()
            .on_integer()
            .unwrap();
        assert_eq!(res, 1);

        let msg = handler
            .bg_task_channel
            .recv_from_bg_task()
            .await
            .into_array()
            .unwrap();
        matches!(msg.first().unwrap(), Frame::Bulk(x) if x.as_ref() == b"message");
        matches!(msg.get(1).unwrap(), Frame::Bulk(x) if x.as_ref() == b"channel2");
        matches!(msg.get(2).unwrap(), Frame::Bulk(x) if x.as_ref() == b"world");

        // 尝试向未订阅的频道发布消息
        let publish =
            Publish::parse(&mut Bulks::from(["channel_not_exist", "hello"].as_ref())).unwrap();
        let res = publish.execute(&mut handler).await.unwrap_err();
        matches!(res, CmdError::ErrorCode { code } if code == 0);

        // 取消订阅channel1
        let unsubscribe = Unsubscribe::parse(&mut Bulks::from(["channel1"].as_ref())).unwrap();
        unsubscribe.execute(&mut handler).await.unwrap();

        assert!(handler
            .shared
            .db()
            .get_channel_all_listener(b"channel1")
            .is_none());

        assert_eq!(
            2,
            handler.context.subscribed_channels.as_ref().unwrap().len()
        );
    }
}
