use super::*;
use crate::{
    cmd::{CmdArg, CmdExecutor, CmdUnparsed},
    conf::AccessControl,
    error::{RutinError, RutinResult},
    frame::{CheapResp3, Resp3},
    server::{AsyncStream, Handler},
    shared::{db::Key, Letter},
    Int,
};
use bytes::Bytes;
use itertools::Itertools;
use std::fmt::Debug;
use tracing::instrument;

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

impl<A> CmdExecutor<A> for Publish
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let db = handler.shared.db();

        // 获取正在监听的订阅者
        let listeners = db
            .get_channel_all_listener(self.topic.as_ref())
            .ok_or(RutinError::from(0))?;

        let mut count = 0;
        // 理论上一定会发送成功，因为Db中保存的发布者与订阅者是一一对应的
        for listener in listeners {
            let res = listener
                .send_async(Letter::Resp3(CheapResp3::new_array(vec![
                    Resp3::new_blob_string("message"),
                    Resp3::new_blob_string(self.topic.clone()),
                    Resp3::new_blob_string(self.msg.clone()),
                ])))
                .await;

            // 如果发送失败，证明订阅者已经关闭连接，此时应该从Db中移除该订阅者
            if res.is_err() {
                db.remove_channel_listener(self.topic.as_ref(), &listener);
            } else {
                count += 1;
            }
        }

        Ok(Some(Resp3::new_integer(count)))
    }

    fn parse(mut args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.len() != 2 {
            return Err(RutinError::WrongArgNum);
        }

        let topic = args.next().unwrap();
        if ac.is_forbidden_channel(&topic) {
            return Err(RutinError::NoPermission);
        }

        Ok(Publish {
            topic: topic.into(),
            msg: args.next().unwrap().into_bytes(),
        })
    }

}

#[derive(Debug)]
pub struct Subscribe {
    topics: Vec<Key>,
}

impl<A> CmdExecutor<A> for Subscribe
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let Handler {
            shared,
            conn,
            context,
            ..
        } = handler;

        let subscribed_channels = &mut context.subscribed_channels;

        for topic in self.topics {
            if !subscribed_channels.contains(&topic) {
                // 没有订阅过，则将该频道加入订阅列表
                subscribed_channels.push(topic.clone());
                shared
                    .db()
                    .add_channel_listener(topic.clone(), context.mailbox.outbox.clone());
            }

            conn.write_frame(&CheapResp3::new_array(vec![
                Resp3::new_blob_string("subscribe"),
                Resp3::new_blob_string(topic),
                Resp3::new_integer(subscribed_channels.len() as Int), // 当前客户端订阅的频道数
            ]))
            .await?;
        }

        Ok(None)
    }

    fn parse(args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        let topics = args
            .map(|k| {
                if ac.is_forbidden_channel(&k) {
                    return Err(RutinError::NoPermission);
                }
                Ok(k.into())
            })
            .try_collect()?;

        Ok(Subscribe { topics })
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

impl<A> CmdExecutor<A> for Unsubscribe
where
    A: CmdArg,
{
    #[instrument(
        level = "debug",
        skip(handler),
        ret(level = "debug"),
        err(level = "debug")
    )]
    async fn execute(
        self,
        handler: &mut Handler<impl AsyncStream>,
    ) -> RutinResult<Option<CheapResp3>> {
        let Handler {
            shared,
            conn,
            context,
            ..
        } = handler;

        let subscribed_channels = &mut context.subscribed_channels;

        for topic in self.topics {
            // 订阅了该频道，需要从订阅列表移除，并且移除Db中的监听器
            if let Some(i) = subscribed_channels.iter().position(|t| *t == topic) {
                subscribed_channels.swap_remove(i);
                shared
                    .db()
                    .remove_channel_listener(topic.as_ref(), &context.mailbox.outbox);
            }

            conn.write_frame(&CheapResp3::new_array(vec![
                Resp3::new_blob_string("unsubscribe"),
                Resp3::new_blob_string(topic),
                Resp3::new_integer(subscribed_channels.len() as Int),
            ]))
            .await?;
        }

        Ok(None)
    }

    fn parse(args: CmdUnparsed<A>, ac: &AccessControl) -> RutinResult<Self> {
        if args.is_empty() {
            return Err(RutinError::WrongArgNum);
        }

        let topics = args
            .map(|k| {
                if ac.is_forbidden_channel(&k) {
                    return Err(RutinError::NoPermission);
                }
                Ok(k.into())
            })
            .try_collect()?;

        Ok(Unsubscribe { topics })
    }

}

#[cfg(test)]
mod cmd_pub_sub_tests {
    use super::*;
    use crate::util::test_init;

    #[tokio::test]
    async fn sub_pub_unsub_test() {
        test_init();

        let (mut handler, _) = Handler::new_fake();

        // 订阅 channel1 和 channel2
        let _subscribe_res = Subscribe::test(&["channel1", "channel2"], &mut handler)
            .await
            .unwrap();

        assert!(handler
            .shared
            .db()
            .get_channel_all_listener("channel1".as_bytes())
            .is_some());
        assert!(handler
            .shared
            .db()
            .get_channel_all_listener("channel2".as_bytes())
            .is_some());

        assert_eq!(2, handler.context.subscribed_channels.len());

        // 订阅 channel3
        let _subscribe_res = Subscribe::test(&["channel3"], &mut handler).await.unwrap();

        assert!(handler
            .shared
            .db()
            .get_channel_all_listener("channel3".as_bytes())
            .is_some());

        assert_eq!(3, handler.context.subscribed_channels.len());

        // 向 channel1 发布消息
        let publish_res = Publish::test(&["channel1", "hello"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        let res = publish_res.into_integer_unchecked();
        assert_eq!(res, 1);

        let msg = handler.context.mailbox.recv_async().await;
        assert_eq!(
            msg.into_resp3_unchecked().into_array_unchecked(),
            &[
                Resp3::new_blob_string("message"),
                Resp3::new_blob_string("channel1"),
                Resp3::new_blob_string("hello")
            ]
        );

        // 向 channel2 发布消息
        let publish_res = Publish::test(&["channel2", "world"], &mut handler)
            .await
            .unwrap()
            .unwrap();
        let res = publish_res.into_integer_unchecked();
        assert_eq!(res, 1);

        let msg = handler.context.mailbox.recv_async().await;
        assert_eq!(
            msg.into_resp3_unchecked().into_array_unchecked(),
            &[
                Resp3::new_blob_string("message"),
                Resp3::new_blob_string("channel2"),
                Resp3::new_blob_string("world")
            ]
        );

        // 尝试向未订阅的频道发布消息
        let publish_res = Publish::test(&["channel_not_exist", "hello"], &mut handler)
            .await
            .unwrap_err();
        matches!(publish_res, RutinError::ErrCode { code } if code == 0);

        // 取消订阅 channel1
        let _unsubscribe_res = Unsubscribe::test(&["channel1"], &mut handler)
            .await
            .unwrap();

        assert!(handler
            .shared
            .db()
            .get_channel_all_listener("channel1".as_bytes())
            .is_none());

        assert_eq!(2, handler.context.subscribed_channels.len());
    }
}
