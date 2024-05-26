use crate::{
    conf::Conf,
    frame::Frame,
    server::{BgTaskChannel, HandlerContext},
    shared::Shared,
};
use async_shutdown::ShutdownManager;
use flume::{Receiver, Sender};
use std::sync::Arc;

pub struct FakeHandler {
    pub shared: Shared,
    pub conn: (Sender<Frame>, Receiver<Frame>),
    pub shutdown_manager: ShutdownManager<()>,
    pub bg_task_channel: BgTaskChannel,
    pub conf: Arc<Conf>,
    pub context: HandlerContext,
}

// TODO: 使用trait抽象Handler

impl FakeHandler {
    pub fn new(shared: Shared, writer: Sender<Frame>, reader: Receiver<Frame>) -> Self {
        let (w, r) = flume::unbounded();
        Self {
            shared,
            conn: (w, r),
            shutdown_manager: Default::default(),
            bg_task_channel: Default::default(),
            conf: Default::default(),
            context: Default::default(),
        }
    }

    pub fn new_with() {}
}
