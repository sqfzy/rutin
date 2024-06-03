// TODO: 支持将多个同一事件归属为一组，并分别加入到不同的键中

use either::Either::Left;
use flume::Sender;
use tracing::instrument;

use crate::{frame::RESP3, Key};

#[derive(Default, Debug, Clone)]
pub struct Event(EventInner);

#[derive(Default, Debug, Clone)]
struct EventInner {
    track: Vec<Sender<RESP3>>,
    update: Vec<Sender<RESP3>>,
    remove: Vec<Sender<RESP3>>,
}

#[derive(Debug, Clone)]
pub enum EventType {
    Track,
    Update,
    Remove,
}

impl Event {
    pub(super) fn add_event(&mut self, sender: Sender<RESP3>, event: EventType) {
        match event {
            EventType::Track => self.0.track.push(sender),
            EventType::Update => self.0.update.push(sender),
            EventType::Remove => self.0.remove.push(sender),
        }
    }

    #[instrument(level = "debug")]
    pub(super) fn trigger_events(&mut self, key: &Key, event: &[EventType]) {
        for e in event {
            match e {
                EventType::Track => self.trigger_track_event(key),
                EventType::Update => self.trigger_update_event(key),
                EventType::Remove => self.trigger_remove_event(key),
            }
        }
    }

    #[inline]
    fn trigger_track_event(&mut self, key: &Key) {
        let events = &mut self.0.track;
        if events.is_empty() {
            return;
        }

        let mut i = 0;
        while let Some(e) = events.get(i) {
            let res = e.send(RESP3::Bulk(Left(key.clone())));

            // 发送失败，证明连接已经断开，移除监听事件
            if res.is_err() {
                events.swap_remove(i);
            }

            i += 1;
        }
    }

    #[inline]
    fn trigger_update_event(&mut self, key: &Key) {
        let events = &mut self.0.update;
        if events.is_empty() {
            return;
        }

        let mut i = 0;
        while let Some(e) = events.get(i) {
            let _ = e.send(RESP3::Bulk(Left(key.clone())));

            // 该事件是一次性事件，无论是否有接收者，都需要移除该事件
            events.swap_remove(i);

            i += 1;
        }
    }

    #[inline]
    fn trigger_remove_event(&mut self, key: &Key) {
        let events = &mut self.0.remove;
        if events.is_empty() {
            return;
        }

        let mut i = 0;
        while let Some(e) = events.get(i) {
            let _ = e.send(RESP3::Bulk(Left(key.clone())));

            // 该事件是一次性事件，无论是否有接收者，都需要移除该事件
            events.swap_remove(i);

            i += 1;
        }
    }
}
