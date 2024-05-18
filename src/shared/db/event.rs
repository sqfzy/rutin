// TODO: 支持将多个同一事件归属为一组，并分别加入到不同的键中

use flume::Sender;
use tracing::instrument;

use crate::{frame::Frame, Key};

#[derive(Default, Debug, Clone)]
pub struct Event(EventInner);

#[derive(Default, Debug, Clone)]
struct EventInner {
    track: Option<Vec<Sender<Frame<'static>>>>,
    update: Option<Vec<Sender<Frame<'static>>>>,
    remove: Option<Vec<Sender<Frame<'static>>>>,
}

#[derive(Debug, Clone)]
pub enum EventType {
    Track,
    Update,
    Remove,
}

impl Event {
    pub(super) fn add_event(&mut self, sender: Sender<Frame<'static>>, event: EventType) {
        match event {
            EventType::Track => self.add_track_event(sender),
            EventType::Update => self.add_update_event(sender),
            EventType::Remove => self.add_remove_event(sender),
        }
    }

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
    fn add_track_event(&mut self, sender: Sender<Frame<'static>>) {
        match &mut self.0.track {
            Some(events) => events.push(sender),
            None => self.0.track = Some(vec![sender]),
        }
    }

    #[inline]
    fn add_update_event(&mut self, sender: Sender<Frame<'static>>) {
        match &mut self.0.update {
            Some(events) => events.push(sender),
            None => self.0.update = Some(vec![sender]),
        }
    }

    #[inline]
    fn add_remove_event(&mut self, sender: Sender<Frame<'static>>) {
        match &mut self.0.remove {
            Some(events) => events.push(sender),
            None => self.0.remove = Some(vec![sender]),
        }
    }

    #[inline]
    #[instrument(level = "debug")]
    fn trigger_track_event(&mut self, key: &Key) {
        let events = if let Some(events) = &mut self.0.track {
            events
        } else {
            return;
        };

        let mut i = 0;
        while let Some(e) = events.get(i) {
            let res = e.send(Frame::new_bulks(&["Invalidated".into(), key.clone()]));

            // 发送失败，证明连接已经断开，移除监听事件
            if res.is_err() {
                events.swap_remove(i);
            }

            i += 1;
        }
    }

    #[inline]
    #[instrument(level = "debug")]
    fn trigger_update_event(&mut self, key: &Key) {
        let events = if let Some(events) = &mut self.0.update {
            events
        } else {
            return;
        };

        let mut i = 0;
        while let Some(e) = events.get(i) {
            let _ = e.send(Frame::new_bulk_owned(key.clone()));

            // 该事件是一次性事件，无论是否有接收者，都需要移除该事件
            events.swap_remove(i);

            i += 1;
        }
    }

    #[inline]
    #[instrument(level = "debug")]
    fn trigger_remove_event(&mut self, key: &Key) {
        let events = if let Some(events) = &mut self.0.remove {
            events
        } else {
            return;
        };

        let mut i = 0;
        while let Some(e) = events.get(i) {
            let _ = e.send(Frame::new_bulk_owned(key.clone()));

            // 该事件是一次性事件，无论是否有接收者，都需要移除该事件
            events.swap_remove(i);

            i += 1;
        }
    }
}
