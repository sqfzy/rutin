use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};

use crate::{
    conf::Conf,
    frame::{Bulks, Frame},
    server::{self, Handler},
    shared::{db::Db, Shared},
    util::FakeStream,
    Connection, Key,
};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use mlua::{prelude::*, StdLib};
use tokio::runtime::Handle;
use try_lock::{Locked, TryLock};

// 创建全局redis表格，redis.call, redis.pacll执行Redis命令, redis.sha1hex, redis.error_reply, redis.status_reply
// 更换random函数
// 新增排序辅助函数
// sandbox: 不允许增删全局变量，不允许require module
//
struct LuaScript {
    max: usize,
    index: AtomicUsize,
    luas: Vec<TryLock<(Lua, Handler<FakeStream>)>>,
    // lua_scripts: DashMap<Bytes>,
}

impl LuaScript {
    fn get_lua_and_handler(
        &mut self,
        shared: Shared,
        conf: Arc<Conf>,
    ) -> anyhow::Result<Locked<(Lua, Handler<FakeStream>)>> {
        let mut should_create_lua = false;
        let mut index = self.index.load(Ordering::Relaxed);
        let len = self.luas.len();
        let max = self.max;

        while let Some(l_and_h) = self.luas.get(index) {
            let len = self.luas.len();

            if let Some(lock) = l_and_h.try_lock() {
                self.index.store((index + 1) % len, Ordering::Relaxed);

                // WARN: polonius problem https://crates.io/crates/polonius-the-crab
                drop(lock);
                return Ok(self.luas.get(index).unwrap().try_lock().unwrap());
            }

            index += 1;

            // 当index第二次达到len时，新建lua
            if should_create_lua && index == len {
                break;
            }

            // index第一次达到len，设should_create_lua为true
            if len < max && index == len {
                should_create_lua = true;
            }

            index %= len;
        }

        let libs = StdLib::TABLE | StdLib::STRING | StdLib::MATH;
        let lua = Lua::new_with(libs, LuaOptions::default())?;
        let (handler, client) = Handler::new_fake_with(shared, conf, None);

        {
            let global = lua.globals();

            let keys = lua.create_table()?;
            let argv = lua.create_table()?;

            global.set("KEYS", keys)?; // 全局变量KEYS，用于存储Redis的键
            global.set("ARGV", argv)?; // 全局变量ARGV，用于存储Redis的参数

            let redis = lua.create_table()?;
            let call = lua.create_function_mut(move |_, cmd: Vec<String>| {
                let cmd = Frame::Array(
                    cmd.into_iter()
                        .map(|s| {
                            let mut b = BytesMut::with_capacity(s.len());
                            b.extend(s.into_bytes());
                            Frame::Bulk(b.freeze())
                        })
                        .collect(),
                );
                Handle::current().block_on(async move {
                    // cmd.into_raw?
                    // 如何高效的处理？
                    // client.write_all(cmd).await.unwrap();
                });
                Ok(())
            })?;
            redis.set("call", call)?;

            global.set("redis", redis)?; // 创建全局redis表格

            // redis.pcall

            // redis.log
            // redis.LOG_DEBUG，redis.LOG_VERBOSE，redis.LOG_NOTICE，以及redis.LOG_WARNING
            // redis.sha1hex
            // redis.error_reply, redis.status_reply
        }

        self.luas.push(TryLock::new((lua, handler)));
        let l_and_h = self.luas.last().unwrap().try_lock().unwrap();

        self.index.store((index + 1) % len, Ordering::Relaxed);
        // Ok(l_and_h)
        todo!()
    }

    // pub fn eval(&self, sha: &str) {
    //     let global = self.luas.globals();
    // }
}

impl Default for LuaScript {
    fn default() -> Self {
        Self {
            max: num_cpus::get(),
            index: AtomicUsize::new(0),
            luas: Vec::new(),
        }
    }
}

#[test]
fn test() {
    // let lua = LuaScript::new().unwrap();
}
