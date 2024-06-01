use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::{
    cmd::dispatch,
    conf::Conf,
    connection::{AsyncStream, FakeStream, ShutdownSignal},
    frame::Frame,
    server::Handler,
    shared::Shared,
    Key,
};
use bytes::{Bytes, BytesMut};
use mlua::{prelude::*, StdLib};
use tracing::debug;
use try_lock::{Locked, TryLock};

// 创建全局redis表格，redis.call, redis.pacll执行Redis命令, redis.sha1hex, redis.error_reply, redis.status_reply
// 更换random函数
// 新增排序辅助函数
// sandbox: 不允许增删全局变量，不允许require module
//
struct LuaScript {
    shared: Shared,
    conf: Arc<Conf>,
    max: usize,
    index: AtomicUsize,
    // ShutdownSignal用于通知Handler伪客户端已经传输消息完毕
    luas: Vec<TryLock<(Lua, Handler<FakeStream>, ShutdownSignal)>>,
    // lua_scripts: DashMap<String>,
}

impl LuaScript {
    pub fn new(shared: Shared, conf: Arc<Conf>) -> Self {
        Self {
            shared,
            conf,
            max: num_cpus::get(),
            index: AtomicUsize::new(0),
            luas: Vec::new(),
        }
    }

    fn get_lua_and_handler(
        &mut self,
    ) -> anyhow::Result<Locked<(Lua, Handler<FakeStream>, ShutdownSignal)>> {
        let mut not_first_time = false;
        let mut index = self.index.load(Ordering::Relaxed);
        let len = self.luas.len();
        let max = self.max;

        while let Some(l_and_h) = self.luas.get(index) {
            let len = self.luas.len();

            if let Some(lock) = l_and_h.try_lock() {
                self.index.store((index + 1) % len, Ordering::Relaxed);

                // WARN: NLL has a borrow check problem, may using polonius solve it. https://crates.io/crates/polonius-the-crab
                drop(lock);
                let l_and_h = loop {
                    if let Some(lock) = self.luas.last().unwrap().try_lock() {
                        break lock;
                    }
                    std::thread::sleep(Duration::from_secs(1));
                };
                return Ok(l_and_h);
            }

            index += 1;

            // 当index第二次达到len时，如果len<max则新建lua，否则等待1秒后，重新尝试获取
            if not_first_time && index == len {
                if len < max {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_secs(1));
            }

            // index第一次达到len后，修改not_first_time 为true
            if index == len {
                not_first_time = true;
            }

            index %= len;
        }

        let libs = StdLib::TABLE | StdLib::STRING | StdLib::MATH;
        let lua = Lua::new_with(libs, LuaOptions::default())?;
        let (handler, mut client) =
            Handler::new_fake_with(self.shared.clone(), self.conf.clone(), None);
        let shutdown_signal = client.shutdown_signal();

        {
            // 停止自动GC，只通过手动方式触发GC
            lua.gc_stop();

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

                debug!("lua call: {:?}", cmd);

                // 每个Lua环境都拥有一个自己的伪客户端
                client.write_frame_blocking(&cmd).unwrap();
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

        self.luas
            .push(TryLock::new((lua, handler, shutdown_signal)));

        let l_and_h = loop {
            if let Some(lock) = self.luas.last().unwrap().try_lock() {
                break lock;
            }
            std::thread::sleep(Duration::from_secs(1));
        };

        self.index.store((index + 1) % (len + 1), Ordering::Relaxed);
        Ok(l_and_h)
    }

    pub async fn exec(
        &mut self,
        script: impl AsRef<[u8]>,
        handler: &mut Handler<impl AsyncStream>,
        keys: Vec<Key>,
        argv: Vec<Bytes>,
    ) -> anyhow::Result<Frame> {
        let mut responds = vec![];
        let mut lock = self.get_lua_and_handler()?;
        let lua = &mut lock.0;

        {
            let global = lua.globals();

            // 传入KEYS和ARGV
            let lua_keys = global.get::<_, LuaTable>("KEYS")?;
            for (i, key) in keys.into_iter().enumerate() {
                // PERF: 会clone key
                lua_keys.set(i + 1, key.as_ref())?;
            }

            let lua_argv = global.get::<_, LuaTable>("ARGV")?;
            for (i, arg) in argv.into_iter().enumerate() {
                // PERF: 会clone arg
                lua_argv.set(i + 1, arg.as_ref())?;
            }

            lua.load(script.as_ref()).exec()?; // 执行脚本，若脚本有错误则不会执行命令

            // 清理Lua环境
            lua_keys.clear()?;
            lua_argv.clear()?;
            lua.gc_collect()?;
        }

        // 通知Handler伪客户端已经传输消息完毕
        let shutdown_signal = &lock.2;
        shutdown_signal.shutdown();

        let fake_handler = &mut lock.1;
        while let Some(frames) = fake_handler.conn.read_frames().await? {
            handler.conn.set_count(frames.len());
            for f in frames.into_iter() {
                if let Some(respond) = dispatch(f, fake_handler).await? {
                    responds.push(respond);
                }
            }
        }

        Ok(Frame::Array(responds))
    }

    pub fn eval_sha() {}
}

#[tokio::test]
async fn test() {
    crate::util::test_init();

    let mut lua_script = LuaScript::new(Shared::default(), Arc::new(Conf::default()));
    let (mut handler, _) = Handler::new_fake();

    lua_script
        .exec(r#"print("exec")"#, &mut handler, vec![], vec![])
        .await
        .unwrap();

    let res = lua_script
        .exec(r#"redis.call({"ping"})"#, &mut handler, vec![], vec![])
        .await
        .unwrap();
    assert_eq!(res, Frame::Array(vec![Frame::new_simple_borrowed("PONG")]));

    let res = lua_script
        .exec(
            r#"
                redis.call({"set", "key", "value"})
                redis.call({"get", "key"})
            "#,
            &mut handler,
            vec![Key::from("value")],
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(
        res,
        Frame::Array(vec![
            Frame::new_simple_borrowed("OK"),
            Frame::new_bulk_from_static(b"value")
        ])
    );
}
