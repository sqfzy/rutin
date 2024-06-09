use crate::{
    cmd::{dispatch, CmdError},
    conf::Conf,
    connection::{AsyncStream, FakeStream, ShutdownSignal},
    frame::RESP3,
    server::{Handler, ServerError},
    shared::Shared,
    Key,
};
use ahash::RandomState;
use bytes::{Bytes, BytesMut};
use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use either::for_both;
use futures::{
    pin_mut,
    task::{noop_waker_ref, WakerRef},
    Future, FutureExt,
};
use mlua::{prelude::*, StdLib};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{ready, Context, Poll, Waker},
    time::Duration,
};
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
    luas: Vec<TryLock<Lua>>,
    lua_scripts: DashMap<Bytes, Bytes, RandomState>, // sha1hex -> script
}

// TODO: 创建脚本
// 删除脚本

impl LuaScript {
    pub fn new(shared: Shared, conf: Arc<Conf>) -> Self {
        Self {
            shared,
            conf,
            max: num_cpus::get(),
            index: AtomicUsize::new(0),
            luas: Vec::new(),
            lua_scripts: DashMap::with_hasher(RandomState::default()),
        }
    }

    fn get_lua(&mut self) -> anyhow::Result<Locked<Lua>> {
        let mut not_first_time = false;
        let mut index = self.index.load(Ordering::Relaxed);
        let len = self.luas.len();
        let max = self.max;

        // 尝试从已有的Lua环境中获取一个可用的Lua环境
        while let Some(lua) = self.luas.get(index) {
            let len = self.luas.len();

            if let Some(lock) = lua.try_lock() {
                self.index.store((index + 1) % len, Ordering::Relaxed);

                // WARN: NLL has a borrow check bug, may using polonius solve it. https://crates.io/crates/polonius-the-crab
                drop(lock);
                let lua = loop {
                    if let Some(lock) = self.luas.last().unwrap().try_lock() {
                        break lock;
                    }
                    std::thread::sleep(Duration::from_secs(1));
                };
                return Ok(lua);
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

        // 创建新的Lua环境
        let libs = StdLib::TABLE | StdLib::STRING | StdLib::MATH;
        let lua = Lua::new_with(libs, LuaOptions::default())?;

        {
            let shared = Box::leak(Box::new(self.shared.clone()));
            let conf = Box::leak(Box::new(self.conf.clone()));

            // 停止自动GC，只通过手动方式触发GC
            lua.gc_stop();

            let global = lua.globals();

            let keys = lua.create_table()?;
            let argv = lua.create_table()?;

            global.set("KEYS", keys)?; // 全局变量KEYS，用于存储Redis的键
            global.set("ARGV", argv)?; // 全局变量ARGV，用于存储Redis的参数

            let redis = lua.create_table()?;

            // redis.call
            // 执行脚本，当发生运行时错误时，中断脚本
            let call = lua.create_async_function(|lua, cmd: LuaMultiValue| {
                async {
                    let (mut handler, _) =
                        Handler::new_fake_with(shared.clone(), conf.clone(), None);

                    let mut cmd_frame = Vec::with_capacity(cmd.len());
                    for v in cmd {
                        match v {
                            LuaValue::String(s) => {
                                // PERF: 避免拷贝
                                cmd_frame.push(RESP3::Bulk(Bytes::copy_from_slice(s.as_bytes())))
                            }
                            _ => {
                                return Err(LuaError::external(
                                    "redis.call only accept string arguments",
                                ));
                            }
                        }
                    }
                    let cmd_frame = RESP3::Array(cmd_frame);

                    debug!("lua call: {:?}", cmd_frame);

                    match dispatch(cmd_frame, &mut handler).await {
                        Ok(ei) => for_both!(ei, ei => {
                            match ei {
                                Some(res) => Ok(res.into_lua(lua)),
                                None => Ok(RESP3::<Bytes, String>::Null.into_lua(lua)),
                            }
                        }),
                        // 中断脚本，返回运行时错误
                        Err(e) => Err(LuaError::external(format!("ERR {}", e))),
                    }
                }
            })?;
            redis.set("call", call)?;

            // redis.pcall
            // 执行脚本，当发生运行时错误时，返回一张表，{ err: Lua String }
            let pcall = lua.create_async_function(|lua, cmd: LuaMultiValue| {
                async {
                    let (mut handler, _) =
                        Handler::new_fake_with(shared.clone(), conf.clone(), None);

                    let mut cmd_frame = Vec::with_capacity(cmd.len());
                    for v in cmd {
                        match v {
                            LuaValue::String(s) => {
                                // PERF: 避免拷贝
                                cmd_frame.push(RESP3::Bulk(Bytes::copy_from_slice(s.as_bytes())))
                            }
                            _ => {
                                return Err(LuaError::external(
                                    "redis.call only accept string arguments",
                                ));
                            }
                        }
                    }
                    let cmd_frame = RESP3::Array(cmd_frame);

                    debug!("lua call: {:?}", cmd_frame);

                    match dispatch(cmd_frame, &mut handler).await {
                        Ok(ei) => for_both!(ei, ei => {
                            match ei {
                                Some(res) => Ok(res.into_lua(lua)),
                                None => Ok(RESP3::<Bytes, String>::Null.into_lua(lua)),
                            }
                        }),
                        // 不返回运行时错误，而是返回一个表 { err: Lua String }
                        Err(e) => Ok(RESP3::SimpleError(e.to_string()).into_lua(lua)),
                    }
                }
            })?;
            redis.set("pcall", pcall)?;

            // redis.status_reply
            // 返回一张表, { ok: Lua String }
            let status_reply =
                lua.create_function_mut(|lua, ok: String| RESP3::SimpleString(ok).into_lua(lua))?;
            redis.set("status_reply", status_reply)?;

            // redis.error_reply
            // 返回一张表, { err: Lua String }
            let error_reply =
                lua.create_function_mut(|lua, err: String| RESP3::SimpleError(err).into_lua(lua))?;
            redis.set("error_reply", error_reply)?;

            // redis.LOG_DEBUG，redis.LOG_VERBOSE，redis.LOG_NOTICE，以及redis.LOG_WARNING
            redis.set("LOG_DEBUG", 0)?;
            redis.set("LOG_VERBOSE", 1)?;
            redis.set("LOG_NOTICE", 2)?;
            redis.set("LOG_WARNING", 3)?;

            // redis.log
            // 打印日志
            let log = lua.create_function_mut(|_, (level, msg): (u8, String)| {
                // TODO:
                match level {
                    0 => debug!("DEBUG: {}", msg),
                    1 => debug!("VERBOSE: {}", msg),
                    2 => debug!("NOTICE: {}", msg),
                    3 => debug!("WARNING: {}", msg),
                    _ => debug!("UNKNOWN: {}", msg),
                }
                Ok(())
            })?;
            redis.set("log", log)?;

            global.set("redis", redis)?; // 创建全局redis表格
        }

        self.luas.push(TryLock::new(lua));

        let lua = loop {
            if let Some(lock) = self.luas.last().unwrap().try_lock() {
                break lock;
            }
            std::thread::sleep(Duration::from_secs(1));
        };

        self.index.store((index + 1) % (len + 1), Ordering::Relaxed);
        Ok(lua)
    }

    // TODO: 支持批处理，hook
    pub async fn eval_ro(
        &mut self,
        script: Bytes,
        keys: Vec<Key>,
        argv: Vec<Bytes>,
    ) -> Result<RESP3, ServerError> {
        let res = async {
            let mut lua = self.get_lua()?;
            let lua = &mut lua;

            let global = lua.globals();

            // 传入KEYS和ARGV
            let lua_keys = global.get::<_, LuaTable>("KEYS")?;
            for (i, key) in keys.into_iter().enumerate() {
                // PERF: 会clone key
                lua_keys.set(
                    i + 1,
                    std::str::from_utf8(&key)
                        .map_err(|e| LuaError::external(format!("invalid key: {}", e)))?,
                )?;
            }

            let lua_argv = global.get::<_, LuaTable>("ARGV")?;
            for (i, arg) in argv.into_iter().enumerate() {
                // PERF: 会clone arg
                lua_argv.set(
                    i + 1,
                    std::str::from_utf8(&arg)
                        .map_err(|e| LuaError::external(format!("invalid arg: {}", e)))?,
                )?;
            }

            // 执行脚本，若脚本有错误则中断脚本
            let res: RESP3 = lua.load(script.as_ref()).eval_async().await?;

            // 清理Lua环境
            lua_keys.clear()?;
            lua_argv.clear()?;
            lua.gc_collect()?;

            Ok::<RESP3, anyhow::Error>(res)
        }
        .await;

        res.map_err(|e| ServerError::from(e.to_string()))
    }

    pub async fn eval(
        &mut self,
        script: Bytes,
        keys: Vec<Key>,
        argv: Vec<Bytes>,
    ) -> Result<RESP3, ServerError> {
        // TODO: 锁住要操作的键

        let res = async {
            let mut lua = self.get_lua()?;
            let lua = &mut lua;

            let global = lua.globals();

            // 传入KEYS和ARGV
            let lua_keys = global.get::<_, LuaTable>("KEYS")?;
            for (i, key) in keys.into_iter().enumerate() {
                // PERF: 会clone key
                lua_keys.set(
                    i + 1,
                    std::str::from_utf8(&key)
                        .map_err(|e| LuaError::external(format!("invalid key: {}", e)))?,
                )?;
            }

            let lua_argv = global.get::<_, LuaTable>("ARGV")?;
            for (i, arg) in argv.into_iter().enumerate() {
                // PERF: 会clone arg
                lua_argv.set(
                    i + 1,
                    std::str::from_utf8(&arg)
                        .map_err(|e| LuaError::external(format!("invalid arg: {}", e)))?,
                )?;
            }

            // 执行脚本，若脚本有错误则中断脚本
            let res: RESP3 = lua.load(script.as_ref()).eval_async().await?;

            // 清理Lua环境
            lua.gc_collect()?;

            Ok::<RESP3, anyhow::Error>(res)
        }
        .await;

        res.map_err(|e| ServerError::from(e.to_string()))
    }

    // 保存脚本的名称和脚本内容
    pub fn register_script(&self, script_name: Bytes, script: Bytes) -> Result<(), CmdError> {
        match self.lua_scripts.entry(script_name) {
            Entry::Vacant(entry) => {
                entry.insert(script);
                Ok(())
            }
            Entry::Occupied(_) => Err("script already exists".into()),
        }
    }

    // 通过脚本名称删除脚本
    pub fn remove_script(&self, script_name: Bytes) -> Result<(), CmdError> {
        match self.lua_scripts.remove(&script_name) {
            Some(_) => Ok(()),
            None => Err("script not found".into()),
        }
    }

    // 通过脚本名称执行脚本
    pub async fn eval_script(
        &mut self,
        script_name: Bytes,
        keys: Vec<Key>,
        argv: Vec<Bytes>,
    ) -> Result<RESP3, ServerError> {
        let script = match self.lua_scripts.get(&script_name) {
            Some(script) => script.clone(),
            None => return Err("script not found".into()),
        };

        self.eval_ro(script, keys, argv).await
    }
}

#[tokio::test]
async fn lua_tests() {
    crate::util::test_init();

    let mut lua_script = LuaScript::new(Shared::default(), Arc::new(Conf::default()));

    lua_script
        .eval_ro(r#"print("exec")"#.into(), vec![], vec![])
        .await
        .unwrap();

    let res = lua_script
        .eval_ro(r#"return redis.call("ping")"#.into(), vec![], vec![])
        .await
        .unwrap();
    assert_eq!(res, RESP3::SimpleString("PONG".to_string()));

    let res = lua_script
        .eval(
            r#"return redis.call("set", KEYS[1], ARGV[1])"#.into(),
            vec!["key".into()],
            vec!["value".into()],
        )
        .await
        .unwrap();
    assert_eq!(res, RESP3::SimpleString("OK".to_string()),);

    let res = lua_script
        .eval_ro(
            r#"return redis.call("get", KEYS[1])"#.into(),
            vec!["key".into()],
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(res, RESP3::SimpleString("value".into()));

    let res = lua_script
        .eval_ro(
            r#"return { err = 'ERR My very special table error' }"#.into(),
            vec![],
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(
        res,
        RESP3::SimpleError("ERR My very special table error".to_string()),
    );

    let script = r#"return redis.call("ping")"#;

    // 创建脚本
    lua_script
        .register_script("f1".into(), script.into())
        .unwrap();

    // 执行保存的脚本
    let res = lua_script
        .eval_script("f1".into(), vec![], vec![])
        .await
        .unwrap();
    assert_eq!(res, RESP3::SimpleString("PONG".to_string()));

    // 删除脚本
    lua_script.remove_script("f1".into()).unwrap();

    lua_script
        .eval_script("f1".into(), vec![], vec![])
        .await
        .unwrap_err();
}
