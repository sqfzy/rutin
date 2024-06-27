use crate::{
    cmd::{CmdError, ServerErrSnafu},
    conf::AccessControl,
    connection::{AsyncStream, FakeStream},
    frame::Resp3,
    server::{Handler, HandlerContext, ServerError, ID},
    shared::Shared,
    Id, Key,
};
use ahash::RandomState;
use bytes::Bytes;
use bytestring::ByteString;
use crossbeam::queue::ArrayQueue;
use dashmap::{mapref::entry::Entry, DashMap};
use event_listener::listener;
use mlua::{prelude::*, StdLib};
use snafu::ResultExt;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio_util::task::LocalPoolHandle;
use tracing::debug;
use try_lock::TryLock;

// TODO: 新增排序辅助函数
#[derive(Debug)]
pub struct LuaScript {
    /// 由于Lua的异步function不满足Send，因此Lua脚本需要在线程池[`LocalPoolHandle`]中执行(该线程池中的异步任务
    /// 不会被其它线程窃取，因此不必满足Send)。
    pool: LocalPoolHandle,

    /// 最大创建的Lua环境的数量
    max: usize,

    /// 当前创建的Lua环境的数量
    count: AtomicUsize,

    /// 用于通知等待的任务，有可用的Lua环境
    event: Arc<event_listener::Event>,

    /// Lua环境保存在无锁队列[`ArrayQueue`]中，Lua环境最多有[`max`]个，当没有可用的Lua环境时，异步等待空闲的Lua环境。
    luas: ArrayQueue<LuaEnv>,

    /// script_name -> script
    lua_scripts: DashMap<Bytes, Bytes, RandomState>,
}

/// Lua环境包含：
/// 全局的KEYS table: 用于存储Redis的键
/// 全局的ARGV table: 用于存储Redis的参数
/// 全局的redis table: 包含了redis.call, redis.pcall, redis.status_reply, redis.error_reply, redis.log等方法
/// fake handler: 用于执行Redis命令。fake handler的执行权限应当与客户端的权限保持一致
struct LuaEnv {
    lua: Lua,
    fake_handler: Arc<TryLock<Handler<FakeStream>>>,
}

impl Default for LuaScript {
    fn default() -> Self {
        let max = num_cpus::get();
        Self {
            pool: LocalPoolHandle::new(max),
            max,
            count: AtomicUsize::new(0),
            event: Arc::new(event_listener::Event::new()),
            luas: ArrayQueue::new(max),
            lua_scripts: DashMap::with_hasher(RandomState::default()),
        }
    }
}

impl LuaScript {
    fn create_lua(&self, shared: Shared) -> Result<(), ServerError> {
        (|| -> anyhow::Result<()> {
            // 创建新的Lua环境
            let libs = StdLib::TABLE | StdLib::STRING | StdLib::MATH;
            let lua = Lua::new_with(libs, LuaOptions::default())?;

            let old_count = self.count.fetch_add(1, Ordering::Acquire) as Id;
            let handler = Handler::new_fake_with(
                shared,
                None,
                Some(HandlerContext::new(old_count, AccessControl::new_strict())),
            )
            .0;
            let handler = Arc::new(TryLock::new(handler));

            // LuaEnv, 可修改的
            {
                // 停止自动GC，只通过手动方式触发GC
                lua.gc_stop();

                let global = lua.globals();

                let keys = lua.create_table()?;
                let argv = lua.create_table()?;

                global.set("KEYS", keys)?; // 全局变量KEYS，用于存储Redis的键
                global.set("ARGV", argv)?; // 全局变量ARGV，用于存储Redis的参数

                // 使用固定的随机数种子
                let seed = 0;
                lua.load(format!("math.randomseed({})", seed).as_str())
                    .exec()?;

                // 创建全局redis表格
                let redis = lua.create_table()?;

                // redis.call
                // 执行脚本，当发生运行时错误时，中断脚本
                let call = lua.create_async_function({
                    let handler = handler.clone();

                    move |lua, cmd: LuaMultiValue| {
                        let handler = handler.clone();

                        async move {
                            let mut cmd_frame = Vec::with_capacity(cmd.len());
                            for v in cmd {
                                match v {
                                    LuaValue::String(s) => {
                                        // PERF: 拷贝
                                        cmd_frame.push(Resp3::new_blob_string(
                                            Bytes::copy_from_slice(s.as_bytes()),
                                        ))
                                    }
                                    _ => {
                                        return Err(LuaError::external(
                                            "redis.call only accept string arguments",
                                        ));
                                    }
                                }
                            }
                            let cmd_frame = Resp3::new_array(cmd_frame);

                            debug!("lua call: {:?}", cmd_frame);

                            let mut handler = handler.try_lock().unwrap();
                            // 将old_count作为fake handler的ID，大小不超过[`RESERVE_MAX_ID`]，
                            // 确保每个Lua环境的handler的ID唯一且不与client handler的ID冲突
                            ID.scope(handler.context.client_id, async move {
                                match handler.dispatch(cmd_frame).await {
                                    Ok(ei) => match ei {
                                        Some(res) => Ok(res.into_lua(lua)),
                                        None => Ok(Resp3::<Bytes, ByteString>::Null.into_lua(lua)),
                                    },
                                    // 中断脚本，返回运行时错误
                                    Err(e) => Err(LuaError::external(format!("ERR {}", e))),
                                }
                            })
                            .await
                        }
                    }
                })?;
                redis.set("call", call)?;

                // redis.pcall
                // 执行脚本，当发生运行时错误时，返回一张表，{ err: Lua String }
                let pcall = lua.create_async_function({
                    let handler = handler.clone();

                    move |lua, cmd: LuaMultiValue| {
                        let handler = handler.clone();

                        async move {
                            let handler = handler;

                            let mut cmd_frame = Vec::with_capacity(cmd.len());
                            for v in cmd {
                                match v {
                                    LuaValue::String(s) => {
                                        // PERF: 拷贝
                                        cmd_frame.push(Resp3::new_blob_string(
                                            Bytes::copy_from_slice(s.as_bytes()),
                                        ))
                                    }
                                    _ => {
                                        return Err(LuaError::external(
                                            "redis.call only accept string arguments",
                                        ));
                                    }
                                }
                            }
                            let cmd_frame = Resp3::new_array(cmd_frame);

                            debug!("lua call: {:?}", cmd_frame);

                            let mut handler = handler.try_lock().unwrap();
                            ID.scope(handler.context.client_id, async {
                                match handler.dispatch(cmd_frame).await {
                                    Ok(ei) => match ei {
                                        Some(res) => Ok(res.into_lua(lua)),
                                        None => Ok(Resp3::<Bytes, ByteString>::Null.into_lua(lua)),
                                    },
                                    // 不返回运行时错误，而是返回一个表 { err: Lua String }
                                    Err(e) => {
                                        Ok(Resp3::new_simple_error(e.to_string()).into_lua(lua))
                                    }
                                }
                            })
                            .await
                        }
                    }
                })?;
                redis.set("pcall", pcall)?;

                // redis.status_reply
                // 返回一张表, { ok: Lua String }
                let status_reply = lua.create_function_mut(|lua, ok: String| {
                    Resp3::new_simple_string(ok).into_lua(lua)
                })?;
                redis.set("status_reply", status_reply)?;

                // redis.error_reply
                // 返回一张表, { err: Lua String }
                let error_reply = lua.create_function_mut(|lua, err: String| {
                    Resp3::new_simple_error(err).into_lua(lua)
                })?;
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

                // 设置元方法 __newindex 禁止增删全局变量
                let metatable = lua.create_table()?;
                metatable.set(
                    "__newindex",
                    lua.create_function(|_, (_t, _n, _v): (LuaValue, LuaValue, LuaValue)| {
                        Err::<(), _>(LuaError::external("global variable is readonly"))
                    })?,
                )?;

                global.set_metatable(Some(metatable));
            }

            let lua_env = LuaEnv {
                lua,
                fake_handler: handler,
            };
            self.push_lua(lua_env);

            Ok(())
        })()
        .map_err(|e| ServerError::from(e.to_string()))
    }

    async fn pop_lua(&self, shared: &Shared) -> Result<LuaEnv, ServerError> {
        // 尝试从已有的Lua环境中获取一个可用的Lua环境
        if let Some(lua) = self.luas.pop() {
            return Ok(lua);
        } else {
            let count = self.count.load(Ordering::Relaxed);
            if count == self.max {
                // 等待一个可用的Lua环境
                let event = &self.event;
                listener!(event => listener);

                listener.await
            } else {
                // 如果没有达到最大数量，则创建一个新的Lua环境
                self.create_lua(shared.clone())?;
            }
        }

        // 只允许创建一次Lua环境，之后必须从已有的Lua环境中获取
        loop {
            if let Some(lua) = self.luas.pop() {
                return Ok(lua);
            } else {
                // 等待一个可用的Lua环境
                let event = &self.event;
                listener!(event => listener);

                listener.await
            }
        }
    }

    fn push_lua(&self, lua_env: LuaEnv) {
        debug_assert!(!self.luas.is_full());

        let _ = self.luas.push(lua_env);

        // 通知等待的任务，有可用的Lua环境
        self.event.notify_relaxed(1);
    }

    pub async fn eval(
        &self,
        handler: &Handler<impl AsyncStream>,
        chunk: Bytes,
        keys: Vec<Key>,
        argv: Vec<Bytes>,
    ) -> Result<Resp3, ServerError> {
        let shared = handler.shared.clone();
        let client_ac = handler.context.ac.clone();

        let script = shared.script().clone();

        let res = self
            .pool
            .spawn_pinned(|| async move {
                let lua_env = script.lua_script.pop_lua(&shared).await?;

                let res = {
                    let LuaEnv { lua, fake_handler } = &lua_env;

                    let mut fake_handler = fake_handler.try_lock().unwrap();
                    // 脚本执行的权限与客户端的权限一致
                    fake_handler.context.ac = client_ac;

                    let mut intention_locks = Vec::with_capacity(keys.len());
                    // 给需要操作的键加上意向锁
                    for key in &keys {
                        if let Some(notify_unlock) = shared
                            .db()
                            .add_lock_event(key.clone(), fake_handler.context.client_id)
                            .await
                        {
                            intention_locks.push(notify_unlock);
                        }
                    }

                    drop(fake_handler);

                    let global = lua.globals();

                    // 传入KEYS和ARGV
                    let lua_keys = global.get::<_, LuaTable>("KEYS")?;
                    for (i, key) in keys.into_iter().enumerate() {
                        lua_keys.set(i + 1, Resp3::<bytes::Bytes, String>::new_blob_string(key))?;
                    }

                    let lua_argv = global.get::<_, LuaTable>("ARGV")?;
                    for (i, arg) in argv.into_iter().enumerate() {
                        lua_argv.set(i + 1, Resp3::<bytes::Bytes, String>::new_blob_string(arg))?;
                    }

                    // 执行脚本，若脚本有错误则中断脚本
                    let res: Resp3 = lua.load(chunk.as_ref()).eval_async().await?;

                    // 脚本执行完毕，唤醒一个等待的任务
                    for intention_lock in intention_locks {
                        intention_lock.unlock();
                    }

                    // 清理Lua环境
                    lua_keys.clear()?;
                    lua_argv.clear()?;
                    lua.gc_collect()?;
                    res
                };

                // 将Lua环境放回队列
                script.lua_script.push_lua(lua_env);

                Ok::<Resp3, anyhow::Error>(res)
            })
            .await;

        res.map_err(|e| ServerError::from(e.to_string()))?
            .map_err(|e| ServerError::from(e.to_string()))
    }

    // 通过脚本名称执行脚本
    pub async fn eval_name(
        &self,
        handler: &Handler<impl AsyncStream>,
        script_name: Bytes,
        keys: Vec<Key>,
        argv: Vec<Bytes>,
    ) -> Result<Resp3, CmdError> {
        let chunk = match self.lua_scripts.get(&script_name) {
            Some(script) => script.clone(),
            None => return Err("script not found".into()),
        };

        self.eval(handler, chunk, keys, argv)
            .await
            .context(ServerErrSnafu)
    }

    pub fn contain(&self, names: &Bytes) -> bool {
        self.lua_scripts.contains_key(names)
    }

    pub fn flush(&self) {
        self.lua_scripts.clear();
    }

    // 保存脚本的名称和脚本内容
    pub fn register_script(&self, script_name: Bytes, chunk: Bytes) -> Result<(), CmdError> {
        match self.lua_scripts.entry(script_name) {
            Entry::Vacant(entry) => {
                entry.insert(chunk);
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
}

#[tokio::test]
async fn lua_tests() {
    crate::util::test_init();

    let shared = Shared::default();
    let pool = tokio_util::task::LocalPoolHandle::new(1);

    pool.spawn_pinned(|| async move {
        let lua_script = LuaScript::default();

        {
            let LuaEnv { lua, .. } = lua_script.pop_lua(&shared).await.unwrap();

            // 不允许require module
            lua.load(
                r#"
        -- 测试 require (应该失败)
        require("module")
    "#,
            )
            .exec()
            .unwrap_err();

            // 不允许增加新的全局变量
            lua.load(
                r#"
        -- 测试增加新的全局变量 (应该失败)
        x = 10
    "#,
            )
            .exec()
            .unwrap_err();
        }

        let handler = Handler::new_fake().0;

        lua_script
            .eval(&handler, r#"print("exec")"#.into(), vec![], vec![])
            .await
            .unwrap();

        let res = lua_script
            .eval(
                &handler,
                r#"return redis.call("ping")"#.into(),
                vec![],
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(res, Resp3::new_simple_string("PONG".into()));

        let res = lua_script
            .eval(
                &handler,
                r#"return redis.call("set", KEYS[1], ARGV[1])"#.into(),
                vec!["key".into()],
                vec!["value".into()],
            )
            .await
            .unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()),);

        let res = lua_script
            .eval(
                &handler,
                r#"return redis.call("get", KEYS[1])"#.into(),
                vec!["key".into()],
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(res, Resp3::new_blob_string("value".into()));

        let res = lua_script
            .eval(
                &handler,
                r#"return { err = 'ERR My very special table error' }"#.into(),
                vec![],
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(
            res,
            Resp3::new_simple_error("ERR My very special table error".into()),
        );

        let script = r#"return redis.call("ping")"#;

        // 创建脚本
        lua_script
            .register_script("f1".into(), script.into())
            .unwrap();

        // 执行保存的脚本
        let res = lua_script
            .eval_name(&handler, "f1".into(), vec![], vec![])
            .await
            .unwrap();
        assert_eq!(res, Resp3::new_simple_string("PONG".into()));

        // 删除脚本
        lua_script.remove_script("f1".into()).unwrap();

        lua_script
            .eval_name(&handler, "f1".into(), vec![], vec![])
            .await
            .unwrap_err();
    });
}
