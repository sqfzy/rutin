use crate::{
    conf::{AccessControl, DEFAULT_USER},
    error::{RutinError, RutinResult},
    frame::Resp3,
    server::{AsyncStream, FakeHandler, Handler, HandlerContext},
    shared::{Shared, NULL_ID},
};
use ahash::RandomState;
use bytes::Bytes;
use bytestring::ByteString;
use dashmap::{mapref::entry::Entry, DashMap};
use futures_intrusive::sync::LocalMutex;
use mlua::{prelude::*, StdLib};
use std::{rc::Rc, sync::Arc};
use tracing::debug;

thread_local! {
    // PERF:
    // 由于client handler执行脚本过程中需要修改并保持Lua环境直到脚本执行完毕，
    // 而同一线程的多个client handler会共享一个Lua环境，因此同一时刻本线程的
    // Lua环境只能被一个client handler使用。使用LocalMutex可以保证线程内的task
    // 互斥。
    static LUA: Rc<LocalMutex<Option<Lua>>> = Rc::new(LocalMutex::new(None, false));
    static FAKE_HANDLER: Rc<LocalMutex<Option<FakeHandler>>> = Rc::new(LocalMutex::new(None, false));
}

/// Lua环境包含：
/// 全局的KEYS table: 用于存储Redis的键
/// 全局的ARGV table: 用于存储Redis的参数
/// 全局的redis table: 包含了redis.call, redis.pcall, redis.status_reply, redis.error_reply, redis.log等方法
/// fake handler: 用于执行Redis命令。fake handler的执行权限应当与客户端的权限保持一致
#[allow(clippy::type_complexity)]
fn get_or_create_lua_env(
    shared: Shared,
) -> anyhow::Result<(
    Rc<LocalMutex<Option<Lua>>>,
    Rc<LocalMutex<Option<FakeHandler>>>,
)> {
    let lua_env = LUA.with(|lua| {
        FAKE_HANDLER.with(|handler| {
            if let Some(lua) = lua.try_lock() {
                if lua.is_none() {
                    return None;
                }
            }

            Some((lua.clone(), handler.clone()))
        })
    });

    if let Some((lua, fake_handler)) = lua_env {
        return Ok((lua, fake_handler));
    }

    let (outbox, inbox) = shared.post_office().new_mailbox_with_special_id(NULL_ID);
    let context = HandlerContext::with_ac(
        NULL_ID,
        outbox,
        inbox,
        Arc::new(AccessControl::new_strict()),
        DEFAULT_USER,
    );
    let fake_handler = Handler::new_fake_with(shared, Some(context), None).0;

    let fake_handler = FAKE_HANDLER.with(|handler| {
        *handler.try_lock().unwrap() = Some(fake_handler);
        handler.clone()
    });

    let libs = StdLib::TABLE | StdLib::STRING | StdLib::MATH;
    let lua = Lua::new_with(libs, LuaOptions::default()).unwrap();

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
            let fake_handler = fake_handler.clone();

            move |lua, cmd: LuaMultiValue| {
                let fake_handler = fake_handler.clone();

                async move {
                    let mut cmd_frame = Vec::with_capacity(cmd.len());
                    for v in cmd {
                        match v {
                            LuaValue::String(s) => {
                                // PERF: 拷贝
                                cmd_frame.push(Resp3::new_blob_string(Bytes::copy_from_slice(
                                    s.as_bytes(),
                                )))
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

                    // 如果cmd_frame是写命令，则会立即传播
                    match fake_handler
                        .lock()
                        .await
                        .as_mut()
                        .unwrap()
                        .dispatch(cmd_frame)
                        .await
                    {
                        Ok(ei) => match ei {
                            Some(res) => Ok(res.into_lua(lua)),
                            None => Ok(Resp3::<Bytes, ByteString>::Null.into_lua(lua)),
                        },
                        // 中断脚本，返回运行时错误
                        Err(e) => Err(LuaError::external(format!("ERR {}", e))),
                    }
                }
            }
        })?;
        redis.set("call", call)?;

        // redis.pcall
        // 执行脚本，当发生运行时错误时，返回一张表，{ err: Lua String }
        let pcall = lua.create_async_function({
            let fake_handler = fake_handler.clone();

            move |lua, cmd: LuaMultiValue| {
                let fake_handler = fake_handler.clone();

                async move {
                    let mut cmd_frame = Vec::with_capacity(cmd.len());
                    for v in cmd {
                        match v {
                            LuaValue::String(s) => {
                                // PERF: 拷贝
                                cmd_frame.push(Resp3::new_blob_string(Bytes::copy_from_slice(
                                    s.as_bytes(),
                                )))
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

                    match fake_handler
                        .lock()
                        .await
                        .as_mut()
                        .unwrap()
                        .dispatch(cmd_frame)
                        .await
                    {
                        Ok(ei) => match ei {
                            Some(res) => Ok(res.into_lua(lua)),
                            None => Ok(Resp3::<Bytes, ByteString>::Null.into_lua(lua)),
                        },
                        // 不返回运行时错误，而是返回一个表 { err: Lua String }
                        Err(e) => Ok(Resp3::new_simple_error(e.to_string()).into_lua(lua)),
                    }
                }
            }
        })?;
        redis.set("pcall", pcall)?;

        // redis.status_reply
        // 返回一张表, { ok: Lua String }
        let status_reply =
            lua.create_function_mut(|lua, ok: String| Resp3::new_simple_string(ok).into_lua(lua))?;
        redis.set("status_reply", status_reply)?;

        // redis.error_reply
        // 返回一张表, { err: Lua String }
        let error_reply =
            lua.create_function_mut(|lua, err: String| Resp3::new_simple_error(err).into_lua(lua))?;
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

    let lua = LUA.with(|l| {
        *l.try_lock().unwrap() = Some(lua);
        l.clone()
    });

    Ok((lua, fake_handler))
}

// TODO: 新增排序辅助函数
#[derive(Debug)]
pub struct LuaScript {
    /// script_name -> script
    lua_scripts: DashMap<Bytes, Bytes, RandomState>,
}

impl Default for LuaScript {
    fn default() -> Self {
        Self {
            lua_scripts: DashMap::with_hasher(RandomState::default()),
        }
    }
}

impl LuaScript {
    pub fn clear(&self) {
        self.lua_scripts.clear();
    }

    pub async fn eval(
        &self,
        handler: &mut Handler<impl AsyncStream>,
        chunk: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
    ) -> RutinResult<Resp3> {
        let shared = handler.shared;

        async {
            let (lua, fake_handler) = get_or_create_lua_env(shared)?;

            // 加锁，确保该async block只能有一个task，即一个client handler在执行
            let mut lua = lua.lock().await;
            let lua = lua.as_mut().unwrap();

            let mut intention_locks = Vec::with_capacity(keys.len());
            {
                let mut fake_handler = fake_handler.lock().await;
                let fake_handler = fake_handler.as_mut().unwrap();

                // 将client handler的上下文复制到fake handler
                std::mem::swap(&mut fake_handler.context, &mut handler.context);

                // 给需要操作的键加上意向锁
                for key in &keys {
                    if let Some(notify_unlock) = shared
                        .db()
                        .add_lock_event(key.clone().into(), fake_handler.context.id)
                        .await?
                    {
                        intention_locks.push(notify_unlock);
                    }
                }
                // 释放fake_handler的锁，以便在执行脚本时可以获取fake_handler的锁，
                // 由于Lua的锁还在，因此不会有其他task尝试获取fake_handler的锁
            }

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

            // 执行脚本
            let res: Resp3 = lua
                .load(chunk.as_ref())
                .eval_async()
                .await
                .unwrap_or_else(|e| Resp3::new_simple_error(e.to_string().into()));

            // 脚本执行完毕，唤醒一个等待的任务
            for intention_lock in intention_locks {
                intention_lock.unlock();
            }

            // 清理Lua环境
            lua_keys.clear()?;
            lua_argv.clear()?;
            lua.gc_collect()?;

            // 换回client handler的上下文
            let mut fake_handler = fake_handler.lock().await;
            let fake_handler = fake_handler.as_mut().unwrap();
            std::mem::swap(&mut fake_handler.context, &mut handler.context);

            Ok::<Resp3, anyhow::Error>(res)
        }
        .await
        .map_err(|e| RutinError::from(e.to_string()))
    }

    // 通过脚本名称执行脚本
    pub async fn eval_name(
        &self,
        handler: &mut Handler<impl AsyncStream>,
        script_name: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
    ) -> RutinResult<Resp3> {
        let chunk = match self.lua_scripts.get(&script_name) {
            Some(script) => script.clone(),
            None => return Err("script not found".into()),
        };

        self.eval(handler, chunk, keys, argv).await
    }

    pub fn contain(&self, names: &Bytes) -> bool {
        self.lua_scripts.contains_key(names)
    }

    pub fn flush(&self) {
        self.lua_scripts.clear();
    }

    // 保存脚本的名称和脚本内容
    pub fn register_script(&self, script_name: Bytes, chunk: Bytes) -> RutinResult<()> {
        match self.lua_scripts.entry(script_name) {
            Entry::Vacant(entry) => {
                entry.insert(chunk);
                Ok(())
            }
            Entry::Occupied(_) => Err("script already exists".into()),
        }
    }

    // 通过脚本名称删除脚本
    pub fn remove_script(&self, script_name: Bytes) -> RutinResult<()> {
        match self.lua_scripts.remove(&script_name) {
            Some(_) => Ok(()),
            None => Err("script not found".into()),
        }
    }
}

#[tokio::test]
async fn lua_tests() {
    crate::util::test_init();

    let pool = tokio_util::task::LocalPoolHandle::new(1);

    pool.spawn_pinned(|| async move {
        let lua_script = LuaScript::default();

        let mut handler = Handler::new_fake().0;

        lua_script
            .eval(&mut handler, r#"print("exec")"#.into(), vec![], vec![])
            .await
            .unwrap();

        // 不允许require module
        let res = lua_script
            .eval(&mut handler, r#"require("module")"#.into(), vec![], vec![])
            .await
            .unwrap();
        assert!(res.is_simple_error());

        //  不允许增加新的全局变量
        let res = lua_script
            .eval(&mut handler, "x = 10".into(), vec![], vec![])
            .await
            .unwrap();
        assert!(res.is_simple_error());

        let res = lua_script
            .eval(
                &mut handler,
                r#"return redis.call("ping")"#.into(),
                vec![],
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(res, Resp3::new_simple_string("PONG".into()));

        let res = lua_script
            .eval(
                &mut handler,
                r#"return redis.call("set", KEYS[1], ARGV[1])"#.into(),
                vec!["key".into()],
                vec!["value".into()],
            )
            .await
            .unwrap();
        assert_eq!(res, Resp3::new_simple_string("OK".into()),);

        let res = lua_script
            .eval(
                &mut handler,
                r#"return redis.call("get", KEYS[1])"#.into(),
                vec!["key".into()],
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(res, Resp3::new_blob_string("value".into()));

        let res = lua_script
            .eval(
                &mut handler,
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
            .eval_name(&mut handler, "f1".into(), vec![], vec![])
            .await
            .unwrap();
        assert_eq!(res, Resp3::new_simple_string("PONG".into()));

        // 删除脚本
        lua_script.remove_script("f1".into()).unwrap();

        lua_script
            .eval_name(&mut handler, "f1".into(), vec![], vec![])
            .await
            .unwrap_err();
    })
    .await
    .unwrap();
}
