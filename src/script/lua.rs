use crate::{shared::db::Db, Key};
use bytes::Bytes;
use dashmap::DashMap;
use mlua::{prelude::*, StdLib};

struct LuaScript {
    lua: Lua,
    // lua_scripts: DashMap<Bytes, >
}

// 创建全局redis表格，redis.call, redis.pacll执行Redis命令, redis.sha1hex, redis.error_reply, redis.status_reply
// 更换random函数
// 新增排序辅助函数
// sandbox: 不允许增删全局变量，不允许require module
//
impl LuaScript {
    // 创建Lua环境
    pub fn new(db: Db) -> anyhow::Result<Self> {
        let libs = StdLib::TABLE | StdLib::STRING | StdLib::MATH;
        let lua = Lua::new_with(libs, LuaOptions::default())?;
        {
            let global = lua.globals();

            let keys = lua.create_table()?;
            let argv = lua.create_table()?;

            global.set("KEYS", keys)?; // 全局变量KEYS，用于存储Redis的键
            global.set("ARGV", argv)?; // 全局变量ARGV，用于存储Redis的参数

            let redis = lua.create_table()?;
            let call = lua
                .create_function(move |_, ()| {
                    let db = &db;
                    Ok(())
                })
                .unwrap();
            redis.set("call", call)?;
        }

        Ok(LuaScript { lua })
    }

    pub fn eval(&self, sha: &str) {
        let global = self.lua.globals();
    }
}
