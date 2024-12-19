pub mod lua;

pub use lua::*;

// TODO: 支持WASM脚本

#[derive(Debug, Default)]
pub struct Script {
    pub lua_script: LuaScript,
}

impl Script {
    pub fn new() -> Self {
        Script {
            lua_script: LuaScript::default(),
        }
    }

    pub fn clear(&self) {
        self.lua_script.clear();
    }
}
