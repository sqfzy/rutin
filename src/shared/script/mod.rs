pub mod lua;

pub use lua::*;

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
}
