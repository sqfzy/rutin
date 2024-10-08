pub mod db;
pub mod post_office;
pub mod script;

use std::{cell::UnsafeCell, time::Duration};

pub use post_office::*;
pub use script::*;
use tracing::debug;

use crate::{conf::Conf, shared::db::Db};
use tokio_util::task::LocalPoolHandle;

#[derive(Debug, Copy, Clone)]
pub struct Shared {
    pub inner: &'static UnsafeCell<SharedInner>,
}

unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

impl Shared {
    // 整个程序运行期间只会创建一次
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let conf = Conf::new().unwrap();

        Self::with_conf(conf)
    }

    pub fn with_conf(conf: Conf) -> Self {
        Self {
            inner: Box::leak(Box::new(UnsafeCell::new(SharedInner::with_conf(conf)))),
        }
    }

    pub fn inner(&self) -> &'static SharedInner {
        unsafe { &*self.inner.get() }
    }

    /// Returns a mutable reference to the inner of this [`Shared`].
    ///
    /// # Safety
    ///
    /// 可以通过send_reset_server()终止除主任务以外的所有任务，然后在主任务中调用此方法
    /// 修改SharedInner
    pub unsafe fn inner_mut(&self) -> &'static mut SharedInner {
        unsafe { &mut *self.inner.get() }
    }

    #[inline]
    pub fn pool(&self) -> &LocalPoolHandle {
        &self.inner().pool
    }

    #[inline]
    pub fn db(&self) -> &'static Db {
        &self.inner().db
    }

    #[inline]
    pub fn script(&self) -> &'static Script {
        &self.inner().script
    }

    #[inline]
    pub fn conf(&self) -> &'static Conf {
        &self.inner().conf
    }

    #[inline]
    pub fn post_office(&self) -> &'static PostOffice {
        &self.inner().post_office
    }

    pub fn clear(&self) {
        self.db().clear();
        self.script().clear();
    }

    // 关闭所有任务，仅剩主任务
    pub async fn wait_shutdown_complete(&self) {
        loop {
            if self.post_office().inner.is_empty()
                && self
                    .pool()
                    .get_task_loads_for_each_worker()
                    .iter()
                    .sum::<usize>()
                    == 0
            {
                break;
            }

            for entry in self.post_office().inner.iter() {
                debug!("waitting task abort, task id={}", entry.key());
            }

            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }
}

pub struct SharedInner {
    // 任务种类：
    //  1. 特殊任务：主任务，ctrl_c任务，aof任务等。它们会被注册到post_office中
    //  2. handler任务：处理用户请求的任务。它们会被注册到post_office中
    //      1. handler子任务：在处理请求时，可能会创建一些子任务，这些子任务应当在handler任务结束时终止
    //  3. delay_shutdown任务：不应当被中断的任务。它们会被注册到post_office中
    pub pool: LocalPoolHandle,
    pub db: Db,
    pub conf: Conf,
    pub script: Script,
    pub post_office: PostOffice,
}

impl SharedInner {
    pub fn with_conf(conf: Conf) -> Self {
        let db = Db::new(conf.memory.oom.clone());
        let script = Script::new();
        let post_office = PostOffice::new(&conf);

        SharedInner {
            pool: LocalPoolHandle::new(num_cpus::get()),
            db,
            conf,
            script,
            post_office,
        }
    }
}

impl std::fmt::Debug for SharedInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedInner")
            .field("db", &self.db)
            .field("conf", &self.conf)
            .field("script", &self.script)
            .finish()
    }
}
