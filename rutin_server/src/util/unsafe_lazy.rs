use std::cell::LazyCell;
use std::ops::Deref;

pub struct UnsafeLazy<T, F = fn() -> T>(LazyCell<T, F>);

impl<T, F: FnOnce() -> T> UnsafeLazy<T, F> {
    pub const fn new(f: F) -> Self {
        Self(LazyCell::new(f))
    }

    /// # Safety
    ///
    /// 必须在进入多线程之前初始化.
    pub unsafe fn init(&self) {
        LazyCell::force(&self.0);
    }
}

unsafe impl<T, F: FnOnce() -> T> Send for UnsafeLazy<T, F> {}
unsafe impl<T, F: FnOnce() -> T> Sync for UnsafeLazy<T, F> {}

impl<T, F: FnOnce() -> T> Deref for UnsafeLazy<T, F> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // TODO: 如果没有初始化，应该 panic
        &self.0
    }
}

impl<T: PartialEq, F: FnOnce() -> T> PartialEq for UnsafeLazy<T, F> {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}

impl<T: PartialEq, F: FnOnce() -> T> PartialEq<T> for UnsafeLazy<T, F> {
    fn eq(&self, other: &T) -> bool {
        self.deref().eq(other)
    }
}

impl<T: Eq, F: FnOnce() -> T> Eq for UnsafeLazy<T, F> {}

impl<T: PartialOrd, F: FnOnce() -> T> PartialOrd for UnsafeLazy<T, F> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl<T: PartialOrd, F: FnOnce() -> T> PartialOrd<T> for UnsafeLazy<T, F> {
    fn partial_cmp(&self, other: &T) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other)
    }
}

impl<T: Ord, F: FnOnce() -> T> Ord for UnsafeLazy<T, F> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deref().cmp(other.deref())
    }
}

impl<T: std::fmt::Debug, F: FnOnce() -> T> std::fmt::Debug for UnsafeLazy<T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("UnsafeLazy").field(self.deref()).finish()
    }
}

impl<T: std::hash::Hash, F: FnOnce() -> T> std::hash::Hash for UnsafeLazy<T, F> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.deref().hash(state)
    }
}
