pub enum NSM<T> {
    None,
    Signal(T),
    Multi(Vec<T>),
}
