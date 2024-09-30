use std::ops::Deref;

// 结构体中存储的数据必须是大写的
#[derive(Debug)]
pub enum Uppercase<const L: usize, M> {
    Const { data: [u8; L], len: usize },
    Mut(M),
}

impl<const L: usize, M> Uppercase<L, M> {
    pub fn from_const(data: &[u8]) -> Self {
        let len = data.len();

        assert!(len <= L, "buf too small");

        let mut buf = [0; L];
        buf[..len].copy_from_slice(data);
        buf.make_ascii_uppercase();

        Self::Const { data: buf, len }
    }
}

impl<const L: usize, M: AsMut<[u8]>> Uppercase<L, M> {
    pub fn from_mut(mut s: M) -> Self {
        s.as_mut().make_ascii_uppercase();
        Self::Mut(s)
    }
}

impl<const L: usize, M: AsRef<[u8]>> Deref for Uppercase<L, M> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<const L: usize, M: AsRef<[u8]>> AsRef<[u8]> for Uppercase<L, M> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Const { data, len } => &data[..*len],
            Self::Mut(s) => s.as_ref(),
        }
    }
}

impl<const L: usize, M: AsRef<[u8]>> PartialEq<[u8]> for Uppercase<L, M> {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_ref() == other
    }
}

impl<const L: usize> From<&[u8]> for Uppercase<L, &mut [u8]> {
    fn from(s: &[u8]) -> Self {
        Uppercase::from_const(s)
    }
}

impl<const L: usize, M: AsMut<[u8]>> From<M> for Uppercase<L, M> {
    fn from(m: M) -> Self {
        Uppercase::from_mut(m)
    }
}
