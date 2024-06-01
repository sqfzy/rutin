use crate::{
    conf::Conf,
    server::{Handler, HandlerContext},
    shared::Shared,
    Connection,
};
use bytes::BytesMut;
use flume::{
    r#async::{RecvFut, SendFut},
    Receiver, Sender,
};
use futures::Future;
use pin_project::{pin_project, pinned_drop};
use std::task::{Context, Poll};
use std::{pin::Pin, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

