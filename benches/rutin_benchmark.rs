use std::time::Instant;

use bytes::Bytes;
use bytestring::ByteString;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rutin::{frame::Resp3, server::Handler};

fn gen_get_cmd(key: &'static str) -> Resp3 {
    Resp3::new_array(vec![
        Resp3::<Bytes, ByteString>::new_blob_string("GET".into()),
        Resp3::new_blob_string(key.into()),
    ])
}

fn gen_set_cmd(key: &'static str, value: &'static str) -> Resp3 {
    Resp3::new_array(vec![
        Resp3::<Bytes, ByteString>::new_blob_string("SET".into()),
        Resp3::new_blob_string(key.into()),
        Resp3::new_blob_string(value.into()),
    ])
}

// [767.00 ns 772.64 ns 778.44 ns]
fn bench_dispatch(c: &mut Criterion) {
    c.bench_function("dispatch", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(rt).iter_custom(|iters| async move {
            let (mut handler, _client) = Handler::new_fake();
            let start = Instant::now();
            for _ in 0..iters {
                handler
                    .dispatch(gen_set_cmd(black_box("key"), black_box("value")))
                    .await
                    .unwrap()
                    .unwrap();
                handler
                    .dispatch(gen_get_cmd(black_box("key")))
                    .await
                    .unwrap()
                    .unwrap();
            }
            start.elapsed()
        })
    });
}

criterion_group!(benches, bench_dispatch);
criterion_main!(benches);
