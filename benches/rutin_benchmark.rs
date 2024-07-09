use std::time::Instant;

use bytes::Bytes;
use bytestring::ByteString;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rutin::{frame::Resp3, server::Handler};
use smallvec::{smallvec, SmallVec};

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

fn bench_vec(c: &mut Criterion) {
    let mut group = c.benchmark_group("dispatch");

    group.bench_function("smallvec", |b| {
        b.iter(|| {
            let _vec: SmallVec<[u8; 10]> = black_box(smallvec![0; 10]);
        });
    });

    group.bench_function("vec", |b| {
        b.iter(|| {
            let _vec: Vec<u8> = black_box(vec![0; 10]);
        });
    });

    group.finish();
}

// [688.22 ns 690.18 ns 692.34 ns]
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

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("dispatch");

    group.bench_function("smallvec", |b| {
        b.iter(|| {
            let mut i = 0;
            for _ in 0..10000000 {
                i += 1;
            }
            println!("{i}");
        });
    });

    group.bench_function("vec", |b| {
        b.iter(|| {
            let i = std::sync::atomic::AtomicI32::default();
            for _ in 0..10000000 {
                i.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            assert_eq!(i.load(std::sync::atomic::Ordering::Relaxed), 10000000);
        });
    });

    group.finish();
}

// criterion_group!(benches, bench_dispatch, bench);
criterion_group!(benches, bench);
criterion_main!(benches);
