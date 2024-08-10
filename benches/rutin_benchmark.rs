use std::time::Instant;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rutin::{
    frame::{Resp3, Resp3Decoder, Resp3Encoder},
    server::{Handler, HandlerContext},
    util::get_test_shared,
};
use tokio_util::codec::{Decoder, Encoder};

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

// fn bench_vec(c: &mut Criterion) {
//     let mut group = c.benchmark_group("dispatch");
//
//     group.bench_function("smallvec", |b| {
//         b.iter(|| {
//             let _vec: SmallVec<[u8; 10]> = black_box(smallvec![0; 10]);
//         });
//     });
//
//     group.bench_function("vec", |b| {
//         b.iter(|| {
//             let _vec: Vec<u8> = black_box(vec![0; 10]);
//         });
//     });
//
//     group.finish();
// }

// bench_encode            time:   [22.754 ns 22.866 ns 22.980 ns]
fn bench_encode(c: &mut Criterion) {
    c.bench_function("bench_encode", |b| {
        b.iter_custom(|iters| {
            let mut buf = BytesMut::with_capacity(1024);

            let resp3 = Resp3::<Bytes, ByteString>::new_array(vec![
                Resp3::new_blob_string("GET".into()),
                Resp3::new_blob_string("key".into()),
            ]);

            let mut encoder = Resp3Encoder;

            let start = Instant::now();
            for _ in 0..iters {
                encoder.encode(&resp3, &mut buf).unwrap();
                buf.clear();
            }
            start.elapsed()
        })
    });
}

// bench_decode            time:   [200.58 ns 201.23 ns 201.93 ns]
fn bench_decode(c: &mut Criterion) {
    c.bench_function("bench_decode", |b| {
        b.iter_custom(|iters| {
            let mut buf = BytesMut::with_capacity(1024);

            let resp3 = Resp3::<Bytes, ByteString>::new_array(vec![
                Resp3::new_blob_string("GET".into()),
                Resp3::new_blob_string("key".into()),
            ]);

            let mut encoder = Resp3Encoder;
            encoder.encode(&resp3, &mut buf).unwrap();

            let mut decoder = Resp3Decoder::default();

            let start = Instant::now();
            for _ in 0..iters {
                decoder.decode(&mut buf.clone()).unwrap();
            }
            start.elapsed()
        })
    });
}

// bench_create_handler_cx    time:   [109.51 ns 111.18 ns 112.79 ns]
fn bench_create_handler_cx(c: &mut Criterion) {
    c.bench_function("bench_create_handler_cx", |b| {
        b.iter_custom(|iters| {
            let shared = get_test_shared();

            let start = Instant::now();
            for _ in 0..iters {
                let mut cx = HandlerContext::new(&shared);
                cx.id = 1;
            }
            start.elapsed()
        })
    });
}

// bench_create_handler    time:   [323.06 ns 324.46 ns 325.89 ns]
fn bench_create_handler(c: &mut Criterion) {
    c.bench_function("bench_create_handler", |b| {
        b.iter_custom(|iters| {
            let shared = get_test_shared();

            let start = Instant::now();
            for _ in 0..iters {
                Handler::new_fake_with(shared.clone(), None, None);
            }
            start.elapsed()
        })
    });
}

// bench_get_cmd           time:   [308.95 ns 309.95 ns 311.01 ns]
fn bench_get_cmd(c: &mut Criterion) {
    c.bench_function("bench_get_cmd", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(rt).iter_custom(|iters| async move {
            let (mut handler, _client) = Handler::new_fake();
            handler
                .dispatch(gen_set_cmd(black_box("key"), black_box("value")))
                .await
                .unwrap()
                .unwrap();

            let start = Instant::now();
            for _ in 0..iters {
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

// bench_set_cmd           time:   [654.36 ns 658.75 ns 663.12 ns]
fn bench_set_cmd(c: &mut Criterion) {
    c.bench_function("bench_set_cmd", |b| {
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
            }
            start.elapsed()
        })
    });
}

criterion_group!(
    benches,
    bench_encode,
    bench_decode,
    bench_create_handler_cx,
    bench_create_handler,
    bench_get_cmd,
    bench_set_cmd,
);
criterion_main!(benches);
