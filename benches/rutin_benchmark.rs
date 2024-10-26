// use std::time::Instant;
//
// use bytes::BytesMut;
// use criterion::{black_box, criterion_group, criterion_main, Criterion};
// use rutin::{
//     frame::{CheapResp3, ExpensiveResp3, Resp3, StaticResp3},
//     server::{Handler, HandlerContext},
//     shared::NULL_ID,
//     util::gen_test_shared,
// };
// use rutin_resp3::codec::{decode::Resp3Decoder, encode::Resp3Encoder};
// use tokio_util::codec::{Decoder, Encoder};
//
// fn gen_get_cmd(key: &'static str) -> CheapResp3 {
//     Resp3::new_array(vec![
//         Resp3::new_blob_string("GET"),
//         Resp3::new_blob_string(key),
//     ])
// }
//
// fn gen_set_cmd(key: &'static str, value: &'static str) -> CheapResp3 {
//     Resp3::new_array(vec![
//         Resp3::new_blob_string("SET"),
//         Resp3::new_blob_string(key),
//         Resp3::new_blob_string(value),
//     ])
// }

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

// // bench_encode            time:   [22.754 ns 22.866 ns 22.980 ns]
// fn bench_encode(c: &mut Criterion) {
//     c.bench_function("bench_encode", |b| {
//         b.iter_custom(|iters| {
//             let mut buf = BytesMut::with_capacity(1024);
//
//             let resp3 = StaticResp3::new_array(vec![
//                 StaticResp3::new_blob_string(leak_bytes(b"GET")),
//                 StaticResp3::new_blob_string(leak_bytes(b"key")),
//             ]);
//
//             let mut encoder = Resp3Encoder;
//
//             let start = Instant::now();
//             for _ in 0..iters {
//                 encoder.encode(&resp3, &mut buf).unwrap();
//                 buf.clear();
//             }
//             start.elapsed()
//         })
//     });
// }
//
// // bench_decode            time:   [200.58 ns 201.23 ns 201.93 ns]
// fn bench_decode(c: &mut Criterion) {
//     c.bench_function("bench_decode", |b| {
//         b.iter_custom(|iters| {
//             let mut buf = BytesMut::with_capacity(1024);
//
//             let resp3 = CheapResp3::new_array(vec![
//                 StaticResp3::new_blob_string("GET"),
//                 StaticResp3::new_blob_string("key"),
//             ]);
//
//             let mut encoder = Resp3Encoder;
//             encoder.encode(&resp3, &mut buf).unwrap();
//
//             let mut decoder = Resp3Decoder::default();
//
//             let start = Instant::now();
//             for _ in 0..iters {
//                 decoder.decode(&mut buf.clone()).unwrap();
//             }
//             start.elapsed()
//         })
//     });
// }
//
// // fn bench_decode2(c: &mut Criterion) {
// //     c.bench_function("bench_decode", |b| {
// //         b.iter_custom(|iters| {
// //             let mut buf = BytesMut::with_capacity(1024);
// //
// //             let resp3 = Resp3::<Bytes, ByteString>::new_array(vec![
// //                 Resp3::new_blob_string("GET".into()),
// //                 Resp3::new_blob_string("key".into()),
// //             ]);
// //
// //             let mut encoder = Resp3Encoder;
// //             encoder.encode(&resp3, &mut buf).unwrap();
// //
// //             let mut decoder = Resp3Decoder::default();
// //
// //             let start = Instant::now();
// //             for _ in 0..iters {
// //                 redis_protocol::resp3::decode::complete::decode(&mut buf.clone()).unwrap();
// //             }
// //             start.elapsed()
// //         })
// //     });
// // }
// //
// // fn bench_decode3(c: &mut Criterion) {
// //     c.bench_function("bench_decode", |b| {
// //         b.iter_custom(|iters| {
// //             let mut buf = BytesMut::with_capacity(1024);
// //
// //             let resp3 = Resp3::<Bytes, ByteString>::new_array(vec![
// //                 Resp3::new_blob_string("GET".into()),
// //                 Resp3::new_blob_string("key".into()),
// //             ]);
// //
// //             let mut encoder = Resp3Encoder;
// //             encoder.encode(&resp3, &mut buf).unwrap();
// //
// //             let mut decoder = Resp3Decoder::default();
// //
// //             let start = Instant::now();
// //             for _ in 0..iters {
// //                 redis_protocol::resp3::decode::complete::decode_range(&mut buf.clone()).unwrap();
// //             }
// //             start.elapsed()
// //         })
// //     });
// // }
//
// // // bench_create_handler_cx    time:   [109.51 ns 111.18 ns 112.79 ns]
// // fn bench_create_handler_cx(c: &mut Criterion) {
// //     c.bench_function("bench_create_handler_cx", |b| {
// //         b.iter_custom(|iters| {
// //             let shared = gen_test_shared();
// //             let post_office = shared.post_office();
// //
// //             let start = Instant::now();
// //             for _ in 0..iters {
// //                 let mailbox = post_office.register_special_mailbox(NULL_ID);
// //                 let mut cx = HandlerContext::new(shared, NULL_ID, mailbox);
// //                 cx.id = 1;
// //             }
// //             start.elapsed()
// //         })
// //     });
// // }
// //
// // // bench_create_handler    time:   [323.06 ns 324.46 ns 325.89 ns]
// // fn bench_create_handler(c: &mut Criterion) {
// //     c.bench_function("bench_create_handler", |b| {
// //         b.iter_custom(|iters| {
// //             let shared = gen_test_shared();
// //
// //             let start = Instant::now();
// //             for _ in 0..iters {
// //                 Handler::new_fake_with(shared, None, None);
// //             }
// //             start.elapsed()
// //         })
// //     });
// // }
//
// // bench_get_cmd           time:   [308.95 ns 309.95 ns 311.01 ns]
// fn bench_get_cmd(c: &mut Criterion) {
//     c.bench_function("bench_get_cmd", |b| {
//         let rt = tokio::runtime::Runtime::new().unwrap();
//
//         b.to_async(rt).iter_custom(|iters| async move {
//             let mut set_cmd = gen_set_cmd(black_box("key"), black_box("value"));
//             let mut get_cmd = gen_get_cmd(black_box("key"));
//
//             let (mut handler, _client) = Handler::new_fake();
//             handler.dispatch(set_cmd).await.unwrap().unwrap();
//
//             let start = Instant::now();
//             for _ in 0..iters {
//                 handler.dispatch(get_cmd).await.unwrap().unwrap();
//             }
//             start.elapsed()
//         })
//     });
// }
//
// // bench_set_cmd           time:   [654.36 ns 658.75 ns 663.12 ns]
// fn bench_set_cmd(c: &mut Criterion) {
//     c.bench_function("bench_set_cmd", |b| {
//         let rt = tokio::runtime::Runtime::new().unwrap();
//
//         b.to_async(rt).iter_custom(|iters| async move {
//             let mut set_cmd = gen_set_cmd(black_box("key"), black_box("value"));
//
//             let (mut handler, _client) = Handler::new_fake();
//
//             let start = Instant::now();
//             for _ in 0..iters {
//                 handler.dispatch(set_cmd).await.unwrap().unwrap();
//             }
//             start.elapsed()
//         })
//     });
// }

// criterion_group!(
//     benches,
//     // bench_encode,
//     // bench_decode,
//     // // bench_decode2,
//     // // bench_decode3,
//     // // bench_create_handler_cx,
//     // // bench_create_handler,
//     // bench_get_cmd,
//     // bench_set_cmd,
// );
// criterion_main!(benches);
