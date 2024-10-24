Ruin是使用rust重构的redis-like数据库，实现了Redis的大部分功能(一些基本类型的命令还未完善，暂不支持集群)

Rutin的实现细节见https://github.com/sqfzy/rutin/blob/main/doc/rutin_implementation_details.md

Rutin中涉及性能的主要技术(概念)见https://sqfzy.notion.site/Rutin-6b0b958a50924ab4b5f7267766aa608c

Rutin涉及安全的主要技术(概念)见https://sqfzy.notion.site/Rutin-51c402d6f76545eeb6b957262772be72

# [Benchmark](https://github.com/sqfzy/rutin/blob/main/benches/compare_redis/result.txt)

OS: Arch Linux on Windows 10 x86_64  
Kernel: 6.6.36.3-microsoft-standard-WSL2  
CPU: 12th Gen Intel i7-12700H (20) @ 2.688GHz  
GPU: 982f:00:00.0 Microsoft Corporation Basic Render Driver

### Benchmark: GET command, varying batch size, measuring throughput and latency

![bench_batch_throughput&latency_get](https://github.com/sqfzy/rutin/blob/main/benches/compare_redis/result/svg/bench_batch_throughput%26latency_get.svg)

### Benchmark: SET command, varying batch size, measuring throughput and latency

![bench_batch_throughput&latency_set](https://github.com/sqfzy/rutin/blob/main/benches/compare_redis/result/svg/bench_batch_throughput&latency_set.svg)

### Benchmark: GET command, varying client sessions, measuring throughput and latency

![bench_client_throughput&latency_get](https://github.com/sqfzy/rutin/blob/main/benches/compare_redis/result/svg/bench_client_throughput&latency_get.svg)

### Benchmark: SET command, varying client sessions, measuring throughput and latency

![bench_client_throughput&latency_set](https://github.com/sqfzy/rutin/blob/main/benches/compare_redis/result/svg/bench_client_throughput&latency_set.svg)

# TODO

- [ ] 完善五个基本类型的命令
- [ ] 支持WASM脚本
- [ ] 支持JSON，protobuf协议
- [ ] 实现集群
