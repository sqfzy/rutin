Ruin是使用rust重构的redis-like数据库，实现了Redis的大部分功能(一些基本类型的命令还未完善，暂不支持集群)

Rutin的实现细节见https://github.com/sqfzy/rutin/blob/main/rutin_server/doc/rutin_implementation_details.md

Rutin中涉及性能的主要技术(概念)见https://sqfzy.notion.site/Rutin-6b0b958a50924ab4b5f7267766aa608c

Rutin涉及安全的主要技术(概念)见https://sqfzy.notion.site/Rutin-51c402d6f76545eeb6b957262772be72

# [Benchmark](https://github.com/sqfzy/rutin/blob/main/rutin_server/benches/compare_redis/result.txt)

OS: Arch Linux on Windows 10 x86_64  
Kernel: 6.6.36.3-microsoft-standard-WSL2  
CPU: 12th Gen Intel i7-12700H (20) @ 2.688GHz  
GPU: 982f:00:00.0 Microsoft Corporation Basic Render Driver

### Benchmark: GET command, varying batch size, measuring throughput and latency

![bench_batch_throughput&latency_get](https://github.com/sqfzy/rutin/blob/main/rutin_server/benches/compare_redis/result/svg/bench_batch_throughput%26latency_get.svg)

### Benchmark: SET command, varying batch size, measuring throughput and latency

![bench_batch_throughput&latency_set](https://github.com/sqfzy/rutin/blob/main/rutin_server/benches/compare_redis/result/svg/bench_batch_throughput&latency_set.svg)

### Benchmark: GET command, varying client sessions, measuring throughput and latency

![bench_client_throughput&latency_get](https://github.com/sqfzy/rutin/blob/main/rutin_server/benches/compare_redis/result/svg/bench_client_throughput&latency_get.svg)

### Benchmark: SET command, varying client sessions, measuring throughput and latency

![bench_client_throughput&latency_set](https://github.com/sqfzy/rutin/blob/main/rutin_server/benches/compare_redis/result/svg/bench_client_throughput&latency_set.svg)

# TODO

- [ ] 完善五个基本类型的命令
- [ ] 支持WASM脚本
- [ ] 支持JSON，protobuf协议
- [ ] 实现集群

# ORAM
ORAM即Oblivious RAM，用于隐藏用户的访问模式。当我们使用远程存储服务时，仅仅通过加密数据还不足以完全保护我们的隐私，因为服务还可以通过观测我们的
访问模式来推断我们的隐私信息，例如，通过统计数据的访问频率来推测数据的重要程；通过观察操作类型，得知数据的值倾向于不变还是
改变等。与传统的ORAM实现不同，`rutin_oram`使用本人提出的基于随机过程实现的ORAM方案。目前其理论与实现都只有雏形，日后会继续完善。
