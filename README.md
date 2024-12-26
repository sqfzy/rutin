```
===============================================================================
 Language            Files        Lines         Code     Comments       Blanks
===============================================================================
 Markdown                3          148            0           83           65
 Python                  6          670          523           35          112
 SVG                     4            4            4            0            0
 Plain Text             35         1107            0         1092           15
 TOML                    9          499          247          205           47
-------------------------------------------------------------------------------
 HTML                    4           36           36            0            0
 |- JavaScript           4           36           32            4            0
 (Total)                             72           68            4            0
-------------------------------------------------------------------------------
 Rust                   88        24061        18424         2367         3270
 |- Markdown            30         1203            5          901          297
 (Total)                          25264        18429         3268         3567
===============================================================================
 Total                 149        26525        19234         3782         3509
===============================================================================
```

Ruin是使用rust重构的redis-like数据库，已经实现的功能有：
1. 关于Key, String, Hash, List, PubSub的部分命令。
2. TLS和ACL(访问控制列表)
3. OOM(内存溢出)的淘汰策略
4. RDB和AOF持久化
5. Lua脚本(事务)
6. 主从复制功能

# Install
## Cargo 
```shell
git clone https://github.com/sqfzy/rutin.git
cd rutin
cargo install --path . --bin rutin_server
```
## Docker
```shell
git clone https://github.com/sqfzy/rutin.git
cd rutin
docker build -t rutin_server .
```

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
- [ ] 支持JSON导入、导出数据
- [ ] 实现集群

# ORAM
ORAM即Oblivious RAM，用于隐藏用户的访问模式。当我们使用远程存储服务时，仅仅通过加密数据还不足以完全保护我们的隐私，因为服务还可以通过观测我们的
访问模式来推断我们的隐私信息，例如，通过统计数据的访问频率来推测数据的重要程；通过观察操作类型，得知数据的值倾向于不变还是
改变等。与传统的ORAM实现不同，`rutin_oram`使用本人提出的基于随机过程实现的ORAM方案。目前其理论与实现都只有雏形，日后会继续完善。
