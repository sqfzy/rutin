```
===============================================================================
 Language            Files        Lines         Code     Comments       Blanks
===============================================================================
 Dockerfile              1            8            7            0            1
 Python                  6          670          523           35          112
 SVG                     4            4            4            0            0
 Plain Text             35         1107            0         1092           15
 TOML                    9          517          253          215           49
-------------------------------------------------------------------------------
 HTML                    4           36           36            0            0
 |- JavaScript           4           36           32            4            0
 (Total)                             72           68            4            0
-------------------------------------------------------------------------------
 Markdown                3          180            0          114           66
 |- Shell                1            6            6            0            0
 (Total)                            186            6          114           66
-------------------------------------------------------------------------------
 Rust                   90        24182        18506         2403         3273
 |- Markdown            30         1203            5          901          297
 (Total)                          25385        18511         3304         3570
===============================================================================
 Total                 152        26704        19329         3859         3516
===============================================================================
```

Ruin是使用rust重构的redis-like数据库，已经实现的功能有：
1. 关于Key, String, Hash, List, PubSub的部分命令。
2. TLS和ACL(访问控制列表)
3. OOM(内存溢出)的淘汰策略以及定期淘汰
4. RDB和AOF持久化
5. Lua脚本(事务)
6. 主从复制功能
7. ORAM

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

# 代码结构
`rutin_dashmap`: 数据库存储的核心实现
`rutin_oram`: Swap ORAM的实现
`rutin_proc`: 一些辅助的宏
`rutin_resp`: Redis的RESP3协议的实现
`rutin_server`: 数据库的实现
 - `src/cmd`: 命令分发，以及各种命令的实现
 - `src/conf`: 数据库配置的实现
 - `src/persist`: 持久化的实现
 - `src/server`: 为连接分配资源，并接收请求
 - `src/shared`: 不同连接的资源共享以及通信的实现，例如数据库的核心存储
 - `src/util`: 一些辅助函数，涉及优化并发，主从复制等

总的来说，代码的架构如下；
![code_architecture](https://github.com/sqfzy/rutin/blob/main/assert/rutin_code.svg)

# ORAM
ORAM即Oblivious RAM，用于隐藏用户的访问模式。当我们使用远程存储服务时，仅仅通过加密数据还不足以完全保护我们的隐私，因为服务还可以通过观测我们的
访问模式来推断我们的隐私信息，例如，通过统计数据的访问频率来推测数据的重要程；通过观察操作类型，得知数据的值倾向于不变还是
改变等。
rutin提供了一个Swap ORAM(本人尚未发表的论文中提出的一种ORAM方案，是对Path ORAM的改进)的proxy实现，用户可以通过代理来实现`get`与`set`命令的茫然随机访问。

# TODO

- [ ] 完善五个基本类型的命令
- [ ] 支持JSON导入、导出数据
- [ ] 实现集群
