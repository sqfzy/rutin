Rutin是使用rust构建的redis-like数据库。该项目仍处于早期阶段，但单机功能的框架已经大体确立（日后会追加多机功能，包括主从复制，哨兵机制等）。目前，数据库支持TLS连接，部分Key，String，List操作，Subcribe/Publish功能，以及RDB与AOF持久化操作。Rutin支持且仅支持RESP3协议。



## 1）IO（使用多线程+多协程，提高CPU利用率）

​	在Redis中使用了事件循环来处理多个连接，在单线程模式下，Redis无法利用多核优势，即使在多线程模式下，Redis也只是利用多线程提升IO性能，但命令仍然需要由主线程来执行。这样做虽然减少了同步以及上下文切换的开销（数据的操作只由主线程执行），但也导致CPU的利用率不足。在一般情况下，这是可以接受的，因为Redis是IO密集型应用并且大部分命令都不是计算昂贵的，所以Redis大部分时间是在等待读数据或等待写数据，执行命令的时间只占小部分。

​	为了让数据库在保持高效IO前提下，也能够充分利用CPU支持一些计算昂贵的操作，Rutin使用`tokio`（一个异步运行时，使用协程实现）来处理客户端的请求。具体地，`tokio`会根据CPU数量创建线程池，每个线程池都有自己的异步任务队列，由tokio进行管理和调度。当收到的客户端连接请求时，Rutin会创建一个异步任务来处理该连接（开销是非常轻量的），在等待IO时（或者需要长时间等待的命令，如BLPOP）时，线程会转去执行其它异步任务，不会被阻塞，当任务队列为空时，当前线程会窃取其它线程的任务，因此可以充分利用CPU



## 2）存储引擎（读写锁+锁分片提升并发性能）

​	Redis通过主线程来执行命令，因此存储引擎不需要考虑并发问题，而Rutin使用多线程来执行命令，需要考虑并发安全问题。Rutin使用`DashMap`库的哈希表作为键值对的基本存储引擎，该哈希表使用读写锁保证并发安全。对于锁的争用的问题，`DashMap`采用锁的分片来减轻争用，默认的锁的分片为cpu的核心数目，只有当多个的CPU都在对同一片锁分区进行操作（至少一个操作是写操作）时才会引起锁争用，因此`DashMap`对于随机的键的写并发也有着优异的性能表现（见Benchmark）



## 3）增加异步命令，避免客户端级别的阻塞

​	在Redis中，命令的执行是单线程的，因此存在着一些可能长时间阻塞服务端的命令，例如blmove，brpop等。执行这些命令会导致其它客户端无法得到及时的响应。在Rutin中，命令的执行是异步的（多线程+多协程），因此命令的执行永远不会阻塞服务端。不仅如此，rutin还提供了新的异步命令，如NBKeys，NBLPop，这些异步命令**不仅可以避免服务端的阻塞，也可以做到避免客户端的阻塞**，当客户端发送请求后，可以立刻继续执行其它命令，不必等待返回的结果。当阻塞命令完成之后，服务端会主动将结果发送给客户端，如果此时客户端刚好在执行其它的命令，那么突如其来的返回结果可能造成用户的困惑，因此对于这些异步命令，Rutin都提供了redict参数将结果返回到指定的客户端，用户可以开启一个客户端专用于收集等待的结果。



## 4）持久化（RDB，AOF）

​	Rutin支持RDB，AOF以及RDB与AOF混合持久化。对于RDB持久化，Redis使用写时复制进行数据的”拷贝“，实际上数据在只有一份，Rutin使用`bytes`库的Bytes类型达到类似的效果，对象只有在编码后才会实际占用更多的内存。为了避免编码时占用大量内存并且保持一定的性能高效，Rutin使用一个256MB的缓冲区缓冲编码的数据然后再写入文件中。对于AOF持久化，Redis使用rewrite机制防止AOF无限膨胀，Rutin也实现了AOF rewrite（即RDB与AOF混合持久化）。注意：开启AOF后会降低写命令的的执行速度（Rutin和Redis都是如此）



## 5）事件机制（几乎0成本的抽象）

​	在Rutin中，每个键都可以被绑定任意数量的事件，目前事件分为Track，MayUpdate，IntentionLock三类。

​	例如某一连接处理`BLPop list1`请求时，如果`list1`为空，**线程不会轮询，而是向`list1`映射的object注册MayUpdate事件**（实际上是保存一个用于向当前连接通信的Sender（这里使用的是`flume`库的通道，克隆Sender几乎没有开销，只是简单地增加引用计数）），然后`.await`等待消息传来。当另一个客户端处理`BLPush list1 v1`时，会检查该键是否有update事件，如果没有，则什么都不做；如果有（即当前的情况），则遍历所有Sender（不同的Sender对应着不同的连接）并发送该键（即发送`list1`），然后清空事件。处理`BLPop list1`的连接收到键后，再次查看数据库中`list1`，返回弹出的值。



## 6）事务（Lua脚本）

​	目前，rutin通过Lua脚本支持事务，满足Isolation和Consistency，但不保证Atomicity和Durability。用户可以通过SCRIPT REGISTER命令注册一个带有名称的脚本， 然后通过EVALNAME命令执行该脚本，也可以直接使用EVAL命令执行临时脚本。

​	事务的Isolation主要通过事件机制的IntentionLock事件实现。在rutin中，每个处理命令的Handler都有其唯一的ID，而rutin中的每个Lua环境（rutin会在运行时动态创建Lua环境）都拥有自己的Handler。当执行脚本时，首先会对事务中涉及的键值对设置IntentionLock事件，IntentionLock事件拥有一个target_id字段，只有ID符合要求的handler，才能修改该键值对（但允许访问），其余handler会在**获取写锁后马上释放**，并等待允许再次获取写锁的通知（等待是异步的，因此不会阻塞线程），每个键值对可能有多个等待的handler，这些**等待的handler都是按序的，因此对于单个键值对的命令也会按序执行**。当事务执行完毕后，会分别向每个键值对中首个等待的handler，发送通知，允许其获取写锁，当该handler使用完写锁后，会继续通知下一个handler，以此类推，直到最后一个handler获取写锁后，由它负责释放IntentionLock事件。在此期间，任何希望想要获取写锁的handler仍然需要按序等待通知（即使事务已经结束了），如果其中有某个handler再次设置了IntentionLock事件，则会修改事件的target_id，然后按同样的方式继续



## 7）访问控制（ACL, Access Control List）

​	Redis提供ACL控制命令执行以及键的访问。Rutin也提供了相似的支持，且配置的可读性更高。每个连接在初始化时，都会拥有自己的***Access Control***。所有连接初始化时都会设置为**default_ac**用户（可以设置disable禁用该用户），如果没有配置ACL则表示禁用ACL（禁用后无法增删用户，可以选择置空），如果设置了ACL则可以通过AUTH命令认证使用该账户。当使用脚本执行命令时，脚本会使用与连接相同的用户（用户名与权限都一致）。

​	目前支持的访问控制包括：允许命令，禁用命令，允许一类命令，禁用一类命令，允许键的读操作（使用正则），允许键的写操作（使用正则），允许使用的通道（使用正则）



# Benchmark

**以下测试中，Rutin没有开启AOF，Redis没有开启多线程和AOF。在单机功能完善之后，可能会更新测试结果**

| -r代表生成随机键的范围，-P代表一个Pipline的请求数量，-n代表请求的总数 | Rutin                                                        | Redis                                                        | 结论                                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| redis-benchmark -t get -r 1 -P 1 -n 100000                   | throughput summary: 103734.44 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        0.445     0.072     0.391     0.839     1.279     8.207 | throughput summary: 94339.62 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        0.485     0.096     0.439     0.895     1.239     3.231 | **性能差异不大**。get, 请求单个键，无pipline，性能差异的关键在于单个请求的处理速度。 |
| redis-benchmark -t get -r 1 -P 200 -n 1000000                | throughput summary: 7633588.00 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        0.979     0.384     0.943     1.431     1.679     1.999 | throughput summary: 3030303.00 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        2.843     0.832     2.831     3.887     4.383     4.727 | **Rutin性能约为Redis的3.38倍**。get, 请求单个键，每个pipline携带200个请求，相当于一次CPU耗时操作。 |
|                                                              |                                                              |                                                              |                                                              |
| redis-benchmark -t set -r 1 -P 1 -n 100000                   | throughput summary: 101729.40 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        0.456     0.064     0.415     0.847     1.151     3.583 | throughput summary: 109289.62 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        0.426     0.072     0.391     0.799     1.047     2.399 | **性能差异不大**。set, 请求单个键，无pipline。               |
| redis-benchmark -t set -r 1 -P 200 -n 1000000                | throughput summary: 2000000.00 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        4.736     0.992     4.759     6.935     7.855     8.439 | throughput summary: 2777778.00 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        3.215     0.648     3.303     4.319     4.695     4.983 | **Rutin性能约为Redis的0.719倍。**set，请求单个键。无pipline。由于写锁是互斥的，因此对单个键的多次请求会产生锁的争用，影响性能 |
| redis-benchmark -t set -r 10000 -P 1 -n 100000               | throughput summary: 100603.62 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        0.458     0.088     0.415     0.855     1.143     2.695 | throughput summary: 104166.67 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        0.447     0.072     0.399     0.847     1.143     2.927 | **性能差异不大**。set，随机请求键，无pipline。               |
| redis-benchmark -t set -r 10000 -P 200 -n 1000000            | throughput summary: 6024096.50 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        1.241     0.480     1.183     1.831     2.255     3.143 | throughput summary: 2159827.25 requests per second<br/>  latency summary (msec):<br/>          avg       min       p50       p95       p99       max<br/>        4.364     0.768     4.311     5.567     7.663    10.087 | **Rutin性能约为Redis的3倍。**set，请求多个键，每个pipline携带200个请求。由于减少了锁的争用，因此性能得以提升，最优情况下（完全没有锁的争用）应该与get命令的效率一致 |
|                                                              |                                                              |                                                              |                                                              |

结论：

1. 处理单个请求时，Rutin与Redis的效率相当。
2. 批处理多个随机键请求时，Rutin的效率明显优于Redis。
3. 批处理单个键的写请求时；Rutin的效率劣于Redis。





# TODO

- [ ] 完善五个基本类型的命令
- [ ] 支持数据溢出到磁盘
- [ ] 支持WASM脚本
- [ ] 支持JSON，protobuf协议
- [ ] 追加多机功能（主从复制，哨兵机制，选举主节点等）

