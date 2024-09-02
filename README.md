Ruin是使用rust构建的redis-like数据库。该项目仍处于早期阶段，但单机功能的框架已经大体确立（日后会追加多机功能，包括主从复制，哨兵机制等）。目前，数据库支持TLS连接，部分Key，String，List操作，Subcribe/Publish功能，以及RDB与AOF持久化操作。Ruin支持且仅支持RESP3协议。



## 1）IO和命令的执行（多线程+多协程，提高CPU利用率）

​	在Redis中使用了事件循环来处理多个连接，在单线程模式下，Redis无法利用多核优势，即使在多线程模式下，Redis也只是利用多线程提升IO性能，但命令仍然需要由主线程来执行。这样做虽然减少了同步以及上下文切换的开销（数据的操作只由主线程执行），但也导致CPU的利用率不足。在一般情况下，这是可以接受的，因为Redis是IO密集型应用并且大部分命令都不是计算昂贵的，所以Redis大部分时间是在等待读数据或等待写数据，执行命令的时间只占小部分。

​	为了让数据库在保持高效IO前提下，也能够充分利用CPU支持一些计算昂贵的操作，Ruin使用`tokio`（一个异步运行时，使用协程实现）来处理客户端的请求。具体地，`tokio`会根据CPU数量创建线程池，每个线程池都有自己的异步任务队列，由tokio进行管理和调度。当收到的客户端连接请求时，Ruin会创建一个异步任务来处理该连接（开销是非常轻量的），在等待IO时（或者需要长时间等待的命令，如BLPOP）时，线程会转去执行其它异步任务，不会被阻塞，当任务队列为空时，当前线程会窃取其它线程的任务，因此可以充分利用CPU



## 2）并发存取（读写锁+锁分片）

​	Redis通过主线程来执行命令，因此存储引擎不需要考虑并发问题，而Ruin使用多线程来执行命令，需要考虑并发安全问题。Ruin使用`DashMap`库的哈希表作为键值对的基本存储引擎，该哈希表使用读写锁保证并发安全。对于锁的争用的问题，`DashMap`采用锁的分片来减轻争用，默认的锁的分片为cpu的核心数目，只有当多个的CPU都在对同一片锁分区进行操作（至少一个操作是写操作）时才会引起锁争用



## 3）增加异步命令，避免客户端级别的阻塞

​	在Redis中，命令的执行是单线程的，因此存在着一些可能长时间阻塞服务端的命令，例如blmove，brpop等。执行这些命令会导致其它客户端无法得到及时的响应。在Ruin中，命令的执行是异步的（多线程+多协程），因此命令的执行永远不会阻塞服务端。不仅如此，Ruin还提供了新的异步命令，如NBKeys，NBLPop，这些异步命令**不仅可以避免服务端的阻塞，也可以做到避免客户端的阻塞**，当客户端发送请求后，可以立刻继续执行其它命令，不必等待返回的结果。当阻塞命令完成之后，服务端会主动将结果发送给客户端，如果此时客户端刚好在执行其它的命令，那么突如其来的返回结果可能造成用户的困惑，因此对于这些异步命令，Ruin都提供了redict参数将结果返回到指定的客户端，用户可以开启一个客户端专用于收集等待的结果。



## 4）持久化（RDB，AOF）

​	Ruin支持RDB，AOF以及RDB与AOF混合持久化。对于RDB持久化，Redis使用写时复制进行数据的”拷贝“，实际上数据在只有一份，Ruin使用`bytes`库的Bytes类型达到类似的效果，对象只有在编码后才会实际占用更多的内存。为了避免编码时占用大量内存并且保持一定的性能高效，Ruin使用一个256MB的缓冲区缓冲编码的数据然后再写入文件中。对于AOF持久化，Redis使用rewrite机制防止AOF无限膨胀，Ruin也实现了AOF rewrite（即RDB与AOF混合持久化）。注意：开启AOF后会降低写命令的的执行速度（Ruin和Redis都是如此）



## 5）事件机制

​	在Ruin中，每个键都可以被绑定任意数量的事件，目前事件有Track（用于实现客户端缓存），MayUpdate，IntentionLock（用于实现事务）三类。

​	例如某一连接处理`BLPop list1`请求时，如果`list1`为空，**线程不会轮询，而是向`list1`映射的object注册MayUpdate事件**（实际上是保存一个用于向当前连接通信的Sender），然后`.await`等待消息传来。当另一个客户端处理`BLPush list1 v1`时，会检查该键是否有update事件，如果没有，则什么都不做；如果有（即当前的情况），则遍历所有update事件的Sender（不同的Sender对应着不同的连接），发送该键（即发送`list1`），然后清空事件。处理`BLPop list1`的连接收到键后，再次查看数据库中`list1`，返回弹出的值。



## 6）事务（Lua脚本）

​	目前，Ruin通过Lua脚本支持事务，满足Isolation和Consistency，但不保证Atomicity和Durability。用户可以通过SCRIPT REGISTER命令注册一个带有名称的脚本， 然后通过EVALNAME命令执行该脚本，也可以直接使用EVAL命令执行临时脚本。

​	事务的Isolation主要通过事件机制的IntentionLock事件实现。在Ruin中，每个处理命令的Handler都有其唯一的ID，而Ruin中的每个Lua环境（Ruin使用thread local存储Lua环境）都拥有自己的Handler。当执行脚本时，首先会对事务中涉及的键值对设置IntentionLock事件，IntentionLock事件包含一个target_id，只有ID符合要求的handler，才能修改该键值对（但允许访问），其余handler会在**获取写锁后马上释放**，并等待允许再次获取写锁的通知（等待是异步的，因此不会阻塞线程），每个键值对可能有多个等待的handler，这些**等待的handler都是按序的，因此对于单个键值对的命令也会按序执行**。当事务执行完毕后，会分别向涉及的每个键值对中首个等待的handler，发送通知，允许其获取写锁，当该handler使用完写锁后，会继续通知下一个handler，以此类推，直到最后一个handler获取写锁后，由它负责释放IntentionLock事件。在此期间，任何希望想要获取写锁的handler仍然需要按序等待通知（即使事务已经结束了），如果其中有某个handler再次设置了IntentionLock事件，则会修改事件的target_id，然后按同样的方式继续



## 7）访问控制（ACL, Access Control List）

​	Redis提供ACL控制命令执行以及键的访问。Ruin也提供了相似的支持，且配置的可读性更高。每个连接在初始化时，都会拥有自己的***Access Control***。所有连接初始化时都会设置为**default_ac**用户，如果没有配置ACL则表示禁用ACL（禁用后无法增删用户，可以选择置空），如果设置了ACL则可以通过AUTH命令认证使用该账户。当使用脚本执行命令时，脚本的权限和用户名与连接一致。

​	目前支持的访问控制包括：允许命令，禁用命令，允许一类命令，禁用一类命令，允许键的读操作（使用正则），允许键的写操作（使用正则），允许使用的pubsub（使用正则）



## 8）OOM处理

​	在Redis中，用户可以选择不同的OOM处理策略（`AllKeysLRU`，`AllKeysLFU`，`AllKeysRandom`，`NoEviction`等），Ruin也提供了相同的支持，具体做法为：Rutin中每个键都有20bit用于记录最新的`access time`，有12bit用于记录最新的`access count`，每次访问键，都会更新access time（通过一个全局的`LRU_CLOCK`更新，该时钟每一分钟增1），并有$\frac{1} {count / 2}$的概率access count增1。Rutin每300毫秒会更新一次`USED_MEMORY`，每次企图修改Object（获取写锁）时，都会检查是否OOM，如果是，（以AllKeysLRU为例）则随机选择`maxmemory_samples`个键，并挑出access time最旧的一个键，淘汰掉。如此往复，直到有了新的可用内存（**Tip**：该检查只保证修改之前必须有可用的内存，但不保证修改后的使用内存不超出`maxmemory`）



# Benchmark

### [Benchmark: GET command, varying batch size, measuring throughput and latency](https://github.com/sqfzy/rutin/blob/main/benches/bench_batch_throughput&latency_get.svg)

Batch Size: 1:  
	Redis Throughput (Kops/sec): 100.20039999999999, Redis Latency (msec): 0.399  
	Rutin Throughput (Kops/sec): 102.66941, Rutin Latency (msec): 0.399  
Batch Size: 4:  
	Redis Throughput (Kops/sec): 375.93984, Redis Latency (msec): 0.439  
	Rutin Throughput (Kops/sec): 412.79672, Rutin Latency (msec): 0.391  
Batch Size: 16:  
	Redis Throughput (Kops/sec): 1560.9756200000002, Redis Latency (msec): 0.407  
	Rutin Throughput (Kops/sec): 1445.34775, Rutin Latency (msec): 0.463  
Batch Size: 64:  
	Redis Throughput (Kops/sec): 3429.79625, Redis Latency (msec): 0.815  
	Rutin Throughput (Kops/sec): 3450.13475, Rutin Latency (msec): 0.767  
Batch Size: 256:  
	Redis Throughput (Kops/sec): 4056.84725, Redis Latency (msec): 3.015  
	Rutin Throughput (Kops/sec): 5691.5925, Rutin Latency (msec): 1.871  
Batch Size: 1024:  
	Redis Throughput (Kops/sec): 4370.797, Redis Latency (msec): 7.503  
	Rutin Throughput (Kops/sec): 6472.7405, Rutin Latency (msec): 6.951  



### [Benchmark: SET command, varying batch size, measuring throughput and latency](https://github.com/sqfzy/rutin/blob/main/benches/bench_batch_throughput&latency_set.svg)

Batch Size: 1:  
	Redis Throughput (Kops/sec): 97.65624000000001, Redis Latency (msec): 0.415  
	Rutin Throughput (Kops/sec): 98.81423, Rutin Latency (msec): 0.415  
Batch Size: 4:  
	Redis Throughput (Kops/sec): 394.86672, Redis Latency (msec): 0.407  
	Rutin Throughput (Kops/sec): 382.77512, Rutin Latency (msec): 0.423  
Batch Size: 16:  
	Redis Throughput (Kops/sec): 1376.9363799999999, Redis Latency (msec): 0.463  
	Rutin Throughput (Kops/sec): 1249.02412, Rutin Latency (msec): 0.543  
Batch Size: 64:  
	Redis Throughput (Kops/sec): 2212.2365, Redis Latency (msec): 1.263  
	Rutin Throughput (Kops/sec): 2910.414, Rutin Latency (msec): 0.911  
Batch Size: 256:  
	Redis Throughput (Kops/sec): 2896.0695, Redis Latency (msec): 4.207  
	Rutin Throughput (Kops/sec): 5268.7715, Rutin Latency (msec): 1.887  
Batch Size: 1024:  
	Redis Throughput (Kops/sec): 2991.78425, Redis Latency (msec): 8.119  
	Rutin Throughput (Kops/sec): 6782.2205, Rutin Latency (msec): 5.743  

### [Benchmark: GET command, varying client sessions, measuring throughput and latency](https://github.com/sqfzy/rutin/blob/main/benches/bench_client_throughput&latency_get.svg)

Client Sessions: 1:  
	Redis Throughput (Kops/sec): 7.75795, Redis Latency (msec): 0.119  
	Rutin Throughput (Kops/sec): 8.285, Rutin Latency (msec): 0.111  
Client Sessions: 2:  
	Redis Throughput (Kops/sec): 12.82051, Redis Latency (msec): 0.143  
	Rutin Throughput (Kops/sec): 11.19821, Rutin Latency (msec): 0.167  
Client Sessions: 4:  
	Redis Throughput (Kops/sec): 23.64066, Redis Latency (msec): 0.159  
	Rutin Throughput (Kops/sec): 19.92032, Rutin Latency (msec): 0.191  
Client Sessions: 8:  
	Redis Throughput (Kops/sec): 39.525690000000004, Redis Latency (msec): 0.183  
	Rutin Throughput (Kops/sec): 36.36364, Rutin Latency (msec): 0.207  
Client Sessions: 16:  
	Redis Throughput (Kops/sec): 61.72839, Redis Latency (msec): 0.231  
	Rutin Throughput (Kops/sec): 59.1716, Rutin Latency (msec): 0.239  
Client Sessions: 32:  
	Redis Throughput (Kops/sec): 84.74577000000001, Redis Latency (msec): 0.311  
	Rutin Throughput (Kops/sec): 82.64461999999999, Rutin Latency (msec): 0.327  
Client Sessions: 64:  
	Redis Throughput (Kops/sec): 120.48192999999999, Redis Latency (msec): 0.391  
	Rutin Throughput (Kops/sec): 109.89011, Rutin Latency (msec): 0.471  
Client Sessions: 128:  
	Redis Throughput (Kops/sec): 123.45679, Redis Latency (msec): 0.759  
	Rutin Throughput (Kops/sec): 136.9863, Rutin Latency (msec): 0.687  

### [Benchmark: SET command, varying client sessions, measuring throughput and latency](https://github.com/sqfzy/rutin/blob/main/benches/bench_client_throughput&latency_set.svg)

Client Sessions: 1:  
	Redis Throughput (Kops/sec): 7.363770000000001, Redis Latency (msec): 0.119  
	Rutin Throughput (Kops/sec): 6.7750699999999995, Rutin Latency (msec): 0.135  
Client Sessions: 2:  
	Redis Throughput (Kops/sec): 11.24859, Redis Latency (msec): 0.175  
	Rutin Throughput (Kops/sec): 11.91895, Rutin Latency (msec): 0.159  
Client Sessions: 4:  
	Redis Throughput (Kops/sec): 19.23077, Redis Latency (msec): 0.199  
	Rutin Throughput (Kops/sec): 20.53388, Rutin Latency (msec): 0.191  
Client Sessions: 8:  
	Redis Throughput (Kops/sec): 35.71429, Redis Latency (msec): 0.207  
	Rutin Throughput (Kops/sec): 36.10108, Rutin Latency (msec): 0.207  
Client Sessions: 16:  
	Redis Throughput (Kops/sec): 56.17977, Redis Latency (msec): 0.247  
	Rutin Throughput (Kops/sec): 57.14286, Rutin Latency (msec): 0.247  
Client Sessions: 32:  
	Redis Throughput (Kops/sec): 88.49558, Redis Latency (msec): 0.295  
	Rutin Throughput (Kops/sec): 83.33333999999999, Rutin Latency (msec): 0.327  
Client Sessions: 64:  
	Redis Throughput (Kops/sec): 117.64705000000001, Redis Latency (msec): 0.423  
	Rutin Throughput (Kops/sec): 112.35955, Rutin Latency (msec): 0.423  
Client Sessions: 128:  
	Redis Throughput (Kops/sec): 147.05881, Redis Latency (msec): 0.631  
	Rutin Throughput (Kops/sec): 128.20512, Rutin Latency (msec): 0.799  

# TODO

- [ ] 完善五个基本类型的命令
- [ ] 支持WASM脚本
- [ ] 支持JSON，protobuf协议
- [ ] 实现集群

