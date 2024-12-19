import select
import socket

import bench_batch_throughput_and_latency_get
import bench_batch_throughput_and_latency_set
import bench_client_throughput_and_latency_get
import bench_client_throughput_and_latency_set


def send_ping_with_timeout(port, host="localhost", timeout=1):
    try:
        # 创建一个 TCP 套接字
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # 连接到 Redis 服务器
        sock.connect((host, port))

        # 将 PING 命令编码为 RESP 协议格式
        ping_command = b"*1\r\n$4\r\nPING\r\n"

        # 发送 PING 命令
        sock.sendall(ping_command)

        # 接收服务器的响应
        response = sock.recv(1024)
        if response != b"+PONG\r\n":
            print("非预期的响应:", response.decode())

    except (socket.timeout, TimeoutError) as e:
        print("超时：请确保在6378端口上运行 Redis 服务，在6379端口上运行 Rutin 服务")
    finally:
        # 确保套接字关闭
        sock.close()


send_ping_with_timeout(6378)
send_ping_with_timeout(6379)


bench_batch_throughput_and_latency_get.run_bench()
bench_batch_throughput_and_latency_set.run_bench()
bench_client_throughput_and_latency_get.run_bench()
bench_client_throughput_and_latency_set.run_bench()
