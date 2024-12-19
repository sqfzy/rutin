import datetime

import plotly.graph_objects as go
from plotly.subplots import make_subplots
from run_benchmark import parse_latency, parse_throughput, run_benchmark


def run_bench():
    batch_size_to_num_requests = {
        1: 100000,
        4: 400000,
        16: 1600000,
        64: 6400000,
        256: 10000000,
        1024: 20000000,
        # 4096: 20000000,
    }

    redis_throughput_results = []
    rutin_throughput_results = []

    redis_latency_results = []
    rutin_latency_results = []

    # 初始化数据
    run_benchmark("redis-cli -p 6378 set key:__rand_int__ foo")
    run_benchmark("redis-cli -p 6379 set key:__rand_int__ foo")

    print(
        "Benchmark: GET command, varying batch size, measuring throughput and latency"
    )

    data = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(f"./result/result_{data}.txt", "a+") as f:
        f.write(
            "\nBenchmark: GET command, varying batch size, measuring throughput and latency\n"
        )

    for batch_size, num_requests in batch_size_to_num_requests.items():
        # Run benchmark on Redis (port 6378)
        command_redis = (
            f"redis-benchmark -p 6378 -t get -n {num_requests} -P {batch_size} -q"
        )
        redis_output = run_benchmark(command_redis)

        redis_throughput = parse_throughput(redis_output)
        redis_throughput_kops = redis_throughput / 1000 if redis_throughput else 0
        redis_latency = parse_latency(redis_output)

        redis_throughput_results.append(redis_throughput_kops)
        redis_latency_results.append(redis_latency)

        # Run benchmark on Rutin (port 6379)
        command_rutin = (
            f"redis-benchmark -p 6379 -t get -n {num_requests} -P {batch_size} -q"
        )
        rutin_output = run_benchmark(command_rutin)

        rutin_throughput = parse_throughput(rutin_output)
        rutin_throughput_kops = rutin_throughput / 1000 if rutin_throughput else 0
        rutin_latency = parse_latency(rutin_output)

        rutin_throughput_results.append(rutin_throughput_kops)
        rutin_latency_results.append(rutin_latency)

        result = (
            f"Batch Size: {batch_size}:\n"
            f"\tRedis Throughput (Kops/sec): {redis_throughput_kops}, Redis Latency (msec): {redis_latency}\n"
            f"\tRutin Throughput (Kops/sec): {rutin_throughput_kops}, Rutin Latency (msec): {rutin_latency}"
        )

        with open(f"result.txt", "a+") as f:
            f.write(result)
            f.write("\n")

        print(result)

    # Create the subplots
    fig = make_subplots(
        rows=1,
        cols=2,
        shared_xaxes=True,
        vertical_spacing=0.15,
        subplot_titles=(
            "GET Command Throughput vs. Batch Size",
            "GET Command Latency vs. Batch Size",
        ),
    )

    # Add throughput traces
    fig.add_trace(
        go.Bar(
            x=[str(batch_size) for batch_size in batch_size_to_num_requests.keys()],
            y=redis_throughput_results,
            name="Redis Throughput",
            marker=dict(color="red"),
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=[str(batch_size) for batch_size in batch_size_to_num_requests.keys()],
            y=rutin_throughput_results,
            name="Rutin Throughput",
            marker=dict(color="blue"),
        ),
        row=1,
        col=1,
    )

    # Add latency traces
    fig.add_trace(
        go.Bar(
            x=[str(batch_size) for batch_size in batch_size_to_num_requests.keys()],
            y=redis_latency_results,
            name="Redis Latency",
            marker=dict(color="pink"),
        ),
        row=1,
        col=2,
    )

    fig.add_trace(
        go.Bar(
            x=[str(batch_size) for batch_size in batch_size_to_num_requests.keys()],
            y=rutin_latency_results,
            name="Rutin Latency",
            marker=dict(color="#66ccff"),
        ),
        row=1,
        col=2,
    )

    fig.update_layout(
        xaxis=dict(
            title="Batch Size", title_font=dict(size=15), tickfont=dict(size=12)
        ),
        yaxis=dict(
            title="Throughput (Kops/sec)",
            title_font=dict(size=15),
            tickfont=dict(size=12),
        ),
        xaxis2=dict(
            title="Batch Size", title_font=dict(size=15), tickfont=dict(size=12)
        ),
        yaxis2=dict(
            title="Latency (msec)", title_font=dict(size=15), tickfont=dict(size=12)
        ),
        barmode="group",
    )

    fig.write_html(
        "result/html/bench_batch_throughput&latency_get.html",
    )
    fig.write_image(
        "result/svg/bench_batch_throughput&latency_get.svg",
        width=1400,
    )
