# run_benchmark.py
import re
import subprocess


def run_benchmark(command):
    # 打开管道，将输出捕获到变量中
    with subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, text=True
    ) as proc:
        output = proc.stdout.read()
    return output


def parse_throughput(output):
    match = re.search(r"(\d+\.\d+) requests per second", output)
    if match:
        return float(match.group(1))
    return None


def parse_latency(output):
    match = re.search(r"p50=(\d+\.\d+) msec", output)
    if match:
        return float(match.group(1))
    return None
