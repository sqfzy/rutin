import subprocess
import re
import pandas as pd


def run_benchmark(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result.stdout


def parse_throughput(output):
    match = re.search(r"(\d+\.\d+) requests per second", output)
    if match:
        return float(match.group(1))
    return None


def parse_latency(output):
    match = re.search(r"(\d+\.\d+) milliseconds", output)
    if match:
        return float(match.group(1))
    return None


# Test 1: GET command, varying client sessions
client_sessions = [1, 2, 4, 8, 16, 32, 64, 128]
throughput_results = []

for clients in client_sessions:
    command = f"redis-benchmark -t get -c {clients} -n 10000"
    output = run_benchmark(command)
    throughput = parse_throughput(output)
    throughput_results.append((clients, throughput))

# Convert results to DataFrame and display
df_throughput_clients = pd.DataFrame(
    throughput_results, columns=["Client Sessions", "Throughput"]
)
print("Test 1: GET command, varying client sessions")
print(df_throughput_clients)

# Test 2: GET command, varying batch size
batch_sizes = [1, 4, 16, 64, 256, 1024, 4096]
throughput_results = []

for batch_size in batch_sizes:
    command = f"redis-benchmark -t get -q -n 10000 -P {batch_size}"
    output = run_benchmark(command)
    throughput = parse_throughput(output)
    throughput_results.append((batch_size, throughput))

# Convert results to DataFrame and display
df_throughput_batch = pd.DataFrame(
    throughput_results, columns=["Batch Size", "Throughput"]
)
print("Test 2: GET command, varying batch size")
print(df_throughput_batch)

# Test 3: GET command, varying client sessions, measuring latency
latency_results = []

for clients in client_sessions:
    command = f"redis-benchmark -t get -c {clients} -n 10000 --latency"
    output = run_benchmark(command)
    latency = parse_latency(output)
    latency_results.append((clients, latency))

# Convert results to DataFrame and display
df_latency_clients = pd.DataFrame(
    latency_results, columns=["Client Sessions", "Latency"]
)
print("Test 3: GET command, varying client sessions, measuring latency")
print(df_latency_clients)

# Test 4: SET command, varying client sessions, measuring latency
latency_results = []

for clients in client_sessions:
    command = f"redis-benchmark -t set -c {clients} -n 10000 --latency"
    output = run_benchmark(command)
    latency = parse_latency(output)
    latency_results.append((clients, latency))

# Convert results to DataFrame and display
df_latency_set_clients = pd.DataFrame(
    latency_results, columns=["Client Sessions", "Latency"]
)
print("Test 4: SET command, varying client sessions, measuring latency")
print(df_latency_set_clients)
