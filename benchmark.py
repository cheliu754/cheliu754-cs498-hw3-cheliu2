import statistics
import time
import uuid
from typing import Dict, List

import httpx

BASE_URL = "http://34.41.114.73"
FAST_ENDPOINT = f"{BASE_URL}/insert-fast"
SAFE_ENDPOINT = f"{BASE_URL}/insert-safe"
NUM_REQUESTS = 50
TIMEOUT_SECONDS = 30.0


def build_payload(make: str, idx: int) -> Dict:
    return {
        "VIN (1-10)": f"TEST{idx:06d}",
        "County": "TestCounty",
        "City": "TestCity",
        "State": "WA",
        "Postal Code": "00000",
        "Model Year": 2024,
        "Make": make,
        "Model": f"Model-{idx}",
        "Electric Vehicle Type": "Battery Electric Vehicle (BEV)",
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "Unknown",
        "Electric Range": 100,
        "Legislative District": 0,
        "DOL Vehicle ID": str(uuid.uuid4()),
        "Vehicle Location": "POINT (-122.0000 47.0000)",
        "Electric Utility": "TEST UTILITY",
        "2020 Census Tract": "00000000000",
    }


def benchmark_insert(client: httpx.Client, endpoint: str, label: str) -> List[float]:
    latencies_ms: List[float] = []

    for i in range(NUM_REQUESTS):
        payload = build_payload(label, i)

        start = time.perf_counter()
        response = client.post(endpoint, json=payload)
        end = time.perf_counter()

        response.raise_for_status()
        latency_ms = (end - start) * 1000
        latencies_ms.append(latency_ms)

    return latencies_ms


def summarize(name: str, values: List[float]) -> None:
    avg_ms = statistics.mean(values)
    min_ms = min(values)
    max_ms = max(values)
    median_ms = statistics.median(values)

    print(f"\n{name}")
    print(f"Requests: {len(values)}")
    print(f"Average latency: {avg_ms:.2f} ms")
    print(f"Median latency:  {median_ms:.2f} ms")
    print(f"Min latency:     {min_ms:.2f} ms")
    print(f"Max latency:     {max_ms:.2f} ms")


def main() -> None:
    with httpx.Client(timeout=TIMEOUT_SECONDS) as client:
        health_response = client.get(f"{BASE_URL}/health")
        health_response.raise_for_status()

        fast_latencies = benchmark_insert(client, FAST_ENDPOINT, "FAST")
        safe_latencies = benchmark_insert(client, SAFE_ENDPOINT, "SAFE")

    summarize("insert-fast", fast_latencies)
    summarize("insert-safe", safe_latencies)

    fast_avg = statistics.mean(fast_latencies)
    safe_avg = statistics.mean(safe_latencies)

    print("\nComparison")
    print(f"insert-fast average: {fast_avg:.2f} ms")
    print(f"insert-safe average: {safe_avg:.2f} ms")

    if safe_avg > 0:
        print(f"slowdown ratio (safe / fast): {safe_avg / fast_avg:.2f}x")


if __name__ == "__main__":
    main()