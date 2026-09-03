#!/usr/bin/env python3
"""
Autonomous High-Concurrency Benchmark Harness
==============================================
Simulates 500 concurrent worker threads ramping up over 10 seconds against
http://localhost:8000/score.
Measures request throughput, error rate, and p50, p90, p95, p99 latency.
"""

import os
import sys
import time
import json
import random
import uuid
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

TARGET_URL = os.getenv("TARGET_URL", "http://127.0.0.1:8000/score")

NUM_THREADS = int(os.getenv("BENCHMARK_THREADS", "500"))
RAMP_UP_SECONDS = float(os.getenv("RAMP_UP_SECONDS", "10.0"))
TOTAL_REQUESTS = int(os.getenv("TOTAL_REQUESTS", "2500"))


def send_score_request(thread_idx: int) -> dict:
    # Staggered ramp-up delay
    if RAMP_UP_SECONDS > 0:
        delay = (thread_idx / NUM_THREADS) * RAMP_UP_SECONDS
        time.sleep(delay)

    txn_id = f"txn_{uuid.uuid4()}"
    user_id = f"user_{random.randint(1, 1000)}"
    amount = round(random.uniform(15.0, 3500.0), 2)

    payload = json.dumps({
        "transaction_id": txn_id,
        "user_id": user_id,
        "amount": amount
    }).encode("utf-8")

    req = urllib.request.Request(
        TARGET_URL,
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST"
    )

    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            latency_ms = (time.time() - t0) * 1000.0
            status_code = resp.getcode()
            body = json.loads(resp.read().decode("utf-8"))
            return {
                "success": status_code == 200,
                "latency_ms": latency_ms,
                "status_code": status_code,
                "decision": body.get("decision", "UNKNOWN")
            }
    except urllib.error.HTTPError as e:
        latency_ms = (time.time() - t0) * 1000.0
        return {"success": False, "latency_ms": latency_ms, "status_code": e.code, "decision": "ERROR"}
    except Exception as e:
        latency_ms = (time.time() - t0) * 1000.0
        return {"success": False, "latency_ms": latency_ms, "status_code": 0, "decision": "EXCEPTION"}


def run_benchmark():
    print(f"Target: {TARGET_URL}")
    print(f"Workers: {NUM_THREADS} threads | Ramp-up: {RAMP_UP_SECONDS}s | Total: {TOTAL_REQUESTS} requests")
    print("Beginning load test run...")

    start_bench = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(send_score_request, i % NUM_THREADS) for i in range(TOTAL_REQUESTS)]
        for f in as_completed(futures):
            results.append(f.result())

    total_wall_time = time.time() - start_bench
    successful = [r for r in results if r["success"]]
    latencies = sorted([r["latency_ms"] for r in successful])

    if not latencies:
        print("\n[!] No successful responses received. Is the Django API running at", TARGET_URL, "?")
        return

    n = len(latencies)
    p50 = latencies[int(n * 0.50)]
    p90 = latencies[int(n * 0.90)]
    p95 = latencies[int(n * 0.95)]
    p99 = latencies[int(n * 0.99)]

    throughput = len(results) / total_wall_time

    print("\n" + "=" * 55)
    print("         BENCHMARK EXECUTION SUMMARY")
    print("=" * 55)
    print(f"Total Requests:     {len(results)}")
    print(f"Successful (200 OK):{len(successful)} ({len(successful)/len(results)*100:.1f}%)")
    print(f"Throughput:         {throughput:.1f} req/sec")
    print(f"Wall Clock Time:    {total_wall_time:.2f}s")
    print("-" * 55)
    print(f"p50 Latency:        {p50:.2f} ms")
    print(f"p90 Latency:        {p90:.2f} ms")
    print(f"p95 Latency:        {p95:.2f} ms")
    print(f"p99 Latency:        {p99:.2f} ms")
    print("=" * 55)


if __name__ == "__main__":
    run_benchmark()
