# High-Performance Real-Time Fraud Detection Microservice

## Overview

This project is a production-oriented backend service for real-time transaction fraud scoring. It combines Django REST Framework, Gunicorn with cooperative `gevent` workers, Redis as a rolling feature store, PostgreSQL as an audit ledger, and TensorFlow for neural-network inference.

The serving path is designed to keep request work bounded and observable:

1. Validate the transaction payload.
2. Read the user's recent aggregate features from Redis.
3. Evaluate the preloaded fraud model.
4. Return an approval, flag, rejection, or manual-review decision.
5. Dispatch the audit record to a background PostgreSQL writer without waiting for the database round trip.

The repository also includes a Kafka and PySpark streaming ingestion path. Spark maintains rolling transaction aggregates and writes monotonic feature snapshots to Redis for low-latency serving.

## Problem Statement And Concurrency Challenge

The initial implementation behaved correctly at low request volume but exhibited severe latency spikes under concurrent load. A 500-thread client benchmark produced latency outliers above 2 seconds and an observed p50 near 800 ms in the original high-contention configuration.

The bottleneck was not a single slow dependency. Several costs compounded under concurrency:

- TensorFlow execution was reached through a model wrapper and could incur first-call graph work on a request thread.
- Dynamic TensorFlow invocation made retracing and wrapper overhead possible.
- TensorFlow's default native thread pools multiplied the number of runnable threads across Gunicorn workers.
- A synchronous native C++ inference call could hold a `gevent` worker's execution path and starve other greenlets.
- A request could wait on database persistence even though the audit record was not needed to produce the scoring decision.
- A client-side benchmark with too many Python threads could measure load-generator contention instead of service latency.

The refactor separates one-time model preparation from request-time inference, constrains CPU parallelism, isolates blocking native execution, and makes the benchmark capable of both safe local tests and controlled burst tests.

## System Architecture

```mermaid
flowchart LR
    K[Kafka raw-transactions] --> S[PySpark Structured Streaming]
    S -->|rolling HSET features| R[(Redis feature store)]

    C[Client / hey / wrk / benchmark_load.py] --> G[Gunicorn gevent workers]
    G --> V[Django scoring endpoint]
    V -->|pooled HGETALL| R
    V -->|fixed [None, 3] tensor| T[Warm TensorFlow infer_fn]
    T --> V
    V -->|background dispatch| Q[gevent ThreadPool]
    Q --> P[(PostgreSQL audit ledger)]
    V --> M[Prometheus metrics and JSON timing logs]
```

### Ingress

The Django service is run by Gunicorn with four cooperative `gevent` workers and up to 1,000 worker connections per process. The container entrypoint is:

```bash
gunicorn fraud_service.wsgi:application \
  --bind 0.0.0.0:8000 \
  --worker-class gevent \
  --workers 4 \
  --worker-connections 1000 \
  --timeout 60
```

### State And Caching

Redis stores rolling per-user features such as transaction velocity, recent aggregate amount, and the timestamp of the latest update. Django uses a shared Redis connection pool with `max_connections=300`, one-second socket and connect timeouts, and a stateful circuit breaker. When Redis is unavailable or features are too stale, the endpoint routes the transaction to `MANUAL_REVIEW` instead of silently approving it.

### Inference Engine

Each Django worker loads the Keras model once in `ScoringConfig.ready()`. The model is called through a concrete TensorFlow function with a fixed input signature:

```python
@tf.function(
    input_signature=[
        tf.TensorSpec(shape=[None, 3], dtype=tf.float32)
    ]
)
def infer_fn(input_tensor):
    return self.model(input_tensor, training=False)
```

The function is warmed with `tf.zeros([1, 3], dtype=tf.float32)` before the worker accepts traffic. Requests construct a `tf.constant` containing `[amount, txn_count_15m, staleness_seconds]` and invoke the compiled function directly.

### Persistence

Audit writes are submitted to an isolated background executor. The request thread records the dispatch duration and returns the scoring response without waiting for `ScoredTransaction.objects.create()` to complete. A unique `transaction_id` constraint prevents duplicate ledger rows when concurrent retries enqueue the same transaction.

## Engineering Deep-Dive: Root Cause Analysis And Solutions

### Eliminating Wrapper Overhead And Retracing

The original path called a high-level model wrapper for each request. The final path loads the Keras model once, creates a strict `@tf.function` with an input shape of `[None, 3]`, and performs a boot-time warm-up call. This gives TensorFlow a stable graph contract and moves tracing and initial compilation away from user traffic.

The request path obtains the Django app configuration and calls `infer_fn` directly:

```python
app_config = django_apps.get_app_config("scoring")
input_tensor = tf.constant(
    [[amount, txn_count_15m, staleness_seconds]],
    dtype=tf.float32,
)
prediction = app_config.infer_fn(input_tensor)
probability = float(prediction[0][0].numpy())
```

The repository retains a deterministic heuristic fallback for environments where TensorFlow is not installed, such as lightweight local test environments. The production Docker image installs `tensorflow-cpu==2.15.0`.

### CPU Core Thrashing Prevention

Four Gunicorn workers can otherwise each create TensorFlow native worker pools, multiplying runnable threads beyond the available CPU cores. During startup, the service constrains TensorFlow's internal pools:

```python
tf.config.threading.set_intra_op_parallelism_threads(1)
tf.config.threading.set_inter_op_parallelism_threads(1)
```

This keeps the per-request CPU footprint predictable and prevents oversubscription from turning concurrent inference into scheduler contention.

### Event-Loop Starvation Resolution

`gevent` cooperatively schedules greenlets, but a synchronous native C++ TensorFlow call does not necessarily yield to the event loop. The service therefore dispatches TensorFlow inference to a dedicated `gevent.threadpool.ThreadPool` with four OS-backed threads:

```python
_tf_inference_pool = ThreadPool(4)
probability = _tf_inference_pool.spawn(_run_tensor_inference).get()
```

The inference still completes before the response is returned, but the native call is isolated from the worker's cooperative scheduling path. Redis I/O and other greenlets remain responsive while CPU inference is running.

### Asynchronous Audit Dispatch

PostgreSQL is an audit dependency, not a prerequisite for calculating the fraud decision. The request path builds the audit payload and submits it to an eight-worker background executor. Database connection cleanup occurs inside the worker so connections are not leaked across thread boundaries. Database failures are logged and reported to the PostgreSQL circuit breaker without delaying the client response.

## Benchmark Performance And Telemetry

The final burst benchmark used 100 concurrent requests with a `0.0` second ramp-up against four Gunicorn workers:

| Metric | Result |
| --- | ---: |
| Throughput | **256.3 req/sec** |
| p50 latency | **122.35 ms** |
| p99 latency | **216.94 ms** |
| Success rate | **100% (200 OK)** |

The benchmark supports two profiles:

```bash
# Resource-safe local defaults: 50 workers, 500 requests, 10-second ramp-up
python benchmark_load.py

# Burst profile: 100 concurrent workers, 100 requests, zero ramp-up
BENCHMARK_PROFILE=burst python benchmark_load.py
```

The burst profile is intended to isolate server-side pipeline latency from client-side Python thread contention. External generators can be used for an independent client implementation:

```bash
hey -n 100 -c 100 -m POST \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"load-1","user_id":"user_1","amount":100}' \
  http://127.0.0.1:8000/score
```

### Prometheus Metrics

The service exposes Prometheus instrumentation for:

- `fraud_scoring_requests_total`: request counts by decision and serving mode.
- `fraud_scoring_latency_seconds`: end-to-end latency histogram.
- `fraud_feature_staleness_seconds`: observed Redis feature age.
- `fraud_scoring_idempotent_replays_total`: replay counter.
- `fraud_circuit_breaker_state`: Redis and PostgreSQL circuit state gauges.

### Structured Phase Timing Logs

Each request emits JSON timing records with an event name, transaction ID, phase, and duration in milliseconds:

```json
{"event":"scoring_phase_timing","transaction_id":"txn-123","phase":"redis_lookup","duration_ms":0.742}
{"event":"scoring_phase_timing","transaction_id":"txn-123","phase":"tensorflow_inference","duration_ms":3.184}
{"event":"scoring_phase_timing","transaction_id":"txn-123","phase":"postgres_audit_dispatch","duration_ms":0.091}
```

These timings distinguish Redis access, actual model execution, and queue submission overhead from client-side connection and scheduling costs.

## Getting Started And Reproducibility

### Prerequisites

- Docker Desktop with Docker Compose.
- Python 3.10 or later for local benchmark and test commands.
- Optional: `hey` for an external HTTP load generator.

### Start The Full Stack

From the repository root:

```bash
docker compose up --build
```

The stack includes Kafka, Redis, PostgreSQL, the Django scoring API, and the Spark streaming ingestion worker. The API is available at `http://127.0.0.1:8000`.

### Run The Integration Tests

```bash
python test_pipeline_integration.py
```

The suite covers model inference behavior, sliding-window deduplication, staleness routing, idempotency semantics, circuit-breaker transitions, and JMeter configuration.

### Run The Benchmark

After the API is healthy:

```bash
python benchmark_load.py
```

For a focused 100-request burst:

```bash
BENCHMARK_PROFILE=burst python benchmark_load.py
```

On PowerShell, set the profile for the command process with:

```powershell
$env:BENCHMARK_PROFILE = "burst"
python benchmark_load.py
```

### Inspect Metrics And Logs

Prometheus metrics are available at:

```text
http://127.0.0.1:8000/metrics
```

Filter service logs for phase timings using the `scoring_phase_timing` event. The `transaction_id` field allows a single request to be followed across Redis lookup, inference, and audit dispatch.

## Repository Layout

```text
fraud_pipeline/
├── benchmark_load.py
├── docker-compose.yml
├── Dockerfile.django
├── Dockerfile.spark
├── generate_sample_data.py
├── jmeter_test.jmx
├── requirements.txt
├── spark_ingestion.py
├── test_pipeline_integration.py
├── test_speed.py
└── fraud_service/
    ├── manage.py
    ├── fraud_service/
    │   ├── settings.py
    │   ├── urls.py
    │   ├── asgi.py
    │   └── wsgi.py
    └── scoring/
        ├── apps.py
        ├── models.py
        ├── serializers.py
        ├── throttles.py
        ├── urls.py
        ├── views.py
        └── ml/
            ├── fraud_model.h5
            ├── generate_model.py
            └── inference.py
```

## Production Considerations

The included Docker Compose deployment is a reproducible local and demonstration environment. A production deployment should additionally provide:

1. A highly available PostgreSQL deployment and a durable message or task queue for audit writes that must survive process termination.
2. Redis Cluster or managed Redis with replication, failover, authentication, and encrypted transport.
3. A multi-broker Kafka cluster with replicated topics and durable object-storage checkpoints for Spark.
4. A model registry, versioned rollout process, model integrity checks, and drift monitoring.
5. Prometheus alerting for request errors, latency, circuit state, feature staleness, queue depth, and audit-write failures.
6. Container resource limits sized to the number of Gunicorn workers, TensorFlow inference threads, Redis connections, and database connections.
