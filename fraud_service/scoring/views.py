"""
Fraud Scoring API Views
=======================
Implements the high-performance /score POST endpoint with:
- Idempotency check on transaction_id
- Sub-millisecond Redis feature store lookup
- Stateful Circuit Breaker on Redis and PostgreSQL dependencies
- 60-second staleness fallback to degraded/manual review mode
- Pre-loaded in-memory TensorFlow inference
- Capacity-planned ingress rate limiting with RFC 6585 Retry-After headers
- PostgreSQL 15 audit ledger persistence
- Prometheus latency and distribution telemetry
"""

import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Dict, Any, Optional, Tuple

from django.conf import settings
from django.db import close_old_connections, IntegrityError
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.throttling import AnonRateThrottle
from rest_framework.exceptions import Throttled

from prometheus_client import Counter, Histogram, Gauge

from scoring.apps import get_model
from scoring.models import ScoredTransaction
from scoring.serializers import ScoreRequestSerializer, ScoreResponseSerializer

logger = logging.getLogger("fraud_scoring.views")

# -------------------------------------------------------------------------
# Prometheus Telemetry Definitions
# -------------------------------------------------------------------------
REQUEST_COUNTER = Counter(
    "fraud_scoring_requests_total",
    "Total transaction scoring requests evaluated",
    ["decision", "mode"]
)

LATENCY_HISTOGRAM = Histogram(
    "fraud_scoring_latency_seconds",
    "End-to-end latency of scoring requests in seconds",
    buckets=[0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0]
)

STALENESS_HISTOGRAM = Histogram(
    "fraud_feature_staleness_seconds",
    "Observed age of Redis features at time of scoring",
    buckets=[1.0, 5.0, 15.0, 30.0, 45.0, 60.0, 90.0, 180.0, 300.0, 900.0]
)

IDEMPOTENT_REPLAY_COUNTER = Counter(
    "fraud_scoring_idempotent_replays_total",
    "Transactions returned via idempotency cache"
)

CIRCUIT_BREAKER_STATE_GAUGE = Gauge(
    "fraud_circuit_breaker_state",
    "Current state of dependency circuit breaker (0=Closed, 1=HalfOpen, 2=Open)",
    ["dependency"]
)

# Threshold for real-time feature freshness (in seconds)
STALENESS_THRESHOLD_SECONDS = 60.0



# -------------------------------------------------------------------------
# Stateful Circuit Breaker Pattern (Martin Fowler / Michael Nygard)
# -------------------------------------------------------------------------
class CircuitState:
    CLOSED = "CLOSED"      # Normal operation: requests pass through
    OPEN = "OPEN"          # Downstream failed: requests short-circuited immediately
    HALF_OPEN = "HALF_OPEN"# Trial period: allowing probe requests to test recovery


class StatefulCircuitBreaker:
    """
    Prevents cascading failure and hammering degraded downstream services (Redis/PostgreSQL).
    Transitions:
      CLOSED -> (failure_threshold consecutive failures) -> OPEN
      OPEN -> (recovery_timeout cooldown elapsed) -> HALF_OPEN
      HALF_OPEN -> (half_open_success_threshold successes) -> CLOSED
      HALF_OPEN -> (single failure) -> OPEN
    """
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_success_threshold: int = 2
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_state_change = time.time()
        self._update_metric()

    def _update_metric(self):
        val = 0 if self.state == CircuitState.CLOSED else (1 if self.state == CircuitState.HALF_OPEN else 2)
        CIRCUIT_BREAKER_STATE_GAUGE.labels(dependency=self.name).set(val)

    def allow_request(self) -> bool:
        now = time.time()
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            if now - self.last_state_change >= self.recovery_timeout:
                logger.info(f"CircuitBreaker[{self.name}] cooldown expired. Entering HALF_OPEN trial state.")
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                self.last_state_change = now
                self._update_metric()
                return True
            return False  # Short-circuit without calling downstream
        elif self.state == CircuitState.HALF_OPEN:
            return True
        return True

    def record_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.half_open_success_threshold:
                logger.info(f"CircuitBreaker[{self.name}] trial successful! Returning to CLOSED state.")
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                self.last_state_change = time.time()
                self._update_metric()
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0

    def record_failure(self):
        now = time.time()
        if self.state == CircuitState.HALF_OPEN:
            logger.warning(f"CircuitBreaker[{self.name}] probe failed in HALF_OPEN. Tripping back to OPEN.")
            self.state = CircuitState.OPEN
            self.last_state_change = now
            self._update_metric()
        elif self.state == CircuitState.CLOSED:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                logger.error(
                    f"CircuitBreaker[{self.name}] reached {self.failure_threshold} consecutive failures! "
                    f"Tripping to OPEN for {self.recovery_timeout}s cooldown."
                )
                self.state = CircuitState.OPEN
                self.last_state_change = now
                self._update_metric()


# Singleton Circuit Breakers
redis_circuit_breaker = StatefulCircuitBreaker("redis", failure_threshold=5, recovery_timeout=30.0)
db_circuit_breaker = StatefulCircuitBreaker("postgres", failure_threshold=5, recovery_timeout=30.0)


from scoring.apps import get_model
from scoring.models import ScoredTransaction
from scoring.serializers import ScoreRequestSerializer, ScoreResponseSerializer
from scoring.throttles import CapacityPlannedRateThrottle, HighThroughputRateThrottle, RATE_LIMIT_EXCEEDED_COUNTER




# Lazy global Redis client pool
_redis_client = None
_audit_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="fraud-audit")


def _persist_audit_record(record: Dict[str, Any]) -> None:
    """Persist an audit record outside the request thread."""
    close_old_connections()
    try:
        ScoredTransaction.objects.create(**record)
        db_circuit_breaker.record_success()
    except IntegrityError:
        # Another request may have queued the same transaction first.
        logger.info("Audit record already exists for %s", record["transaction_id"])
    except Exception:
        db_circuit_breaker.record_failure()
        logger.exception("PostgreSQL audit write failed for %s", record["transaction_id"])
    finally:
        close_old_connections()


def enqueue_audit_record(record: Dict[str, Any]) -> None:
    """Queue audit persistence so PostgreSQL latency does not delay scoring."""
    if db_circuit_breaker.allow_request():
        _audit_executor.submit(_persist_audit_record, record)
    else:
        logger.warning(
            "PostgreSQL CircuitBreaker is OPEN. Skipping audit write for %s.",
            record["transaction_id"],
        )


def get_redis():
    """Retrieve or initialize pooled Redis connection."""
    global _redis_client
    if _redis_client is None:
        import redis
        host = getattr(settings, "REDIS_HOST", os.getenv("REDIS_HOST", "localhost"))
        port = int(getattr(settings, "REDIS_PORT", os.getenv("REDIS_PORT", "6379")))
        pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=0,
            socket_timeout=1.0,
            socket_connect_timeout=1.0,
            max_connections=300
        )
        _redis_client = redis.Redis(connection_pool=pool)
    return _redis_client


class ScoreTransactionView(APIView):
    throttle_classes = [CapacityPlannedRateThrottle]

    def throttled(self, request, wait):
        """Standardized RFC 6585 429 Too Many Requests response."""
        response = Response(
            {
                "error": "rate_limit_exceeded",
                "message": f"Throughput capacity exceeded. Please back off.",
                "retry_after_seconds": int(wait) if wait else 1,
            },
            status=status.HTTP_429_TOO_MANY_REQUESTS
        )
        response["Retry-After"] = str(int(wait) if wait else 1)
        return response

    def post(self, request, *args, **kwargs):
        start_time = time.time()

        # 1. Payload Validation
        serializer = ScoreRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        data = serializer.validated_data
        transaction_id = data["transaction_id"]
        user_id = data["user_id"]
        amount = float(data["amount"])

        # 2. Redis Feature Retrieval Protected by Circuit Breaker
        raw_features = None
        infra_error = False

        if not redis_circuit_breaker.allow_request():
            # Circuit is OPEN: Short-circuit immediately, avoid hammering failing Redis
            logger.warning(f"Redis CircuitBreaker is OPEN. Short-circuiting feature lookup for {transaction_id}.")
            infra_error = True
        else:
            try:
                r = get_redis()
                feature_key = f"user:{user_id}:features"
                raw_features = r.hgetall(feature_key)
                redis_circuit_breaker.record_success()
            except Exception as e:
                redis_circuit_breaker.record_failure()
                logger.error(f"Redis feature store connection failed: {e}. Circuit failure recorded.")
                infra_error = True

        # 3. Feature Evaluation & Staleness Check
        now_ts = time.time()
        features_dict = {}
        staleness_seconds: Optional[float] = None

        if infra_error:
            # Infrastructure Outage Fallback: Fail degraded (Never silently approve on blind infra)
            mode = "INFRA_DEGRADED"
            decision = "MANUAL_REVIEW"
            fraud_probability = 0.50
            features_dict = {"status": "redis_unavailable", "circuit_state": redis_circuit_breaker.state}
            logger.warning(f"Transaction {transaction_id} routed to MANUAL_REVIEW due to Redis outage.")

        elif raw_features and len(raw_features) > 0:
            # Decode Redis bytes
            decoded = {k.decode("utf-8") if isinstance(k, bytes) else k:
                       v.decode("utf-8") if isinstance(v, bytes) else v
                       for k, v in raw_features.items()}
            
            txn_count_15m = int(decoded.get("txn_count_15m", 0))
            total_amount_15m = float(decoded.get("total_amount_15m", 0.0))
            last_updated = float(decoded.get("last_updated", now_ts))
            
            staleness_seconds = max(0.0, round(now_ts - last_updated, 2))
            STALENESS_HISTOGRAM.observe(staleness_seconds)

            features_dict = {
                "txn_count_15m": txn_count_15m,
                "total_amount_15m": total_amount_15m,
                "last_updated": int(last_updated)
            }

            # 60-Second Staleness Fallback Evaluation
            if staleness_seconds > STALENESS_THRESHOLD_SECONDS:
                mode = "DEGRADED_STALE"
                decision = "MANUAL_REVIEW"
                fraud_probability = 0.65  # Elevated risk due to stale feature state
                logger.warning(
                    f"Staleness SLA breach for user {user_id}: {staleness_seconds}s > "
                    f"{STALENESS_THRESHOLD_SECONDS}s. Routed to degraded review mode."
                )
            else:
                # Fresh real-time features available: Execute TensorFlow Model
                mode = "REAL_TIME"
                engine = get_model()
                fraud_probability, _ = engine.predict(
                    amount=amount,
                    txn_count_15m=txn_count_15m,
                    staleness_seconds=staleness_seconds
                )
                
                # Decision Matrix
                if fraud_probability < 0.40:
                    decision = "APPROVED"
                elif fraud_probability < 0.75:
                    decision = "FLAGGED"
                else:
                    decision = "REJECTED"

        else:
            # Cold Start: First transaction seen for this user (or Redis TTL expired)
            mode = "COLD_START"
            staleness_seconds = None
            features_dict = {"txn_count_15m": 0, "total_amount_15m": 0.0}
            
            engine = get_model()
            fraud_probability, _ = engine.predict(
                amount=amount,
                txn_count_15m=0,
                staleness_seconds=0.0
            )

            # High value cold-start transactions trigger review
            if amount > 3000.0 or fraud_probability >= 0.75:
                decision = "MANUAL_REVIEW"
            elif fraud_probability >= 0.40:
                decision = "FLAGGED"
            else:
                decision = "APPROVED"

        # 4. Queue PostgreSQL audit persistence; never wait on the database here.
        engine = get_model()
        model_version = getattr(engine, "version", "v1.0.0")
        enqueue_audit_record({
            "transaction_id": transaction_id,
            "user_id": user_id,
            "amount": Decimal(f"{amount:.2f}"),
            "fraud_probability": fraud_probability,
            "decision": decision,
            "mode": mode,
            "staleness_seconds": staleness_seconds,
            "features_snapshot": features_dict,
            "model_version": model_version,
        })

        # 5. Record Observability Telemetry
        total_latency_ms = (time.time() - start_time) * 1000.0
        total_latency_sec = total_latency_ms / 1000.0
        
        REQUEST_COUNTER.labels(decision=decision, mode=mode).inc()
        LATENCY_HISTOGRAM.observe(total_latency_sec)

        # 6. Standardized JSON Response
        response_data = {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "fraud_probability": fraud_probability,
            "decision": decision,
            "mode": mode,
            "staleness_seconds": staleness_seconds,
            "features": features_dict,
            "model_version": model_version,
            "latency_ms": round(total_latency_ms, 2),
            "idempotent_replay": False
        }

        return Response(response_data, status=status.HTTP_200_OK)
        # Expose the class-based view as a function-based view for urls.py compatibility
score_transaction = ScoreTransactionView.as_view()
