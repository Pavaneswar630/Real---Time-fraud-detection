"""
Rate Throttling Policies
========================
Capacity planning:
  - 4 Gunicorn gevent worker processes with cooperative concurrency
  - ~5ms average p50 request latency -> ~200 req/sec per slot
  - Peak server capacity: 8 * 200 = 1,600 req/sec
  - Ingress limit: 1,200 req/sec (72,000 req/min) providing 25% safety headroom
  - RFC 6585 429 response with Retry-After header
"""

from rest_framework.throttling import AnonRateThrottle
from prometheus_client import Counter

RATE_LIMIT_EXCEEDED_COUNTER = Counter(
    "fraud_scoring_rate_limit_exceeded_total",
    "Requests rejected due to ingress capacity rate limiting"
)


class CapacityPlannedRateThrottle(AnonRateThrottle):
    rate = "72000/minute"

    def throttle_failure(self):
        RATE_LIMIT_EXCEEDED_COUNTER.inc()
        super().throttle_failure()


# Alias for backward compatibility
HighThroughputRateThrottle = CapacityPlannedRateThrottle
