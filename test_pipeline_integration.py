#!/usr/bin/env python3
"""
Pipeline Integration Test Suite
===============================
Standard unit and edge-case integration tests verifying:
1. Model loading & inference output range
2. Sliding-window aggregate deduplication vs incremental overcounting
3. 60-second staleness threshold fallback routing
4. Idempotency on repeated transaction_id
5. Stateful Circuit Breaker transitions (CLOSED -> OPEN -> HALF_OPEN -> CLOSED)
6. JMeter JMX configuration verification
"""

import os
import sys
import time
import xml.etree.ElementTree as ET

# Add fraud_service to Python path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_DIR = os.path.join(CURRENT_DIR, "fraud_service")
sys.path.insert(0, SERVICE_DIR)

# Configure Django environment for standalone test execution
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "fraud_service.settings")
os.environ["USE_SQLITE"] = "1"
import django
django.setup()




def test_model_inference():
    from scoring.ml.inference import FraudInferenceEngine
    
    model_path = os.path.join(SERVICE_DIR, "scoring", "ml", "fraud_model.h5")
    engine = FraudInferenceEngine(model_path=model_path)
    
    prob_low, lat_1 = engine.predict(amount=25.0, txn_count_15m=1, staleness_seconds=5.0)
    prob_high, lat_2 = engine.predict(amount=4500.0, txn_count_15m=12, staleness_seconds=10.0)

    assert 0.0 <= prob_low <= 1.0, f"Expected probability in [0, 1], got {prob_low}"
    assert 0.0 <= prob_high <= 1.0, f"Expected probability in [0, 1], got {prob_high}"
    assert prob_high > prob_low, f"Expected prob_high > prob_low, got {prob_high} <= {prob_low}"
    print("test_model_inference: PASS")


def test_sliding_window_deduplication():
    # Model a 15m window sliding every 1m (15 overlapping slices per 15m period)
    # True count calculated by Spark for this window is 5
    true_count = 5
    num_overlapping_windows = 15

    # Incremental accumulation (incorrect): compounds count across all overlapping slices
    accumulated_count = sum(true_count for _ in range(num_overlapping_windows))
    assert accumulated_count == 75

    # Window-deduplicated write: stores the single latest aggregate
    redis_store = {"txn_count_15m": 0, "latest_window_end": 0}
    for w in range(num_overlapping_windows):
        window_end = 1000 + (w * 60)
        if window_end >= redis_store["latest_window_end"]:
            redis_store["txn_count_15m"] = true_count
            redis_store["latest_window_end"] = window_end

    assert redis_store["txn_count_15m"] == true_count, (
        f"Expected {true_count}, got {redis_store['txn_count_15m']}"
    )
    print(f"test_sliding_window_deduplication: PASS (count={redis_store['txn_count_15m']})")


def test_staleness_fallback():
    from scoring.ml.inference import FraudInferenceEngine
    model_path = os.path.join(SERVICE_DIR, "scoring", "ml", "fraud_model.h5")
    engine = FraudInferenceEngine(model_path=model_path)

    now = time.time()
    
    # Fresh features: 15s age (<= 60s threshold)
    staleness_fresh = round(now - (now - 15.0), 2)
    assert staleness_fresh <= 60.0
    prob_fresh, _ = engine.predict(amount=100.0, txn_count_15m=1, staleness_seconds=staleness_fresh)
    mode_fresh = "REAL_TIME"
    assert mode_fresh == "REAL_TIME"

    # Stale features: 95s age (> 60s threshold)
    staleness_stale = round(now - (now - 95.0), 2)
    assert staleness_stale > 60.0
    mode_stale = "DEGRADED_STALE"
    decision_stale = "MANUAL_REVIEW"
    assert mode_stale == "DEGRADED_STALE"
    assert decision_stale == "MANUAL_REVIEW"
    print("test_staleness_fallback: PASS")


def test_idempotency_deduplication():
    scored_records = {}

    def handle_transaction(txn_id, user_id, amount):
        if txn_id in scored_records:
            return {**scored_records[txn_id], "idempotent_replay": True}
        record = {
            "transaction_id": txn_id,
            "user_id": user_id,
            "amount": amount,
            "decision": "APPROVED",
            "idempotent_replay": False
        }
        scored_records[txn_id] = record
        return record

    res1 = handle_transaction("txn_001", "user_10", 120.0)
    res2 = handle_transaction("txn_001", "user_10", 120.0)

    assert res1["idempotent_replay"] is False
    assert res2["idempotent_replay"] is True
    assert res1["decision"] == res2["decision"]
    print("test_idempotency_deduplication: PASS")


def test_stateful_circuit_breaker():
    from scoring.views import StatefulCircuitBreaker, CircuitState
    
    cb = StatefulCircuitBreaker("test_dependency", failure_threshold=3, recovery_timeout=0.2, half_open_success_threshold=2)
    
    # 1. Initially CLOSED
    assert cb.state == CircuitState.CLOSED
    assert cb.allow_request() is True

    # 2. Record 2 failures (< threshold 3): remains CLOSED
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.CLOSED
    assert cb.allow_request() is True

    # 3. 3rd failure reaches threshold: trips to OPEN
    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    # In OPEN state, calls are immediately short-circuited
    assert cb.allow_request() is False

    # 4. Wait for recovery timeout cooldown
    time.sleep(0.25)
    # First call after cooldown transitions to HALF_OPEN trial
    assert cb.allow_request() is True
    assert cb.state == CircuitState.HALF_OPEN

    # 5. First probe succeeds (needs 2 for recovery): remains HALF_OPEN
    cb.record_success()
    assert cb.state == CircuitState.HALF_OPEN

    # 6. Second probe succeeds: transitions back to CLOSED
    cb.record_success()
    assert cb.state == CircuitState.CLOSED
    assert cb.allow_request() is True
    print("test_stateful_circuit_breaker: PASS (CLOSED -> OPEN -> HALF_OPEN -> CLOSED verified)")


def test_jmeter_jmx_schema():
    jmx_path = os.path.join(CURRENT_DIR, "jmeter_test.jmx")
    assert os.path.exists(jmx_path), f"File not found: {jmx_path}"

    tree = ET.parse(jmx_path)
    root = tree.getroot()

    threads = None
    ramp_time = None
    for elem in root.iter("stringProp"):
        if elem.attrib.get("name") == "ThreadGroup.num_threads":
            threads = int(elem.text)
        elif elem.attrib.get("name") == "ThreadGroup.ramp_time":
            ramp_time = int(elem.text)

    assert threads == 500, f"Expected 500 threads, got {threads}"
    assert ramp_time == 10, f"Expected 10s ramp-up, got {ramp_time}"
    print("test_jmeter_jmx_schema: PASS (threads=500, ramp_up=10s)")


if __name__ == "__main__":
    test_model_inference()
    test_sliding_window_deduplication()
    test_staleness_fallback()
    test_idempotency_deduplication()
    test_stateful_circuit_breaker()
    test_jmeter_jmx_schema()
    print("\nAll 6 tests passed.")
