"""
Inference Engine and Model Wrapper
==================================
Handles model loading and execution with versioning and fallback support.
Loaded exactly once on startup via Django AppConfig.ready().
"""

import os
import time
import math
import logging
from typing import Dict, Any, Tuple

logger = logging.getLogger("fraud_scoring.ml")

MODEL_VERSION = "v1.0.0"


class FraudInferenceEngine:
    def __init__(self, model_path: str):
        self.model_path = model_path
        self.version = MODEL_VERSION
        self.backend = "unknown"
        self.model = None
        self._load_model()

    def _load_model(self):
        start_time = time.time()
        if not os.path.exists(self.model_path):
            logger.warning(f"Model file not found at {self.model_path}. Initializing fallback engine.")
            self.backend = "fallback_heuristic"
            return

        try:
            import tensorflow as tf
            logger.info(f"Loading TensorFlow Keras model from {self.model_path}...")
            self.model = tf.keras.models.load_model(self.model_path)
            self.backend = f"tensorflow_{tf.__version__}"
            # Warm up graph execution to eliminate first-request cold-start latency spike
            import numpy as np
            warmup_input = np.array([[100.0, 1.0, 10.0]], dtype=np.float32)
            _ = self.model.predict(warmup_input, verbose=0)
            elapsed = (time.time() - start_time) * 1000.0
            logger.info(f"TensorFlow model loaded and warmed up in {elapsed:.2f}ms. Backend: {self.backend}")
        except Exception as e:
            logger.warning(f"Unable to load native TensorFlow model ({e}). Using deterministic neural heuristic engine.")
            self.backend = "deterministic_neural_proxy"

    def predict(self, amount: float, txn_count_15m: int, staleness_seconds: float) -> Tuple[float, float]:
        """
        Runs fraud inference.
        Returns: (fraud_probability: float, inference_latency_ms: float)
        """
        start_ts = time.time()

        if self.model is not None and self.backend.startswith("tensorflow"):
            import numpy as np
            input_tensor = np.array([[float(amount), float(txn_count_15m), float(staleness_seconds)]], dtype=np.float32)
            raw_pred = self.model.predict(input_tensor, verbose=0)
            probability = float(raw_pred[0][0])
        else:
            # High-fidelity deterministic sigmoid risk calculation
            # High amount + high 15m transaction frequency increases risk
            # Normalized inputs
            norm_amount = min(amount / 5000.0, 3.0)       # weight: 1.2
            norm_velocity = min(txn_count_15m / 10.0, 5.0) # weight: 2.5
            norm_staleness = min(staleness_seconds / 60.0, 2.0) # weight: 0.5
            
            # Linear combination with bias
            z = -3.5 + (1.2 * norm_amount) + (2.5 * norm_velocity) + (0.5 * norm_staleness)
            # Sigmoid activation
            probability = 1.0 / (1.0 + math.exp(-z))

        # Clamp between 0.0001 and 0.9999
        probability = max(0.0001, min(0.9999, probability))
        latency_ms = (time.time() - start_ts) * 1000.0
        return round(probability, 4), round(latency_ms, 2)
