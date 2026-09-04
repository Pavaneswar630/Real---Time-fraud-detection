"""
Scoring Application Configuration
================================
Loads the TensorFlow inference model into memory exactly once on application startup
to eliminate per-request disk I/O and graph compilation latency spikes.
"""

import os
import sys
import logging
from django.apps import AppConfig, apps as django_apps

logger = logging.getLogger("fraud_scoring.apps")


class ScoringConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "scoring"
    
    # Singleton model and compiled inference function held in memory.
    model = None
    infer_fn = None
    backend = "unknown"
    version = "v1.0.0"

    def ready(self):
        """
        Executed once when Django initializes application registry.
        Guarded against re-execution during development autoreload.
        """
        # Skip loading during migration generation or check commands to speed up CLI
        skip_commands = {"makemigrations", "migrate", "check", "collectstatic"}
        if len(sys.argv) > 1 and any(cmd in sys.argv for cmd in skip_commands):
            return

        # Avoid redundant loading in the parent watcher process when using runserver
        if os.environ.get("RUN_MAIN") != "true" and "runserver" in sys.argv:
            return

        if self.infer_fn is None and self.model is None:
            logger.info("Initializing Fraud Scoring Neural Network model in memory...")
            base_dir = os.path.dirname(os.path.abspath(__file__))
            model_path = os.path.join(base_dir, "ml", "fraud_model.h5")

            try:
                import tensorflow as tf

                # Constrain internal thread pools to prevent CPU core thrashing under concurrent load
                tf.config.threading.set_intra_op_parallelism_threads(1)
                tf.config.threading.set_inter_op_parallelism_threads(1)

                self.model = tf.keras.models.load_model(model_path)
                self.version = "v1.0.0"
                self.backend = f"tensorflow_{tf.__version__}"

                @tf.function(
                    input_signature=[
                        tf.TensorSpec(shape=[None, 3], dtype=tf.float32)
                    ]
                )
                def infer_fn(input_tensor):
                    return self.model(input_tensor, training=False)

                self.infer_fn = infer_fn
                self.infer_fn(tf.zeros([1, 3], dtype=tf.float32))
                logger.info(
                    "Model successfully loaded and warmed up. Version: %s, Backend: %s",
                    self.version,
                    self.backend,
                )
            except Exception as exc:
                logger.warning(
                    "Unable to initialize TensorFlow inference (%s). "
                    "Using deterministic neural heuristic engine.",
                    exc,
                )
                from scoring.ml.inference import FraudInferenceEngine

                self.model = FraudInferenceEngine(model_path=model_path)
                self.version = self.model.version
                self.backend = self.model.backend


def get_model():
    """Helper to access the pre-loaded singleton model instance."""
    app_config = django_apps.get_app_config("scoring")
    if app_config.model is None:
        # Fallback if ready() was skipped or during direct testing
        from scoring.ml.inference import FraudInferenceEngine
        base_dir = os.path.dirname(os.path.abspath(__file__))
        model_path = os.path.join(base_dir, "ml", "fraud_model.h5")
        app_config.model = FraudInferenceEngine(model_path=model_path)
        app_config.version = app_config.model.version
        app_config.backend = app_config.model.backend
    return app_config.model
