"""
Scoring Application Configuration
================================
Loads the TensorFlow inference model into memory exactly once on application startup
to eliminate per-request disk I/O and graph compilation latency spikes.
"""

import os
import sys
import logging
from django.apps import AppConfig

logger = logging.getLogger("fraud_scoring.apps")


class ScoringConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "scoring"
    
    # Singleton model instance held in memory
    model = None

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

        if ScoringConfig.model is None:
            logger.info("Initializing Fraud Scoring Neural Network model in memory...")
            from scoring.ml.inference import FraudInferenceEngine
            
            base_dir = os.path.dirname(os.path.abspath(__file__))
            model_path = os.path.join(base_dir, "ml", "fraud_model.h5")
            
            ScoringConfig.model = FraudInferenceEngine(model_path=model_path)
            logger.info(
                f"Model successfully loaded. Version: {ScoringConfig.model.version}, "
                f"Backend: {ScoringConfig.model.backend}"
            )


def get_model():
    """Helper to access the pre-loaded singleton model instance."""
    if ScoringConfig.model is None:
        # Fallback if ready() was skipped or during direct testing
        from scoring.ml.inference import FraudInferenceEngine
        base_dir = os.path.dirname(os.path.abspath(__file__))
        model_path = os.path.join(base_dir, "ml", "fraud_model.h5")
        ScoringConfig.model = FraudInferenceEngine(model_path=model_path)
    return ScoringConfig.model
