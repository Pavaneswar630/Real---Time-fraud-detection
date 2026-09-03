"""
Fraud Scoring Audit Models
==========================
Persisted to PostgreSQL 15 for regulatory audit compliance,
chargeback investigation, and fraud model retraining feedback loops.
"""

from django.db import models


class ScoredTransaction(models.Model):
    DECISION_CHOICES = [
        ("APPROVED", "Approved"),
        ("FLAGGED", "Flagged for Async Verification"),
        ("MANUAL_REVIEW", "Routed to Manual Review"),
        ("REJECTED", "Rejected (High Fraud Risk)"),
    ]

    MODE_CHOICES = [
        ("REAL_TIME", "Real-Time Streaming Feature Serving"),
        ("DEGRADED_STALE", "Degraded Mode (Staleness > 60s)"),
        ("COLD_START", "Cold Start (No Prior Redis Features)"),
        ("INFRA_DEGRADED", "Degraded Mode (Infrastructure Outage)"),
    ]

    # Idempotency constraint: each transaction_id can only be scored and recorded once
    transaction_id = models.CharField(max_length=64, unique=True, db_index=True)
    user_id = models.CharField(max_length=64, db_index=True)
    amount = models.DecimalField(max_digits=12, decimal_places=2)
    fraud_probability = models.FloatField()
    decision = models.CharField(max_length=32, choices=DECISION_CHOICES)
    mode = models.CharField(max_length=32, choices=MODE_CHOICES)
    staleness_seconds = models.FloatField(null=True, blank=True)
    features_snapshot = models.JSONField(default=dict)
    model_version = models.CharField(max_length=32, default="v1.0.0")
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "scored_transactions"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["user_id", "-created_at"]),
            models.Index(fields=["decision", "-created_at"]),
        ]

    def __str__(self):
        return f"{self.transaction_id} ({self.user_id}) - {self.decision} [{self.fraud_probability:.4f}]"
