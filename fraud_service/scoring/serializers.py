"""
DRF Serializers for Fraud Scoring Endpoint
==========================================
Enforces strict payload validation and standardized API responses.
"""

from rest_framework import serializers


class ScoreRequestSerializer(serializers.Serializer):
    transaction_id = serializers.CharField(
        max_length=64,
        required=True,
        help_text="Unique external transaction identifier used as an idempotency key."
    )
    user_id = serializers.CharField(
        max_length=64,
        required=True,
        help_text="Unique customer identifier."
    )
    amount = serializers.FloatField(
        min_value=0.01,
        required=True,
        help_text="Transaction monetary amount in base currency."
    )


class ScoreResponseSerializer(serializers.Serializer):
    transaction_id = serializers.CharField()
    user_id = serializers.CharField()
    fraud_probability = serializers.FloatField()
    decision = serializers.CharField()
    mode = serializers.CharField()
    staleness_seconds = serializers.FloatField(allow_null=True)
    features = serializers.DictField()
    model_version = serializers.CharField()
    latency_ms = serializers.FloatField()
    idempotent_replay = serializers.BooleanField(default=False)
