from django.urls import path
from scoring.views import ScoreTransactionView

urlpatterns = [
    path("score", ScoreTransactionView.as_view(), name="score_transaction"),
]