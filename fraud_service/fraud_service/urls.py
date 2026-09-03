from django.urls import path, include
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from django.http import HttpResponse

def prometheus_metrics_view(request):
    """Serve Prometheus format metrics."""
    data = generate_latest()
    return HttpResponse(data, content_type=CONTENT_TYPE_LATEST)

urlpatterns = [
    path("", include("scoring.urls")),
    path("metrics", prometheus_metrics_view, name="prometheus_metrics"),
]