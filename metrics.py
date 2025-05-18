from prometheus_client import Counter, Histogram, Gauge
from functools import wraps
import time

# Métricas gerais
REQUESTS_TOTAL = Counter(
    "crawler_requests_total", "Total de requisições feitas à API", ["entity"]
)

REQUEST_DURATION = Histogram(
    "crawler_request_duration_seconds",
    "Duração das requisições",
    ["entity"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
)

RECORDS_PROCESSED = Counter(
    "crawler_records_processed_total", "Total de registros processados", ["entity"]
)

ERRORS_TOTAL = Counter(
    "crawler_errors_total", "Total de erros ocorridos", ["type", "entity"]
)

ACTIVE_REQUESTS = Gauge(
    "crawler_active_requests", "Número de requisições ativas", ["entity"]
)


def track_time(entity: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            ACTIVE_REQUESTS.labels(entity=entity).inc()
            try:
                result = func(*args, **kwargs)
                REQUEST_DURATION.labels(entity=entity).observe(time.time() - start_time)
                return result
            except Exception as e:
                ERRORS_TOTAL.labels(type=type(e).__name__, entity=entity).inc()
                raise
            finally:
                ACTIVE_REQUESTS.labels(entity=entity).dec()

        return wrapper

    return decorator
