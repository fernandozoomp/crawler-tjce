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


def track_time(func):
    """Um decorador para rastrear a duração, contagem e erros de uma função,
    extraindo a 'entity' dinamicamente dos argumentos da função decorada.

    Espera que a função decorada receba um argumento nomeado 'entity_slug_or_official_name'
    ou que o primeiro argumento posicional seja a entidade.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        entity_label = "unknown_entity_metric"

        # Tenta obter a entidade de kwargs
        if "entity_slug_or_official_name" in kwargs:
            entity_label = kwargs["entity_slug_or_official_name"]
        elif "entity" in kwargs:  # Fallback para kwarg 'entity'
            entity_label = kwargs["entity"]
        elif args:  # Fallback para o primeiro argumento posicional
            # Isso é um pouco arriscado, pois o primeiro arg pode não ser a entidade
            # Idealmente, as funções decoradas devem usar kwargs nomeados para a entidade.
            if isinstance(args[0], str):
                entity_label = args[0]
            # Se o primeiro arg for 'self' (em um método de classe), tenta o segundo
            elif len(args) > 1 and isinstance(args[1], str):
                entity_label = args[1]

        start_time = time.time()
        ACTIVE_REQUESTS.labels(entity=entity_label).inc()
        try:
            result = func(*args, **kwargs)
            REQUEST_DURATION.labels(entity=entity_label).observe(
                time.time() - start_time
            )
            return result
        except Exception as e:
            ERRORS_TOTAL.labels(type=type(e).__name__, entity=entity_label).inc()
            raise
        finally:
            ACTIVE_REQUESTS.labels(entity=entity_label).dec()

    return wrapper
