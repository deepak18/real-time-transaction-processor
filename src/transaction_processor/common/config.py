import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_partitions: int = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "3"))
    replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    settlement_delay_seconds: float = float(os.getenv("SETTLEMENT_DELAY_SECONDS", "1.0"))

    # Consumer partition assignor. Default ("range,roundrobin") is the librdkafka
    # default => EAGER rebalancing (revoke-all-then-reassign). Set to
    # "cooperative-sticky" to use incremental cooperative rebalancing (E2 follow-up).
    partition_assignment_strategy: str = os.getenv(
        "KAFKA_PARTITION_ASSIGNMENT_STRATEGY", "range,roundrobin"
    )

    # Docs: disable Swagger UI in production to avoid exposing internals
    DOCS_URL: str | None = "/docs"
    REDOC_URL: str | None = "/redoc"
    OPENAPI_URL: str | None = "/openapi.json"


settings = Settings()

