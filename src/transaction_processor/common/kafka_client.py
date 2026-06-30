from typing import Any, Dict

from .config import settings


def producer_config() -> Dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "transaction-processor",
    }


def consumer_config(group_id: str, auto_offset_reset: str = "earliest") -> Dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,
        # E2 follow-up: choose eager (default) vs cooperative-sticky rebalancing.
        "partition.assignment.strategy": settings.partition_assignment_strategy,
    }

