from confluent_kafka.admin import AdminClient, NewTopic

from src.transaction_processor.common.config import settings
from src.transaction_processor.common.topics import TOPICS


def main() -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    topics = [
        NewTopic(
            name,
            num_partitions=settings.topic_partitions,
            replication_factor=settings.replication_factor,
        )
        for name in TOPICS.values()
    ]

    futures = admin.create_topics(topics)
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"created topic={topic_name}")
        except Exception as exc:  # noqa: BLE001
            print(f"topic={topic_name} skipped or failed: {exc}")


if __name__ == "__main__":
    main()
