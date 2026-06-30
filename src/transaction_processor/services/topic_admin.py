"""Create and manage Kafka topics (E8: increasing partitions).

Default (no args): create every topic in `TOPICS` with the configured partition count
and replication factor (idempotent — existing topics are skipped).

E8 subcommands:
  --describe                 print the current partition count for each topic.
  --alter NAME --partitions N  increase topic NAME to N partitions.

NOTE: Kafka can only **grow** a topic's partitions, never shrink them, and adding
partitions changes the key->partition mapping (murmur2(key) % N) for FUTURE records —
so existing per-key ordering guarantees can break. See LEARNING_NOTES 1.8.
"""

import argparse

from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

from src.transaction_processor.common.config import settings
from src.transaction_processor.common.topics import TOPICS

_TIMEOUT = 10.0


def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})


def create_topics(admin: AdminClient) -> None:
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


def describe_topics(admin: AdminClient) -> None:
    metadata = admin.list_topics(timeout=_TIMEOUT)
    print("current partition counts:")
    for name in TOPICS.values():
        topic_md = metadata.topics.get(name)
        if topic_md is None or topic_md.error is not None:
            print(f"  {name}: <not found>")
        else:
            print(f"  {name}: partitions={len(topic_md.partitions)}")


def alter_partitions(admin: AdminClient, topic: str, new_total: int) -> None:
    # create_partitions only INCREASES the count; Kafka rejects a value <= current.
    futures = admin.create_partitions([NewPartitions(topic, new_total)])
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"altered topic={topic_name} -> partitions={new_total}")
        except Exception as exc:  # noqa: BLE001
            print(f"alter failed topic={topic_name}: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or manage Kafka topics")
    parser.add_argument(
        "--describe", action="store_true", help="print current partition counts and exit"
    )
    parser.add_argument("--alter", metavar="TOPIC", help="topic to grow partitions on (E8)")
    parser.add_argument(
        "--partitions", type=int, help="new total partition count for --alter (must be > current)"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    admin = _admin()

    if args.describe:
        describe_topics(admin)
        return

    if args.alter:
        if not args.partitions:
            raise SystemExit("--alter requires --partitions N")
        alter_partitions(admin, args.alter, args.partitions)
        return

    create_topics(admin)


if __name__ == "__main__":
    main()
