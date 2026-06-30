"""Create and manage Kafka topics (E8: increasing partitions).

Default (no args): create every topic in `TOPICS` with the configured partition count
and replication factor (idempotent — existing topics are skipped).

E8 subcommands:
  --describe                   print the current partition count for each topic.
  --alter NAME --partitions N  GROW topic NAME to N partitions (in place, non-destructive).
  --delete NAME                delete topic NAME (destructive — drops all data/offsets).
  --recreate NAME [--partitions N]
                               RESIZE topic NAME by delete + create (destructive). This is
                               the only way to REDUCE partitions, since create_partitions can
                               only grow. Defaults N to the configured partition count.

NOTE: Kafka can only **grow** a topic's partitions in place, never shrink them, and adding
partitions changes the key->partition mapping (murmur2(key) % N) for FUTURE records —
so existing per-key ordering guarantees can break. See LEARNING_NOTES 1.8.
"""

import argparse
import time

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


def _create_one(admin: AdminClient, topic: str, partitions: int) -> None:
    new_topic = NewTopic(
        topic, num_partitions=partitions, replication_factor=settings.replication_factor
    )
    for topic_name, future in admin.create_topics([new_topic]).items():
        try:
            future.result()
            print(f"created topic={topic_name} partitions={partitions}")
        except Exception as exc:  # noqa: BLE001
            print(f"create failed topic={topic_name}: {exc}")


def delete_topics(admin: AdminClient, names: list[str]) -> None:
    # Destructive: deletes the log AND the committed offsets for every consumer group.
    for topic_name, future in admin.delete_topics(names, operation_timeout=_TIMEOUT).items():
        try:
            future.result()
            print(f"deleted topic={topic_name}")
        except Exception as exc:  # noqa: BLE001
            print(f"delete failed topic={topic_name}: {exc}")


def _wait_until_absent(admin: AdminClient, topic: str, timeout: float = _TIMEOUT) -> bool:
    # Topic deletion is asynchronous; poll metadata until the name disappears.
    deadline = time.time() + timeout
    while time.time() < deadline:
        if topic not in admin.list_topics(timeout=_TIMEOUT).topics:
            return True
        time.sleep(0.5)
    return False


def recreate_topic(admin: AdminClient, topic: str, partitions: int) -> None:
    # The ONLY way to REDUCE partitions: drop and rebuild. Destroys all data/offsets.
    print(f"WARNING: recreating {topic} DELETES all its data and consumer offsets")
    delete_topics(admin, [topic])
    if not _wait_until_absent(admin, topic):
        print(f"timed out waiting for {topic} to be deleted; aborting recreate")
        return
    _create_one(admin, topic, partitions)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or manage Kafka topics")
    parser.add_argument(
        "--describe", action="store_true", help="print current partition counts and exit"
    )
    parser.add_argument("--alter", metavar="TOPIC", help="topic to grow partitions on (E8)")
    parser.add_argument("--delete", metavar="TOPIC", help="delete a topic (destructive)")
    parser.add_argument(
        "--recreate",
        metavar="TOPIC",
        help="resize a topic via delete+create (destructive; only way to shrink)",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        help="partition count: required for --alter (must be > current), optional for "
        "--recreate (defaults to KAFKA_TOPIC_PARTITIONS)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    admin = _admin()

    if args.describe:
        describe_topics(admin)
        return

    if args.delete:
        delete_topics(admin, [args.delete])
        return

    if args.recreate:
        partitions = args.partitions or settings.topic_partitions
        recreate_topic(admin, args.recreate, partitions)
        return

    if args.alter:
        if not args.partitions:
            raise SystemExit("--alter requires --partitions N")
        alter_partitions(admin, args.alter, args.partitions)
        return

    create_topics(admin)


if __name__ == "__main__":
    main()
