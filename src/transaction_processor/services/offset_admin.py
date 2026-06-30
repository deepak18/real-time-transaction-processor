"""Inspect and reset consumer-group offsets (E5: offset reset / replay).

A consumer group's read position is **server-side state** stored in Kafka's internal
`__consumer_offsets` topic. This tool lets you:

  * SHOW  — print each partition's committed offset, low/high watermarks, and lag.
  * RESET — move the group's committed offsets to the start (`earliest`) or end (`latest`).

`auto.offset.reset` (in `consumer_config`) only decides where a group starts when it has
**no committed offset**. To replay an *existing* group's history you must explicitly reset
its committed offsets — which is exactly what `--reset earliest` does here.

IMPORTANT: stop the workers in the target group before resetting. Committing offsets while
the group has active members is unreliable (live members can overwrite what you commit).

Examples (PowerShell):
    # Inspect where risk-cg currently is on txn.created
    python -m src.transaction_processor.services.offset_admin --group risk-cg --topic txn.created

    # Rewind risk-cg to the beginning to REPLAY all history
    python -m src.transaction_processor.services.offset_admin --group risk-cg --reset earliest

    # Fast-forward risk-cg to the end (skip backlog)
    python -m src.transaction_processor.services.offset_admin --group risk-cg --reset latest
"""

import argparse

from confluent_kafka import Consumer, TopicPartition

from src.transaction_processor.common.kafka_client import consumer_config
from src.transaction_processor.common.topics import GROUP_IDS, TOPICS

_TIMEOUT = 10.0


def _partitions_for_topic(consumer: Consumer, topic: str) -> list[int]:
    metadata = consumer.list_topics(topic, timeout=_TIMEOUT)
    topic_md = metadata.topics.get(topic)
    if topic_md is None or topic_md.error is not None:
        raise RuntimeError(f"topic not found or in error state: {topic}")
    return sorted(topic_md.partitions.keys())


def show(consumer: Consumer, topic: str) -> None:
    partitions = [TopicPartition(topic, p) for p in _partitions_for_topic(consumer, topic)]
    committed = consumer.committed(partitions, timeout=_TIMEOUT)

    print(f"group offsets for topic={topic}")
    total_lag = 0
    for tp in sorted(committed, key=lambda x: x.partition):
        low, high = consumer.get_watermark_offsets(tp, timeout=_TIMEOUT, cached=False)
        if tp.offset < 0:  # no committed offset yet (OFFSET_INVALID)
            committed_str = "<none>"
            lag = high - low
        else:
            committed_str = str(tp.offset)
            lag = high - tp.offset
        total_lag += lag
        print(
            f"  partition={tp.partition} committed={committed_str} "
            f"low={low} high={high} lag={lag}"
        )
    print(f"  total_lag={total_lag}")


def reset(consumer: Consumer, topic: str, target: str) -> None:
    new_offsets: list[TopicPartition] = []
    for p in _partitions_for_topic(consumer, topic):
        tp = TopicPartition(topic, p)
        low, high = consumer.get_watermark_offsets(tp, timeout=_TIMEOUT, cached=False)
        offset = low if target == "earliest" else high
        new_offsets.append(TopicPartition(topic, p, offset))

    consumer.commit(offsets=new_offsets, asynchronous=False)
    print(f"reset committed offsets for topic={topic} to {target}")
    for tp in sorted(new_offsets, key=lambda x: x.partition):
        print(f"  partition={tp.partition} -> offset={tp.offset}")
    print("done. start the worker(s) again to consume from the new position.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect or reset consumer-group offsets")
    parser.add_argument(
        "--group",
        default=GROUP_IDS["risk"],
        help="consumer group id (default: risk-cg)",
    )
    parser.add_argument(
        "--topic",
        default=TOPICS["created"],
        help="topic to inspect/reset (default: txn.created)",
    )
    parser.add_argument(
        "--reset",
        choices=["earliest", "latest"],
        help="reset committed offsets to the start or end; omit to just SHOW current offsets",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    # Manual-commit consumer used purely as an offset-admin handle; we never subscribe,
    # so it does not join the group's subscription or trigger a rebalance of live members.
    consumer = Consumer(consumer_config(group_id=args.group))
    try:
        if args.reset:
            reset(consumer, args.topic, args.reset)
        else:
            show(consumer, args.topic)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
