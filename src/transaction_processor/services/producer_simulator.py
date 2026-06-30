import argparse
import random
import time
import uuid
from collections import Counter

from confluent_kafka import KafkaError, Message, Producer

from src.transaction_processor.common.events import build_event, to_json_bytes
from src.transaction_processor.common.kafka_client import producer_config
from src.transaction_processor.common.topics import TOPICS

# E1: track how many records land on each partition so we can SEE the fan-out.
partition_counts: Counter[int] = Counter()
# E1: track which partition each card_id maps to, to PROVE same key -> same partition.
card_to_partition: dict[str, int] = {}


def delivery_report(err: KafkaError | None, msg: Message) -> None:
    """Called once per record after the broker acknowledges (or fails) it.

    This is the only reliable place to learn the final partition/offset.
    In production this is also where you detect and react to delivery failures.
    """
    if err is not None:
        print(f"DELIVERY FAILED: {err}")
        return

    partition = msg.partition()
    key = msg.key().decode("utf-8") if msg.key() else "<none>"
    partition_counts[partition] += 1

    previous = card_to_partition.setdefault(key, partition)
    ordering_ok = "OK" if previous == partition else "VIOLATED"
    print(
        f"delivered key={key} -> partition={partition} offset={msg.offset()} "
        f"ordering={ordering_ok}"
    )


def generate_transaction(index: int) -> dict:
    card_id = f"card-{(index % 5) + 1}"
    txn_id = str(uuid.uuid4())
    amount = round(random.uniform(10, 3500), 2)
    merchant_risk = random.randint(1, 9)
    is_cross_border = random.choice([False, False, False, True])

    return {
        "txn_id": txn_id,
        "card_id": card_id,
        "account_id": f"acct-{(index % 3) + 1}",
        "merchant_id": f"m-{(index % 8) + 1}",
        "amount": amount,
        "currency": "USD",
        "merchant_risk": merchant_risk,
        "is_cross_border": is_cross_border,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce transaction created events")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--sleep-ms", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    producer = Producer(producer_config())

    for index in range(args.count):
        txn = generate_transaction(index)
        event = build_event(
            event_type=TOPICS["created"],
            txn_id=txn["txn_id"],
            card_id=txn["card_id"],
            body=txn,
        )
        producer.produce(
            topic=TOPICS["created"],
            key=txn["card_id"].encode("utf-8"),
            value=to_json_bytes(event),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        print(f"produced txn={txn['txn_id']} card_id={txn['card_id']} amount={txn['amount']}")
        time.sleep(max(args.sleep_ms, 0) / 1000)

    producer.flush()

    # E1: summarize the fan-out so the distribution is easy to read.
    print("\n--- partition fan-out summary ---")
    for partition in sorted(partition_counts):
        print(f"partition {partition}: {partition_counts[partition]} records")
    print("\n--- card_id -> partition mapping ---")
    for card_id in sorted(card_to_partition):
        print(f"{card_id} -> partition {card_to_partition[card_id]}")


if __name__ == "__main__":
    main()
