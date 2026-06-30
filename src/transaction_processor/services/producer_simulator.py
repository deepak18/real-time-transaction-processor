import argparse
import random
import time
import uuid

from confluent_kafka import Producer

from src.transaction_processor.common.events import build_event, to_json_bytes
from src.transaction_processor.common.kafka_client import producer_config
from src.transaction_processor.common.topics import TOPICS


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
        )
        producer.poll(0)
        print(f"produced txn={txn['txn_id']} card_id={txn['card_id']} amount={txn['amount']}")
        time.sleep(max(args.sleep_ms, 0) / 1000)

    producer.flush()


if __name__ == "__main__":
    main()
