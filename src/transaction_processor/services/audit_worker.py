from confluent_kafka import Consumer

from src.transaction_processor.common.events import from_json_bytes
from src.transaction_processor.common.kafka_client import consumer_config
from src.transaction_processor.common.topics import GROUP_IDS, TOPICS


def main() -> None:
    consumer = Consumer(consumer_config(group_id=GROUP_IDS["audit"]))
    consumer.subscribe(
        [
            TOPICS["created"],
            TOPICS["risk_scored"],
            TOPICS["authorized"],
            TOPICS["declined"],
            TOPICS["settlement_initiated"],
            TOPICS["settled"],
            TOPICS["dlq"],
        ]
    )

    print("audit-worker listening to lifecycle topics")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            # E7: a poison record (e.g., bad-json) may be un-parseable; never let one
            # bad message crash the auditor. Log a fallback line and keep going.
            try:
                event = from_json_bytes(msg.value())
                print(
                    "audit "
                    f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} "
                    f"txn_id={event.get('txn_id')} event_type={event.get('event_type')}"
                )
            except Exception as exc:  # noqa: BLE001
                print(
                    "audit UNPARSEABLE "
                    f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} "
                    f"error={exc}"
                )
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        print("audit-worker stopping")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
