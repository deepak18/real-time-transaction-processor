from confluent_kafka import Consumer, Producer

from src.transaction_processor.common.events import build_event, from_json_bytes, to_json_bytes
from src.transaction_processor.common.kafka_client import consumer_config, producer_config
from src.transaction_processor.common.topics import GROUP_IDS, TOPICS
from src.transaction_processor.domain.risk_engine import score_transaction


def main() -> None:
    consumer = Consumer(consumer_config(group_id=GROUP_IDS["risk"]))
    producer = Producer(producer_config())
    consumer.subscribe([TOPICS["created"]])

    print("risk-worker listening on txn.created")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            try:
                txn_event = from_json_bytes(msg.value())
                txn = txn_event["body"]
                result = score_transaction(txn)
                out_event = build_event(
                    event_type=TOPICS["risk_scored"],
                    txn_id=txn_event["txn_id"],
                    card_id=txn_event["card_id"],
                    body={**txn, **result},
                )
                producer.produce(
                    topic=TOPICS["risk_scored"],
                    key=txn_event["card_id"].encode("utf-8"),
                    value=to_json_bytes(out_event),
                )
                producer.poll(0)
                consumer.commit(message=msg, asynchronous=False)
                print(f"risk scored txn={txn_event['txn_id']} score={result['risk_score']}")
            except Exception as exc:  # noqa: BLE001
                dlq_event = build_event(
                    event_type=TOPICS["dlq"],
                    txn_id="unknown",
                    card_id="unknown",
                    body={"error": str(exc), "raw": msg.value().decode("utf-8", errors="ignore")},
                )
                producer.produce(topic=TOPICS["dlq"], value=to_json_bytes(dlq_event))
                producer.poll(0)
                consumer.commit(message=msg, asynchronous=False)
                print(f"sent bad message to dlq error={exc}")
    except KeyboardInterrupt:
        print("risk-worker stopping")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
