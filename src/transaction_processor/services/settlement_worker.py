import time

from confluent_kafka import Consumer, Producer

from src.transaction_processor.common.config import settings
from src.transaction_processor.common.events import build_event, from_json_bytes, to_json_bytes
from src.transaction_processor.common.kafka_client import consumer_config, producer_config
from src.transaction_processor.common.topics import GROUP_IDS, TOPICS


def main() -> None:
    consumer = Consumer(consumer_config(group_id=GROUP_IDS["settlement"]))
    producer = Producer(producer_config())
    consumer.subscribe([TOPICS["authorized"]])

    print("settlement-worker listening on txn.authorized")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            event = from_json_bytes(msg.value())
            initiated = build_event(
                event_type=TOPICS["settlement_initiated"],
                txn_id=event["txn_id"],
                card_id=event["card_id"],
                body=event["body"],
            )
            producer.produce(
                topic=TOPICS["settlement_initiated"],
                key=event["card_id"].encode("utf-8"),
                value=to_json_bytes(initiated),
            )
            producer.poll(0)

            time.sleep(settings.settlement_delay_seconds)
            settled = build_event(
                event_type=TOPICS["settled"],
                txn_id=event["txn_id"],
                card_id=event["card_id"],
                body={**event["body"], "settlement_status": "settled"},
            )
            producer.produce(
                topic=TOPICS["settled"],
                key=event["card_id"].encode("utf-8"),
                value=to_json_bytes(settled),
            )
            producer.poll(0)
            consumer.commit(message=msg, asynchronous=False)
            print(f"settled txn={event['txn_id']}")
    except KeyboardInterrupt:
        print("settlement-worker stopping")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
