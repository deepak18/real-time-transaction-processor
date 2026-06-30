from confluent_kafka import Consumer, Producer

from src.transaction_processor.common.events import build_event, from_json_bytes, to_json_bytes
from src.transaction_processor.common.kafka_client import consumer_config, producer_config
from src.transaction_processor.common.topics import GROUP_IDS, TOPICS
from src.transaction_processor.domain.risk_engine import authorize_from_score


def main() -> None:
    consumer = Consumer(consumer_config(group_id=GROUP_IDS["decision"]))
    producer = Producer(producer_config())
    consumer.subscribe([TOPICS["risk_scored"]])

    print("decision-worker listening on txn.risk_scored")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            event = from_json_bytes(msg.value())
            score = int(event["body"]["risk_score"])
            decision = authorize_from_score(score)
            output_topic = TOPICS["authorized"] if decision["approved"] else TOPICS["declined"]
            out_event = build_event(
                event_type=output_topic,
                txn_id=event["txn_id"],
                card_id=event["card_id"],
                body={**event["body"], **decision},
            )
            producer.produce(
                topic=output_topic,
                key=event["card_id"].encode("utf-8"),
                value=to_json_bytes(out_event),
            )
            producer.poll(0)
            consumer.commit(message=msg, asynchronous=False)
            print(f"decision txn={event['txn_id']} approved={decision['approved']}")
    except KeyboardInterrupt:
        print("decision-worker stopping")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
