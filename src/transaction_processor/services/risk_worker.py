import os
import time

from confluent_kafka import Consumer, Producer, TopicPartition

from src.transaction_processor.common.events import build_event, from_json_bytes, to_json_bytes
from src.transaction_processor.common.kafka_client import consumer_config, producer_config
from src.transaction_processor.common.topics import GROUP_IDS, TOPICS
from src.transaction_processor.domain.risk_engine import score_transaction

# E2: a short per-process id so multiple instances are distinguishable in logs.
WORKER_ID = f"risk-{os.getpid()}"

# E6: fault-injection hook (OFF by default). When RISK_WORKER_CRASH_AFTER_PRODUCE=1,
# the worker crashes AFTER the output event has been durably produced (flushed) but
# BEFORE the source offset is committed. On restart, the same txn.created message is
# re-read and txn.risk_scored is emitted a SECOND time -> proves at-least-once delivery
# and motivates idempotency/exactly-once (Phase 3).
CRASH_AFTER_PRODUCE = os.getenv("RISK_WORKER_CRASH_AFTER_PRODUCE", "0") == "1"

# E9: slow-processing knob (OFF by default). When RISK_WORKER_PROCESS_DELAY_MS > 0, the
# worker sleeps that long per message to simulate expensive processing. Drive a fast
# producer against a slow consumer and watch consumer LAG grow (Kafka UI / offset_admin).
PROCESS_DELAY_MS = int(os.getenv("RISK_WORKER_PROCESS_DELAY_MS", "0"))


class _FaultInjected(Exception):
    """Deliberate E6 crash to demonstrate at-least-once duplicate delivery."""


def _fmt(parts: list[TopicPartition]) -> str:
    return ", ".join(f"{p.topic}#{p.partition}" for p in parts) or "<none>"


def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    # E2: fired after a rebalance when this consumer GAINS partitions.
    print(f"[{WORKER_ID}] ASSIGNED -> {_fmt(partitions)}")


def on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    # E2: fired before a rebalance when this consumer is about to LOSE partitions.
    print(f"[{WORKER_ID}] REVOKED  -> {_fmt(partitions)}")


def main() -> None:
    consumer = Consumer(consumer_config(group_id=GROUP_IDS["risk"]))
    producer = Producer(producer_config())
    consumer.subscribe([TOPICS["created"]], on_assign=on_assign, on_revoke=on_revoke)

    print(f"[{WORKER_ID}] risk-worker listening on txn.created")
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
                # E9: simulate slow processing so lag builds under a fast producer.
                if PROCESS_DELAY_MS > 0:
                    time.sleep(PROCESS_DELAY_MS / 1000)
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

                # E6: crash AFTER the output is durably on the broker but BEFORE the
                # source offset is committed. flush() forces delivery so the duplicate
                # we observe on restart is real (not a buffered, never-sent record).
                if CRASH_AFTER_PRODUCE:
                    producer.flush()
                    print(
                        f"[{WORKER_ID}] E6 fault: produced txn.risk_scored for "
                        f"txn={txn_event['txn_id']} but crashing BEFORE commit"
                    )
                    raise _FaultInjected(txn_event["txn_id"])

                consumer.commit(message=msg, asynchronous=False)
                print(
                    f"[{WORKER_ID}] risk scored txn={txn_event['txn_id']} "
                    f"partition={msg.partition()} score={result['risk_score']}"
                )
            except _FaultInjected:
                # E6: let the deliberate crash escape the DLQ handler so the offset is
                # NOT committed. The process exits; on restart the message is re-read.
                raise
            except Exception as exc:  # noqa: BLE001
                # E7: a "poison" message (bad JSON or missing fields) can't be processed.
                # Route it to the DLQ, then STILL commit the source offset so one bad
                # event cannot block the partition for every message queued behind it.
                txn_id, card_id = "unknown", "unknown"
                try:
                    parsed = from_json_bytes(msg.value())
                    # Prefer the envelope's top-level ids, but fall back to body so a
                    # malformed-but-parseable record is still traceable.
                    body = parsed.get("body") if isinstance(parsed, dict) else None
                    body = body if isinstance(body, dict) else {}
                    txn_id = parsed.get("txn_id") or body.get("txn_id") or "unknown"
                    card_id = parsed.get("card_id") or body.get("card_id") or "unknown"
                except Exception:  # noqa: BLE001 - message may not even be valid JSON
                    pass

                dlq_event = build_event(
                    event_type=TOPICS["dlq"],
                    txn_id=txn_id,
                    card_id=card_id,
                    body={
                        "error": str(exc),
                        "source_topic": msg.topic(),
                        "source_partition": msg.partition(),
                        "source_offset": msg.offset(),
                        "raw": msg.value().decode("utf-8", errors="ignore"),
                    },
                )
                # Preserve per-card grouping in the DLQ when the key is recoverable.
                dlq_key = card_id.encode("utf-8") if card_id != "unknown" else None
                producer.produce(topic=TOPICS["dlq"], key=dlq_key, value=to_json_bytes(dlq_event))
                producer.poll(0)
                consumer.commit(message=msg, asynchronous=False)
                print(
                    f"[{WORKER_ID}] DLQ <- poison txn={txn_id} "
                    f"src={msg.topic()}#{msg.partition()} @{msg.offset()} error={exc}"
                )
    except KeyboardInterrupt:
        print(f"[{WORKER_ID}] risk-worker stopping")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
