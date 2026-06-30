# AGENTS.md

## Project snapshot
- Kafka-based fintech transaction pipeline in Python 3.10+.
- Architecture is **event-driven choreography**: `FastAPI` ingests transactions, then workers pass events topic-to-topic.
- Keep changes inside `src/transaction_processor/...`; prefer module execution (`python -m ...`) over ad-hoc scripts.

## Core flow
- Ingress: `src/transaction_processor/api/main.py` exposes `POST /v1/transactions` and publishes `txn.created`.
- Workers:
  - `services/risk_worker.py` consumes `txn.created` → emits `txn.risk_scored`.
  - `services/decision_worker.py` consumes `txn.risk_scored` → emits `txn.authorized` or `txn.declined`.
  - `services/settlement_worker.py` consumes `txn.authorized` → emits `txn.settlement.initiated` and then `txn.settled` after `SETTLEMENT_DELAY_SECONDS`.
  - `services/audit_worker.py` consumes the lifecycle topics in its own consumer group.
- All event metadata is built with `common/events.build_event(...)` and serialized with `to_json_bytes(...)`.

## Must-follow conventions
- Topic and consumer-group names come only from `common/topics.py`.
- Transaction events are keyed by `card_id` everywhere; this preserves per-card ordering across partitions.
- Consumers use `enable.auto.commit=False` and commit **after** successful processing; this repo currently models **at-least-once** delivery.
- On processing failure in `risk_worker.py`, the message is published to `txn.dlq` and the offset is still committed so one poison message does not block the partition.
- Keep the event envelope shape consistent: `event_type`, `event_version`, `occurred_at`, `txn_id`, `card_id`, `body`.
- `common/config.py` is env-driven (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_PARTITIONS`, `KAFKA_REPLICATION_FACTOR`, `SETTLEMENT_DELAY_SECONDS`).

## Runtime/developer workflow
- Local infra:
  - `docker compose up -d`
  - `python -m src.transaction_processor.services.topic_admin`
- Run workers in separate terminals:
  - `python -m src.transaction_processor.services.risk_worker`
  - `python -m src.transaction_processor.services.decision_worker`
  - `python -m src.transaction_processor.services.settlement_worker`
  - `python -m src.transaction_processor.services.audit_worker`
- Start the API:
  - `uvicorn src.transaction_processor.api.main:app --host 0.0.0.0 --port 8000`
- Generate test traffic:
  - `python -m src.transaction_processor.services.producer_simulator --count 30 --sleep-ms 50`
- Lightweight logic check:
  - `python tests/smoke_test.py`

## Infrastructure notes
- Kafka runs in **KRaft mode** via `docker-compose.yaml`; no ZooKeeper.
- Auto topic creation is disabled; topics are created explicitly by `services/topic_admin.py`.
- Docker resources are project-scoped (`rtp-kafka`, `rtp_kafka_data`, `rtp-kafka-ui`) to avoid clashing with sibling projects.
- Kafka UI is available at `http://localhost:8080` for partitions, offsets, and consumer-group inspection.

## Code patterns worth preserving
- `domain/risk_engine.py` contains pure decision logic; keep business rules isolated from Kafka I/O.
- `api/main.py` produces the event synchronously, then flushes the producer before returning the response.
- `producer_simulator.py` is the easiest way to exercise partitioning because it cycles `card_id` values (`card-1`..`card-5`).
- `audit_worker.py` subscribes to multiple topics in one separate group, which is the project’s pub/sub example.

## Documentation source of truth
- `README.md` is the runbook for setup and execution.
- `TODO.md` is the broader project guide and roadmap; it explains why this repo is structured this way.
- `docs/LEARNING_NOTES.md` is the timeless concept reference (per technology, Kafka first) for revising how each tool works and why.
- Keep those docs in sync with any workflow or architecture change.

