# real-time-transaction-processor

Kafka-based fintech transaction pipeline with production-style Python project structure.

## Architecture (v1)

- `Transaction API` (FastAPI): accepts transaction requests and publishes `txn.created`
- `Risk Worker`: consumes `txn.created`, emits `txn.risk_scored`
- `Decision Worker`: consumes `txn.risk_scored`, emits `txn.authorized` or `txn.declined`
- `Settlement Worker`: consumes `txn.authorized`, emits settlement lifecycle events
- `Audit Worker`: consumes all lifecycle topics in a separate consumer group

Event flow:

`txn.created -> txn.risk_scored -> txn.authorized/txn.declined -> txn.settlement.initiated -> txn.settled`

DLQ topic:

`txn.dlq`

## Project layout

- `src/transaction_processor/common/`: config, Kafka client settings, topics, event helpers
- `src/transaction_processor/domain/`: domain logic (`risk_engine.py`)
- `src/transaction_processor/services/`: long-running workers and admin tools
- `src/transaction_processor/api/`: FastAPI app and request/response schemas
- `tests/`: lightweight smoke test

Legacy scripts in `src/*.py` are compatibility wrappers and can be removed later.

## Why FastAPI now?

Yes, we should set up FastAPI early.

- It gives a clean synchronous ingress point (`POST /v1/transactions`) while processing remains async in Kafka.
- It mirrors real systems where external channels call REST and backend services use events.
- It lets us add auth, validation, idempotency keys, and rate limits without redesigning ingestion later.

Alternative now:
- CLI-only producer (`producer_simulator`) is faster for experiments but not representative of production entrypoints.

## Docker Compose decisions

- Same Kafka image (`confluentinc/cp-kafka`) is fine across projects.
- Do not share the same container identity or volume across separate projects.
- This repo uses project-specific resources:
  - container: `rtp-kafka`
  - volume: `rtp_kafka_data`
  - optional UI: `rtp-kafka-ui` on `http://localhost:8080`

## Setup

```powershell
cd "C:\Users\Ashi\Data\Tech\Projects\GitHub\real-time-transaction-processor"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Start infra

```powershell
docker compose up -d
python -m src.transaction_processor.services.topic_admin
```

## Run services (separate terminals)

```powershell
python -m src.transaction_processor.services.risk_worker
```

```powershell
python -m src.transaction_processor.services.decision_worker
```

```powershell
python -m src.transaction_processor.services.settlement_worker
```

```powershell
python -m src.transaction_processor.services.audit_worker
```

```powershell
uvicorn src.transaction_processor.api.main:app --host 0.0.0.0 --port 8000
```

## Test ingestion API

```powershell
curl -X POST "http://localhost:8000/v1/transactions" `
  -H "Content-Type: application/json" `
  -d '{"account_id":"acct-1","card_id":"card-1","merchant_id":"m-11","amount":240.5,"currency":"USD","merchant_risk":3,"is_cross_border":false}'
```

## Simulator path (optional)

```powershell
python -m src.transaction_processor.services.producer_simulator --count 30 --sleep-ms 50
```

## Local logic test

```powershell
python tests/smoke_test.py
```
