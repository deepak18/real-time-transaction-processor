import uuid

from confluent_kafka import Producer
from fastapi import FastAPI

from src.transaction_processor.api.schemas import TransactionCreateRequest, TransactionCreateResponse
from src.transaction_processor.common.config import settings
from src.transaction_processor.common.events import build_event, to_json_bytes
from src.transaction_processor.common.kafka_client import producer_config
from src.transaction_processor.common.topics import TOPICS

app = FastAPI(
    title="Transaction Processor API",
    version="0.1.0",
    docs_url=settings.DOCS_URL,
    redoc_url=settings.REDOC_URL,
    openapi_url=settings.OPENAPI_URL,
)
producer = Producer(producer_config())


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/v1/transactions", response_model=TransactionCreateResponse)
def create_transaction(payload: TransactionCreateRequest) -> TransactionCreateResponse:
    txn_id = str(uuid.uuid4())
    body = payload.model_dump()
    body["txn_id"] = txn_id

    event = build_event(
        event_type=TOPICS["created"],
        txn_id=txn_id,
        card_id=payload.card_id,
        body=body,
    )
    producer.produce(
        topic=TOPICS["created"],
        key=payload.card_id.encode("utf-8"),
        value=to_json_bytes(event),
    )
    producer.flush(timeout=5)

    return TransactionCreateResponse(
        txn_id=txn_id,
        event_type=TOPICS["created"],
        message="transaction accepted for async processing",
    )
