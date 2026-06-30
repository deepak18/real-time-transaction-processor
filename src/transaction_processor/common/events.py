import json
from datetime import datetime, timezone
from typing import Any, Dict


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def to_json_bytes(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def from_json_bytes(payload: bytes) -> Dict[str, Any]:
    return json.loads(payload.decode("utf-8"))


def build_event(event_type: str, txn_id: str, card_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": now_iso(),
        "txn_id": txn_id,
        "card_id": card_id,
        "body": body,
    }

