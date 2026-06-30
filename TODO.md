# Real-Time Transaction Processor — Project Guide & Learning Roadmap

> **Purpose of this file:** This is the single source of truth for the project. It is written
> so that a **human** or a **new AI assistant** can read it cold and immediately understand
> (1) what the project is, (2) why it exists, (3) what is already built, (4) where we are on
> the roadmap, and (5) what to do next. If you are an AI picking this up in a fresh chat,
> read the whole file top-to-bottom before making changes, then continue from the
> **"Current State"** and **"Next Actions"** sections.

---

## 1. Project Goal & Learning Philosophy

**Goal:** Learn distributed-systems engineering by building a realistic **fintech real-time
transaction processing pipeline**, starting with **Apache Kafka** and adding **one technology
at a time**. Each new technology is introduced only when the project hits a concrete,
real-world problem that the technology naturally solves. This mirrors how real engineering
teams evolve their architecture.

**Learning principles (important — follow these when teaching/continuing):**
- **Hand-hold and explain reasoning.** Every step explains *why*, not just *how*.
- **Show alternatives.** For each major decision, briefly compare with at least one alternative
  and explain the trade-offs.
- **Problem-first scaling.** Do not add tech for its own sake. First create/observe a problem,
  then introduce the technology that solves it.
- **Keep the big picture.** Answer side questions, but always steer back to the overall roadmap.
- **Production-grade structure.** Code is organized like a real product, not loose scripts.

**Domain chosen:** Card/payment **transaction authorization & settlement**. This domain is
rich enough to demonstrate ordering guarantees, idempotency, fraud/risk scoring, auditing,
and settlement — all natural fits for streaming concepts.

---

## 2. High-Level Architecture (v1 — current)

A transaction flows through a chain of independent workers, each consuming one topic and
producing the next. This is an **event-driven choreography** pattern.

```
Client / Simulator
      │  POST /v1/transactions
      ▼
┌─────────────┐   txn.created    ┌─────────────┐   txn.risk_scored   ┌──────────────┐
│ FastAPI     │ ───────────────▶ │ Risk Worker │ ──────────────────▶ │ Decision     │
│ (ingress)   │                  │             │                     │ Worker       │
└─────────────┘                  └─────────────┘                     └──────┬───────┘
                                                                            │
                              txn.authorized / txn.declined                 │
                                            ┌───────────────────────────────┘
                                            ▼
                                   ┌──────────────────┐  txn.settlement.initiated   ┌──────────┐
                                   │ Settlement Worker │ ──────────────────────────▶ │ (settled)│
                                   └──────────────────┘        txn.settled          └──────────┘

   Audit Worker  ── consumes ALL lifecycle topics in its own consumer group (audit-cg)
   DLQ (txn.dlq) ── any worker routes un-processable messages here
```

**Event flow (happy path):**
`txn.created → txn.risk_scored → txn.authorized | txn.declined → txn.settlement.initiated → txn.settled`

**Why this shape?** It lets us demonstrate Kafka core concepts naturally:
- **Partitioning + keys** → ordering per `card_id`.
- **Consumer groups** → scaling each worker independently + a separate audit group reading the same data.
- **DLQ** → poison-message handling.
- **Topic-per-event-type** → clear contracts between services.

**Alternative considered:** A single "orchestrator" service (orchestration pattern) instead of
choreography. Rejected for v1 because choreography exposes more Kafka concepts and decoupling;
orchestration may be revisited later when we add workflow/state complexity.

---

## 3. Current State (what is already built) ✅

> Tech stack today: **Python 3.10+**, **confluent-kafka**, **FastAPI + Uvicorn**, **Kafka in KRaft mode** (no ZooKeeper) via Docker Compose, **Kafka UI** for inspection.

### Project layout
```
real-time-transaction-processor/
├── docker-compose.yaml          # Kafka (KRaft) + Kafka UI, project-scoped container/volume
├── pyproject.toml               # packaging (src layout)
├── requirements.txt             # confluent-kafka, fastapi, uvicorn, pydantic
├── README.md                    # setup + run instructions
├── TODO.md                      # THIS FILE — roadmap & project guide
├── src/transaction_processor/
│   ├── common/                  # cross-cutting shared code
│   │   ├── config.py            # Settings dataclass (env-driven): bootstrap, partitions, RF, etc.
│   │   ├── topics.py            # TOPICS dict + GROUP_IDS dict (single source for names)
│   │   ├── events.py            # event envelope builder + JSON (de)serialization helpers
│   │   ├── kafka_client.py      # producer_config() / consumer_config() factories
│   ├── domain/
│   │   └── risk_engine.py       # pure functions: score_transaction(), authorize_from_score()
│   ├── api/
│   │   ├── main.py              # FastAPI app: GET /health, POST /v1/transactions → txn.created
│   │   └── schemas.py           # Pydantic request/response models
│   └── services/                # long-running workers + admin tools
│       ├── topic_admin.py       # creates all topics with configured partitions/RF
│       ├── producer_simulator.py# CLI to generate synthetic transactions
│       ├── risk_worker.py       # txn.created → txn.risk_scored
│       ├── decision_worker.py   # txn.risk_scored → txn.authorized | txn.declined
│       ├── settlement_worker.py # txn.authorized → txn.settlement.initiated → txn.settled
│       └── audit_worker.py      # consumes all lifecycle topics (separate group)
└── tests/
    └── smoke_test.py            # lightweight local logic test (no Kafka required)
```

### Key implementation facts an AI should know
- **Event envelope** (`events.build_event`) is standardized:
  `{ event_type, event_version, occurred_at, txn_id, card_id, body }`.
  Serialized with `json.dumps(..., sort_keys=True)` for deterministic bytes.
- **Partition key = `card_id`** everywhere (producer `key=card_id`). This guarantees
  per-card ordering across the pipeline. Remember this when reasoning about ordering.
- **Manual offset commits**: consumers use `enable.auto.commit=False` and commit
  **after** successful processing (at-least-once semantics). See `risk_worker.py`.
- **DLQ pattern**: on processing exception, the worker publishes to `txn.dlq` and still
  commits the offset so the bad message doesn't block the partition.
- **Config is env-driven** via `common/config.py` (`KAFKA_BOOTSTRAP_SERVERS`,
  `KAFKA_TOPIC_PARTITIONS` default 3, `KAFKA_REPLICATION_FACTOR` default 1,
  `SETTLEMENT_DELAY_SECONDS`).
- **Topics & consumer groups** are centralized in `common/topics.py`:
  - Topics: `txn.created`, `txn.risk_scored`, `txn.authorized`, `txn.declined`,
    `txn.settlement.initiated`, `txn.settled`, `txn.dlq`.
  - Groups: `risk-cg`, `decision-cg`, `settlement-cg`, `audit-cg`.
- **Docker**: project-scoped names to avoid clashing with the sibling `kafka-streaming-orders`
  project — container `rtp-kafka`, volume `rtp_kafka_data`, UI `rtp-kafka-ui` on `:8080`.
  `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` (topics are created explicitly via `topic_admin.py`).
- **Note:** `lock_manager.py` and `signature_utils.py` in `common/` are leftover coding-exercise
  scratch files and are **not** part of the transaction pipeline. They can be removed/relocated.

### How to run (summary; full details in README.md)
```powershell
docker compose up -d
python -m src.transaction_processor.services.topic_admin
# then run each worker in its own terminal:
python -m src.transaction_processor.services.risk_worker
python -m src.transaction_processor.services.decision_worker
python -m src.transaction_processor.services.settlement_worker
python -m src.transaction_processor.services.audit_worker
# ingress API:
uvicorn src.transaction_processor.api.main:app --host 0.0.0.0 --port 8000
# generate load:
python -m src.transaction_processor.services.producer_simulator --count 30 --sleep-ms 50
```

---

## 4. Learning Roadmap (phases)

Each phase introduces ONE new capability/technology, triggered by a concrete problem.
Status legend: ✅ done · 🚧 in progress · ⬜ not started.

| Phase | Trigger problem | Technology / concept introduced | Status |
|------|------------------|----------------------------------|--------|
| **0. Foundations** | Need an entry point + async backbone | Kafka (KRaft), FastAPI ingress, worker chain, DLQ, src layout, Docker | ✅ |
| **1. Kafka Core Mastery** | Understand scaling & ordering before adding tech | Partitions, keys, consumer groups, rebalancing, offsets, delivery semantics, DLQ | 🚧 (see §5 — focus area) |
| **2. Schema & Contracts** | JSON drift breaks consumers silently | Schema Registry + Avro/Protobuf, versioned contracts, compatibility modes | ⬜ |
| **3. Idempotency & Exactly-Once** | Retries cause double settlement | Idempotent producer, transactions/EOS, idempotency keys, dedup store | ⬜ |
| **4. State & Stream Processing** | Need windowed fraud rules / aggregations | Stateful processing (e.g., Faust/Kafka Streams concepts), state stores | ⬜ |
| **5. Persistence & Query** | "What's the status of txn X?" needs a queryable store | PostgreSQL (write model / read model), CQRS-lite | ⬜ |
| **6. Caching & Hot State** | Risk lookups hammer the DB | Redis (cache, rate limits, distributed locks) | ⬜ |
| **7. Observability** | Can't see lag/throughput/failures | Metrics (Prometheus), tracing, structured logging, consumer-lag monitoring | ⬜ |
| **8. Read APIs / Aggregation** | Clients need flexible queries over many entities | GraphQL gateway over read models | ⬜ |
| **9. Document/Event Store** | Flexible audit & event sourcing | MongoDB for audit/event store | ⬜ |
| **10. Scale & Resilience** | Single broker = SPOF; uneven load | Multi-broker cluster, replication, partition rebalancing, backpressure | ⬜ |

> **The roadmap is intentionally flexible.** Order can change based on what problem we hit
> next. Always tie the next phase to an observed problem and explain the reasoning.

---

## 5. Phase 1 — Kafka Core Concepts (current focus) 🚧

This is where we are now. The goal is to deeply understand Kafka's building blocks **by
experimenting on the existing pipeline**. For each concept below: read the explanation,
run the experiment, observe the result (use Kafka UI on `http://localhost:8080`), and note
findings.

### 5.1 Concepts to learn
1. **Topics & partitions** — a topic is split into partitions; partitions are the unit of
   parallelism and ordering. Ordering is guaranteed **only within a partition**.
2. **Producer keys & partitioning** — same key → same partition (default murmur2 hashing).
   We key by `card_id` to keep per-card events ordered.
3. **Consumer groups** — consumers in the same `group.id` share partitions (each partition is
   read by exactly one consumer in the group). Different groups each get a full copy
   (pub/sub). This is why `audit-cg` sees everything independently of `risk-cg`.
4. **Rebalancing** — when consumers join/leave, partitions are reassigned. Understand
   eager vs. cooperative-sticky assignors and the cost of rebalances.
5. **Offsets & commit strategies** — auto vs. manual commit; commit-before vs.
   commit-after processing. Current code commits **after** success → at-least-once.
6. **Delivery semantics** — at-most-once, at-least-once (current), exactly-once (future phase).
7. **Replication & ISR** — leader/followers, `replication.factor` (currently 1 → no fault
   tolerance, intentional for local dev), in-sync replicas, `acks`.
8. **Retention & compaction** — time/size retention vs. log-compacted topics (useful later
   for "latest state per key").
9. **Dead-letter queue (DLQ)** — isolating poison messages so one bad event can't stall a
   partition.
10. **Backpressure & lag** — consumer lag as the core health signal of a streaming system.

### 5.2 Experiments to run (hands-on)
> Tip: use the Kafka UI to watch partitions, consumer-group assignments, and lag while doing these.

- **E1 — Partition fan-out & ordering** ✅ (done — see Findings Log)
  - Produce many transactions across several `card_id`s with `producer_simulator`.
  - Observe how messages spread across the 3 partitions of `txn.created`.
  - Verify all events for one `card_id` land on the **same** partition (ordering proof).

- **E2 — Scale a consumer group**
  - Start **two** `risk_worker` instances (same group `risk-cg`).
  - Watch partitions get split between them; kill one and watch a **rebalance** reassign
    partitions to the survivor. Note the brief pause.

- **E3 — Over-provision consumers**
  - Start **4** `risk_worker` instances on a 3-partition topic.
  - Observe that the 4th consumer stays **idle** (more consumers than partitions = waste).
  - Lesson: partition count is the ceiling on consumer parallelism.

- **E4 — Separate consumer groups (pub/sub)**
  - Confirm `audit-cg` and `risk-cg` both receive `txn.created`-derived events independently
    (different offsets, no competition).

- **E5 — Offset reset / replay**
  - Stop a worker, delete/reset its group offsets, restart with `auto.offset.reset=earliest`,
    and watch it **replay** history. Contrast with `latest`.

- **E6 — At-least-once proof (duplicate delivery)**
  - Add an artificial crash *after* producing the output event but *before* committing the
    offset in `risk_worker`. Restart and observe the **duplicate** `txn.risk_scored`.
  - This motivates Phase 3 (idempotency/EOS).

- **E7 — Poison message → DLQ**
  - Produce a malformed event onto `txn.created` (e.g., missing `amount`).
  - Confirm it lands in `txn.dlq` and the worker keeps processing subsequent messages.

- **E8 — Increase partitions**
  - Recreate a topic with more partitions (or use `topic_admin`), reproduce load, and observe
    higher consumer parallelism. Discuss why you **can't reduce** partitions and how it affects
    key→partition mapping (re-keying risk).

- **E9 — Consumer lag under load**
  - Add a `time.sleep()` in a worker to simulate slow processing; drive high producer rate;
    watch **lag** grow in Kafka UI. Discuss backpressure strategies.

- **E10 — `acks` & durability (conceptual + config)**
  - Experiment with producer `acks=0|1|all` and discuss data-loss trade-offs (full durability
    benefits need `replication.factor > 1`, which arrives in Phase 10).

### 5.3 Phase 1 exit criteria (definition of "done")
- Can explain, with examples from this repo, how keys map to partitions and why ordering holds.
- Can demonstrate scaling, rebalancing, and the consumer≤partition rule live.
- Can articulate at-least-once vs. exactly-once and point to the exact code line that
  determines the semantics (the `consumer.commit(...)` placement).
- Can reproduce and explain the DLQ flow and consumer lag.

---

## 6. Conventions & Decisions (locked)
- **src layout** packaging with explicit module execution (`python -m ...`).
- **Topic & group names** only via `common/topics.py` — never hard-code strings elsewhere.
- **Event envelope** is mandatory for all events (`common/events.build_event`).
- **Partition key = `card_id`** for all transaction events.
- **Manual commits, commit-after-success** → at-least-once until Phase 3.
- **FastAPI** is the synchronous ingress; all heavy work is async via Kafka.
- **Docker resources are project-scoped** (`rtp-*`); do not share containers/volumes with the
  sibling `kafka-streaming-orders` project.
- **Auto topic creation disabled**; topics created explicitly via `topic_admin.py`.

---

## 7. Next Actions (pick up here) ▶️
1. **Finish Phase 1 experiments E1–E10** and record findings (add entries to §8).
2. Decide the **Phase 2 trigger**: introduce Schema Registry once we intentionally break a
   consumer with a JSON shape change (demonstrate the problem first).
3. Keep the README run-book and this roadmap in sync after each phase.

---

## 8. Findings Log (append as you experiment)
> Format: `- [E#] date — what you changed — what you observed — takeaway`

- [E1] 2026-06-29 — Added a delivery-report callback to `producer_simulator` that prints
  each record's partition/offset, a `card_id → partition` map, and a fan-out summary.
  Ran `--count 30` with 5 cards over 3 partitions. — Observed 6 records/card (even per key)
  but skewed per partition: **p0=6 (1 card), p1=12 (2 cards), p2=12 (2 cards)**. Mapping is
  deterministic across re-runs. — Takeaway: same key → same partition gives per-card ordering,
  but with few keys the load is unavoidably skewed (1/2/2 split). Even key distribution ≠ even
  partition load; this is the hot-partition tension. Confirmed in Kafka UI.

---

## 9. Notes for a New AI Assistant (read me)
- This repo is a **teaching project**: prioritize clear explanations, reasoning, and
  alternative comparisons over terse changes.
- Before coding, confirm **which phase** we're in (see §4 status + §7 Next Actions).
- Do **not** introduce a new technology unless we've first demonstrated the problem it solves.
- Respect the **locked conventions** in §6.
- The sibling project `kafka-streaming-orders` is **separate** — do not couple them.
- Keep changes production-grade and within the established `src/transaction_processor/...`
  structure; avoid dropping loose scripts into the package.
