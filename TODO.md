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
├── docs/
│   └── LEARNING_NOTES.md        # timeless concept notes per technology (Kafka first) — revision reference
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
│       ├── offset_admin.py      # inspect / reset consumer-group offsets (E5 replay tool)
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
| **1. Kafka Core Mastery** | Understand scaling & ordering before adding tech | Partitions, keys, consumer groups, rebalancing, offsets, delivery semantics, DLQ | ✅ (E1–E10 done) |
| **2. Schema & Contracts** | JSON drift breaks consumers silently | Schema Registry + Avro/Protobuf, versioned contracts, compatibility modes | 🚧 (next focus — see §5b) |
| **3. Idempotency & Exactly-Once** | Retries cause double settlement | Idempotent producer, transactions/EOS, idempotency keys, dedup store | ⬜ (planned — see §5b) |
| **4. State & Stream Processing** | Need windowed fraud rules / aggregations | Stateful stream processing — **Apache Flink (PyFlink)** leading option; alts: Kafka Streams, Bytewax, Faust | ⬜ |
| **5. Persistence & Query** | "What's the status of txn X?" needs a queryable store | PostgreSQL (write model / read model), CQRS-lite | ⬜ |
| **6. Caching & Hot State** | Risk lookups hammer the DB | Redis (cache, rate limits, distributed locks) | ⬜ |
| **7. Observability** | Can't see lag/throughput/failures | Metrics (Prometheus), tracing, structured logging, consumer-lag monitoring | ⬜ |
| **8. Read APIs / Aggregation** | Clients need flexible queries over many entities | GraphQL gateway over read models | ⬜ |
| **9. Document/Event Store** | Flexible audit & event sourcing | MongoDB for audit/event store | ⬜ |
| **10. Scale & Resilience** | Single broker = SPOF; uneven load | Multi-broker cluster, replication, partition rebalancing, backpressure | ⬜ |

> **The roadmap is intentionally flexible.** Order can change based on what problem we hit
> next. Always tie the next phase to an observed problem and explain the reasoning.

---

## 5. Phase 1 — Kafka Core Concepts (COMPLETE ✅)

Phase 1 is done — all experiments E1–E10 have been run on the pipeline. This section is kept as
a reference for the concepts and experiments. The active work has moved to **§5b (Phase 2)**.

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

- **E2 — Scale a consumer group** ✅ (done — see Findings Log)
  - Start **two** `risk_worker` instances (same group `risk-cg`).
  - Watch partitions get split between them; kill one and watch a **rebalance** reassign
    partitions to the survivor. Note the brief pause.

- **E3 — Over-provision consumers** ✅ (done — see Findings Log)
  - Start **4** `risk_worker` instances on a 3-partition topic.
  - Observe that the 4th consumer stays **idle** (more consumers than partitions = waste).
  - Lesson: partition count is the ceiling on consumer parallelism.

- **E4 — Separate consumer groups (pub/sub)** ✅ (done — see Findings Log)
  - Confirm `audit-cg` and `risk-cg` both receive `txn.created`-derived events independently
    (different offsets, no competition).

- **E5 — Offset reset / replay** ✅ (done — see Findings Log)
  - A group's committed offset is **server-side state**; `auto.offset.reset` only applies when a
    group has **no** committed offset. Use `services/offset_admin.py` to inspect and reset it.
  - Steps: (1) run the pipeline so `risk-cg` commits offsets; (2) **stop** `risk_worker`;
    (3) `offset_admin --group risk-cg` to SHOW offsets/lag; (4) `--reset earliest` to rewind;
    (5) restart `risk_worker` and watch it **replay** all history. Contrast `--reset latest`
    (skip backlog → only new messages).
  ```powershell
  # inspect current position + lag
  python -m src.transaction_processor.services.offset_admin --group risk-cg --topic txn.created
  # rewind to replay everything (stop risk_worker first)
  python -m src.transaction_processor.services.offset_admin --group risk-cg --reset earliest
  # or fast-forward to skip the backlog
  python -m src.transaction_processor.services.offset_admin --group risk-cg --reset latest
  ```

- **E6 — At-least-once proof (duplicate delivery)** ✅ (done — see Findings Log)
  - Env-guarded fault hook `RISK_WORKER_CRASH_AFTER_PRODUCE=1` raises in `risk_worker` *after*
    producing/flushing `txn.risk_scored` but *before* `consumer.commit(...)`. Restart (hook off)
    and observe the **duplicate** `txn.risk_scored` for the same `txn_id`.
  - This motivates Phase 3 (idempotency/EOS). See `docs/LEARNING_NOTES.md` §1.6 for run/validate.

- **E7 — Poison message → DLQ** ✅ (done — see Findings Log)
  - Inject a malformed event onto `txn.created` with
    `producer_simulator --poison N [--poison-kind missing-field|bad-json]`.
  - Confirm it lands in `txn.dlq` (now with `txn_id`/source coordinates) and the worker keeps
    processing subsequent messages. See `docs/LEARNING_NOTES.md` §1.7.

- **E8 — Increase partitions** ✅ (done — see Findings Log)
  - Recreate a topic with more partitions (or use `topic_admin --alter`), reproduce load, and observe
    higher consumer parallelism. Discuss why you **can't reduce** partitions and how it affects
    key→partition mapping (re-keying risk).

- **E9 — Consumer lag under load** ✅ (done — see Findings Log)
  - Add a `time.sleep()` in a worker (`RISK_WORKER_PROCESS_DELAY_MS`) to simulate slow processing;
    drive high producer rate; watch **lag** grow in Kafka UI / `offset_admin`. Discuss backpressure
    strategies.

- **E10 — `acks` & durability (conceptual + config)** ✅ (done — see Findings Log)
  - Experiment with producer `acks=0|1|all` and discuss data-loss trade-offs (full durability
    benefits need `replication.factor > 1`, which arrives in Phase 10).

### 5.3 Phase 1 exit criteria (definition of "done")
- Can explain, with examples from this repo, how keys map to partitions and why ordering holds.
- Can demonstrate scaling, rebalancing, and the consumer≤partition rule live.
- Can articulate at-least-once vs. exactly-once and point to the exact code line that
  determines the semantics (the `consumer.commit(...)` placement).
- Can reproduce and explain the DLQ flow and consumer lag.

---

## 5b. Phase 2 & 3 — Detailed Plan (next focus) 🚧

> These two phases are tightly linked: Phase 2 makes the **event contract** explicit and safe to
> evolve; Phase 3 makes **processing** safe to retry/replay. Both build directly on Phase 1.

### Phase 2 — Schema & Contracts (Schema Registry)
**Trigger problem to demonstrate FIRST (problem-first rule):** today every event is a hand-built
dict serialized with `json.dumps` in `common/events.py`. Nothing enforces shape or type. To make
the pain real: change the producer's payload (rename `amount` → `amount_cents`, or change a type),
and watch a downstream worker silently mis-handle it or `KeyError` at runtime — a *silent contract
break* that only fails in production.

**Technology introduced:** **Confluent Schema Registry** + a schema'd serialization format.
- Add `schema-registry` to `docker-compose.yaml` (project-scoped, e.g. `rtp-schema-registry`).
- Define a schema per event type (start with `txn.created`, then the rest).
- Switch producers/consumers to Schema-Registry-aware (de)serializers; the registry stores schema
  versions and assigns a schema **ID** embedded in each message.
- Enforce a **compatibility mode** (`BACKWARD` recommended) so incompatible changes are *rejected
  at publish time*, not discovered downstream.

**Format decision (compare):**
- **Avro** — Kafka-classic, compact, great schema-evolution story, first-class registry support. *Default recommendation.*
- **Protobuf** — strong typing, polyglot/gRPC synergy, also registry-supported.
- **JSON Schema** — keeps human-readable JSON; least efficient, easiest migration from today's JSON.

**Concepts to learn:** schema evolution, compatibility modes (BACKWARD/FORWARD/FULL), subject
naming strategies, schema IDs on the wire, why the value of schemas grows with the number of
independent consumers.

**Experiments (P2):**
- **P2-E1** Register `txn.created` schema; produce/consume via Avro; inspect the schema in the registry.
- **P2-E2** Make a **backward-compatible** change (add an optional field with default) → succeeds; old consumers still read.
- **P2-E3** Make a **breaking** change (remove/rename a required field) → registry **rejects** it under BACKWARD. This is the "problem solved" moment.
- **P2-E4** Consumer reads a newer-schema message with an older schema (demonstrate resolution).

**Exit criteria:** can explain compatibility modes and show a breaking change being rejected before it reaches consumers.

### Phase 3 — Idempotency & Exactly-Once (EOS)
**Trigger problem (already demonstrated in E6):** at-least-once + retries/replay ⇒ **duplicate
`txn.risk_scored`** ⇒ in a real system, **double settlement**. We have the proof from E6; now we fix it.

**Three layers (teach the distinction — this is the key insight):**
1. **Idempotent producer** (`enable.idempotence=true`, `acks=all`): dedups *producer retry*
   duplicates within a partition via sequence numbers. Cheap, always-on best practice. Does NOT
   solve consumer-side reprocessing.
2. **Kafka transactions / EOS** (`transactional.id`, `begin_transaction` →
   produce → `send_offsets_to_transaction` → `commit_transaction`): makes the **read-process-write**
   loop atomic — the output event and the input offset commit happen together or not at all. The
   E6 crash would then yield **no** duplicate.
3. **Application-level idempotency keys / dedup store:** EOS only covers **Kafka→Kafka**. Any
   **external** side effect (settlement, DB write, calling a payment rail) still needs an
   idempotency key + a dedup table so a retry is a no-op. This naturally introduces the need for
   **PostgreSQL** (Phase 5) as the dedup/store of record.

**Experiments (P3):**
- **P3-E1** Turn on the idempotent producer; confirm config and acks semantics.
- **P3-E2** Wrap `risk_worker` in a Kafka transaction; re-run the E6 crash; show the duplicate is **gone**.
- **P3-E3** Show EOS does NOT cover an external effect: simulate a settlement side effect and prove a replay double-applies it WITHOUT an idempotency key; then add a dedup key (in-memory first) and show it becomes safe → motivates Phase 5 Postgres.

**Exit criteria:** can explain idempotent-producer vs transactions vs app-level idempotency, and
point to where each is (and is NOT) sufficient; the E6 duplicate is eliminated for the Kafka→Kafka path.

### Phase 4 note — is Apache Flink a good fit?
**Yes.** For windowed/stateful fraud rules (velocity checks, "N txns per card per 5 min", rolling
aggregates), **Flink is an excellent, industry-standard fit**: event-time processing with
watermarks, keyed state, windowing, CEP for fraud patterns, and exactly-once via checkpointing.
Trade-off: it's a heavier system (JobManager/TaskManager) and its richest ecosystem is JVM.
**Recommendation:** keep it Python-first with **PyFlink** (real Flink, Python API) to learn the
canonical concepts that transfer everywhere; lighter Python-native alt is **Bytewax**; **Kafka
Streams** is JVM-only; **Faust** is Python but less actively maintained. As always, demonstrate the
problem first (a naive in-memory windowed counter that loses state on restart and can't scale),
*then* introduce Flink to solve it.

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
1. ✅ **Phase 1 complete** — all Kafka core experiments E1–E10 done (findings in §8).
2. **Start Phase 2 (Schema & Contracts)** per §5b: FIRST demonstrate a silent JSON contract break,
   THEN add Confluent Schema Registry + Avro and enforce `BACKWARD` compatibility (P2-E1…E4).
3. **Then Phase 3 (Idempotency/EOS)** per §5b: idempotent producer → Kafka transactions (eliminate
   the E6 duplicate) → app-level idempotency keys (motivates Phase 5 PostgreSQL).
4. Keep `README.md`, this roadmap, and `docs/LEARNING_NOTES.md` in sync after each phase.

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

- [E2] 2026-06-30 — Added `on_assign`/`on_revoke` rebalance callbacks and a per-process
  `WORKER_ID` to `risk_worker`, then ran two instances against the 3-partition `txn.created`.
  — With both alive: instance #1 owned `txn.created#2`; instance #2 owned `txn.created#0` and
  `#1` (3 partitions split with no overlap). On killing instance #2, the survivor first got
  `#2` **REVOKED**, then **ASSIGNED** all three (`#0, #1, #2`) — the classic *eager* rebalance
  (revoke-all-then-reassign). — Takeaway: each partition is owned by exactly one consumer in a
  group; adding/removing members auto-redistributes partitions via a rebalance, and the default
  eager protocol briefly revokes everything (stop-the-world pause) before reassigning.

- [E2-followup] 2026-06-30 — Made the consumer assignor env-driven
  (`KAFKA_PARTITION_ASSIGNMENT_STRATEGY`, default `range,roundrobin` = eager) and re-ran the
  two-instance test with `cooperative-sticky`. — Instance #1 started owning all three
  (`#0, #1, #2`). When instance #2 joined, #1 logged only `REVOKED -> #0` (it **kept** `#1`/`#2`
  the whole time) and #2 ended up `ASSIGNED -> #0`. Only **one** partition moved; the other two
  never paused. Contrast with the eager run where the survivor revoked *all three* first. —
  Takeaway: cooperative-sticky rebalances are **incremental** — only partitions that actually
  change hands are revoked, so unaffected partitions keep processing (much smaller pause). This
  is the preferred assignor for large/frequently-redeployed consumer groups. Deep-dive notes and
  the eager-vs-cooperative comparison live in `docs/LEARNING_NOTES.md`.

- [E3] 2026-06-30 — Started **four** `risk_worker` instances against the 3-partition
  `txn.created` (cooperative-sticky assignor still set from the E2 follow-up). — As each instance
  joined, exactly **one** partition migrated incrementally: instance #1 began with `#0,#1,#2`,
  handed `#0` to instance #2, then `#1` to instance #3, and kept `#2`. The **4th** instance
  (`risk-15316`) only ever logged `ASSIGNED -> <none>` and processed zero messages. Final split:
  `#2 / #0 / #1 / <none>`. — Takeaway: a partition is owned by exactly one consumer in a group,
  so **partition count is the hard ceiling on parallelism** — extra consumers sit idle as hot
  standby (assignor-independent: the 4th is idle under both eager and cooperative). To scale past
  3 you must add partitions (E8), which can't later be reduced.

- [E4] 2026-06-30 — Ran `risk_worker` (group `risk-cg`) and `audit_worker` (group `audit-cg`)
  together while producing to `txn.created`. — For the same `txn_id`
  (`94412b9f-…`), `risk_worker` logged `risk scored … partition=2` while `audit_worker`
  independently logged **both** `topic=txn.created partition=2` and (after the chain advanced)
  `topic=txn.risk_scored partition=2`. The two groups consumed the same event with their **own
  offsets**, neither stealing from the other. — Takeaway: different `group.id`s each get a full,
  independent copy of the stream (pub/sub), whereas members of one group divide partitions. This
  is how you bolt on new consumers (audit, analytics, fraud) without touching existing ones. Also
  note both records sat on partition 2 — same `card_id` key → same partition across topics.

- [E5] 2026-06-30 — Added `services/offset_admin.py` (show/reset group offsets) and exercised
  replay on `risk-cg`/`txn.created`. — Start: caught up (`committed` = `high`, `total_lag=0`).
  `--reset earliest` moved committed to the **low watermarks (20/40/40, not 0)** and lag jumped to
  **301** (54+123+124) — restarting the worker replayed that history. `--reset latest` moved
  committed back to the **high watermarks**, `total_lag=0`, so a restart processes only new
  messages. — Takeaways: (1) offsets are durable, **per-group, server-side** state; resetting one
  group doesn't touch others. (2) `auto.offset.reset` is only a *bootstrap* fallback; replay
  requires explicitly rewinding committed offsets. (3) **You can only rewind as far back as the
  low watermark** — older records (offsets 0–19 / 0–39) had already aged out via retention, which
  why replay started at 20/40/40. (4) Replay under at-least-once re-emits duplicates downstream
  → motivates idempotency (Phase 3). Concept notes in `docs/LEARNING_NOTES.md`.

- [E6] 2026-06-30 — Added an env-guarded fault hook to `risk_worker`
  (`RISK_WORKER_CRASH_AFTER_PRODUCE=1`) that flushes the produced `txn.risk_scored` then raises
  `_FaultInjected` **before** `consumer.commit(...)`; the hook re-raises past the DLQ handler so
  the offset is never committed. Produced one txn and ran the worker with the hook ON, then
  restarted with it OFF. — Run 1 (`risk-22668`) logged `E6 fault: produced txn.risk_scored for
  txn=deed6e5a-… but crashing BEFORE commit` and died (offset uncommitted). Run 2 (`risk-9364`)
  re-read the same `txn.created` and logged `risk scored txn=deed6e5a-… partition=2 score=35`.
  `audit_worker` recorded the proof: **two** `txn.risk_scored` for the same `txn_id` at
  **partition=2 offset=412 and offset=413** (source `txn.created` read once at offset 164). —
  Takeaways: (1) commit-after-process = **at-least-once**: a crash in the produce→commit gap
  re-delivers the source and **re-emits** the downstream event (duplicate, never lost). (2) The
  duplicate `txn_id` is identical, so safe reprocessing must dedup on a **stable id**, not on
  delivery count. (3) Both duplicates landed on the same partition (same `card_id` key → ordering
  preserved even across the crash). (4) This is the exact double-settlement failure mode that
  motivates **Phase 3 (idempotency/EOS)**. Concept notes in `docs/LEARNING_NOTES.md`.

- [E8] 2026-06-30 — Extended `topic_admin` with `--describe` and `--alter NAME --partitions N`,
  then grew `txn.created` from **3 → 6** partitions and re-ran `producer_simulator` to compare the
  `card_id → partition` map before/after. — Before (mod 3): card-1→2, card-2→1, card-3→0, card-4→1,
  card-5→2. After (mod 6): card-1→2, card-2→**4**, card-3→0, card-4→1, card-5→**5**. So **2 of 5
  cards remapped** (card-2, card-5) while the other three stayed. The partitioner is
  `murmur2(card_id) % N`: with a fixed hash `h`, a key stays put when `h%6 == h%3` and moves (to
  `h%3 + 3`) otherwise — about half of keys move when doubling. Shrinking back was rejected:
  `INVALID_PARTITIONS … 3 would not be an increase`. — Takeaways: (1) adding partitions raises the
  parallelism ceiling (now up to 6 `risk_worker`s do work) but (2) **changes key→partition mapping
  for future records**, so a card's new events can land on a different partition than its history —
  **breaking per-card ordering** across the change. (3) Partitions can only **grow**, never shrink,
  so size deliberately up front. Concept notes in `docs/LEARNING_NOTES.md`.

- [E9] 2026-06-30 — Added an env-guarded slow-processing knob to `risk_worker`
  (`RISK_WORKER_PROCESS_DELAY_MS`) that sleeps per message, then ran a fast producer against
  1/2/3 slow consumers on the 6-partition `txn.created` and watched lag via `offset_admin`. —
  With 3 consumers sharing 6 partitions, a sample read was: p0 lag=26, p1 lag=46, p2 lag=45,
  p3 `committed=<none>` lag=0 (no records routed there that run), p4 lag=44, p5 lag=27 →
  **total_lag=188**. Adding consumers split partitions among them and they drained in parallel;
  stopping the producer let lag fall as the backlog cleared. — Takeaways: (1) **lag = high −
  committed** is the core health signal — it grows whenever ingest rate > processing rate.
  (2) Backpressure remedies: scale consumers up to the partition ceiling, add partitions to raise
  that ceiling (E8), speed up per-message work, or batch/shed load. (3) Under at-least-once a
  backlog means slower recovery, not loss. (4) An idle partition (p3) contributes zero lag and
  zero work — skew leaves some consumers hot and others idle. Concept notes in
  `docs/LEARNING_NOTES.md`.

- [E10] 2026-06-30 — Made the producer ack level env-driven (`KAFKA_PRODUCER_ACKS`, default `all`)
  via `common/config.py` + `kafka_client.producer_config()`, and had `producer_simulator` print
  the active mode at startup. Ran with `acks=all`, `acks=1`, and `acks=0`. — All three produced
  successfully against the local single broker; the startup line confirmed the mode each run.
  Because the cluster runs `replication.factor=1` (no followers), `acks=all` and `acks=1` behave
  **identically** today — the leader is the only replica, so "all in-sync replicas" *is* the
  leader. `acks=0` returns without waiting for any broker ack (fastest, but a loss at the wrong
  moment is invisible to the delivery report). — Takeaways: (1) `acks` trades **latency vs
  durability**: 0 = fire-and-forget, 1 = leader persisted, all = leader + all ISR. (2) The real
  durability win of `acks=all` only materializes with **`replication.factor > 1`** plus
  `min.insync.replicas` — there's nothing to replicate to yet. (3) This sets up **Phase 10
  (multi-broker)** where we raise RF and can actually demonstrate fault-tolerant durability.
  Concept notes in `docs/LEARNING_NOTES.md`.

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
