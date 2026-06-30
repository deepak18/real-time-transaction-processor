# Learning Notes — Distributed Systems by Building

> **What this file is:** a single, durable **reference/revision** doc for the *concepts* we learn
> while building this project — one technology at a time (Kafka first, then Schema Registry,
> idempotency/EOS, PostgreSQL, Redis, observability, GraphQL, MongoDB, multi-broker scaling).
>
> **How it differs from the other docs:**
> - `README.md` = how to **run** the project (runbook).
> - `TODO.md` = **roadmap, current state, next actions, and a chronological Findings Log**.
> - `LEARNING_NOTES.md` (this file) = **timeless concept notes** organized by technology and
>   concept, written so you can revise quickly without re-reading the experiment history.
>
> **Per-concept template** (used throughout): **Concept → How it works → How to test →
> How to validate → Why it's helpful → Gotchas**. Experiment IDs (E1, E2, …) cross-reference
> the Findings Log in `TODO.md`.

---

## Table of Contents
- [Part 1 — Apache Kafka](#part-1--apache-kafka)
  - [1.0 Mental model & vocabulary](#10-mental-model--vocabulary)
  - [1.1 Partitioning & message keys (E1)](#11-partitioning--message-keys-e1)
  - [1.2 Consumer groups & horizontal scaling (E2, E3)](#12-consumer-groups--horizontal-scaling-e2-e3)
  - [1.3 Rebalancing: eager vs cooperative-sticky (E2 follow-up)](#13-rebalancing-eager-vs-cooperative-sticky-e2-follow-up)
  - [1.4 Config & command cheat-sheet](#14-config--command-cheat-sheet)
  - [1.5 Offsets, commits & replay (E5)](#15-offsets-commits--replay-e5)

---

# Part 1 — Apache Kafka

## 1.0 Mental model & vocabulary

**Concept.** Kafka is a distributed, append-only **commit log**. Producers append records to the
end of a log; consumers read forward at their own pace by tracking an **offset**. Nothing is
deleted on read — many independent consumers can read the same data.

**Key terms.**
- **Topic** — a named stream (e.g., `txn.created`). A logical category of events.
- **Partition** — a topic is split into N partitions; each partition is an *ordered, immutable*
  log. **Ordering is guaranteed only within a partition**, never across a whole topic.
- **Offset** — a monotonically increasing position within a partition. A consumer "commits" the
  offset it has processed so it can resume after a restart.
- **Producer** — appends records; may attach a **key** that decides the partition.
- **Consumer** — reads records; usually part of a **consumer group**.
- **Consumer group** (`group.id`) — a set of cooperating consumers that **divide** a topic's
  partitions among themselves. Each partition is owned by exactly one member at a time.
- **Broker** — a Kafka server hosting partitions. We run a single broker locally in **KRaft mode**
  (no ZooKeeper).
- **Replication factor** — copies of each partition across brokers (1 locally = no fault
  tolerance; raised in the multi-broker phase).

**Why it's helpful.** Decouples producers from consumers in time and scale: services don't call
each other directly; they publish facts ("a transaction was created") that any number of
consumers can react to, replay, or audit independently.

---

## 1.1 Partitioning & message keys (E1)

**Concept.** The **partition** is the unit of *both* parallelism and ordering. A producer record
key deterministically maps to a partition, so all records sharing a key land on the same
partition and are processed **in order**. We key every transaction event by **`card_id`** to
preserve per-card ordering (created → scored → authorized → settled).

**How it works.**
- Default partitioner: `partition = murmur2(key) % num_partitions`.
- Same key → same partition (as long as partition count doesn't change).
- No key → records are spread round-robin/sticky across partitions → **ordering per entity is
  lost**.
- `producer.produce()` is *fire-and-forget* into an in-memory queue; the broker ack arrives later
  via a **delivery report callback** (`on_delivery`). That callback is the only reliable place to
  learn the final `partition`/`offset` or detect a delivery failure.

**How to test.** `producer_simulator.py` cycles `card-1..card-5` and prints, from the delivery
callback, a `card_id → partition` map and a per-partition fan-out summary:
```powershell
docker compose up -d
python -m src.transaction_processor.services.topic_admin
python -m src.transaction_processor.services.producer_simulator --count 30 --sleep-ms 20
```

**How to validate.**
- The `card_id → partition` map is **identical across re-runs** (hashing is deterministic).
- Every record for a given card sits on **one** partition — confirm in Kafka UI
  (`http://localhost:8080` → Topics → `txn.created` → Messages, grouped by partition).
- Observed fan-out with 5 keys / 3 partitions: **p0=6, p1=12, p2=12** (a 1/2/2 key split).

**Why it's helpful.** Per-key ordering is exactly what payment processing needs: a card's events
must not be reordered. Keys give you that guarantee *and* horizontal parallelism at the same time.

**Gotchas.**
- **Data skew / hot partitions:** few keys (or one whale card/merchant) overload a single
  partition. Even key distribution ≠ even partition load.
- **Partition count is sticky:** you can add partitions but not remove them, and adding them
  **changes the key→partition mapping** for future records — breaking ordering assumptions. Size
  deliberately up front.
- Calling `produce()` ≠ durably stored. Only the delivery callback (or `flush()`) confirms it.

---

## 1.2 Consumer groups & horizontal scaling (E2, E3)

**Concept.** Consumers sharing one `group.id` form a **consumer group** and split the topic's
partitions among themselves: **each partition is owned by exactly one member**. Add members to
scale out; the group reassigns partitions automatically. Different groups each get their **own
full copy** of the stream (pub/sub) — this is why `audit-cg` sees everything independently of
`risk-cg`.

**How it works.**
- Partitions are distributed across members by the configured **assignor** (see §1.3).
- **Parallelism ceiling = partition count.** With 3 partitions, at most 3 members do work; a 4th
  sits **idle** (hot standby) until a partition frees up.
- We use **manual commits** (`enable.auto.commit=False`) and commit *after* processing →
  **at-least-once** delivery.

**How to test.**
- **E2 (scale):** start two `risk_worker` instances (each prints a per-process `WORKER_ID` and
  its `ASSIGNED`/`REVOKED` partitions), drive load, then `Ctrl+C` one and watch the survivor
  take over.
- **E3 (over-provision):** start **four** instances on the 3-partition topic.
```powershell
# run each in its own terminal
python -m src.transaction_processor.services.risk_worker
# then, in another terminal:
python -m src.transaction_processor.services.producer_simulator --count 60 --sleep-ms 50
```

**How to validate.**
- Two instances → the 3 partitions split with **no overlap** (E2 observed: #1 owned `#2`; #2
  owned `#0,#1`).
- Four instances → exactly **three** own one partition each; the **4th logs `ASSIGNED -> <none>`**
  and processes zero messages (E3 observed final split: `#2 / #0 / #1 / <none>`). The idle 4th is
  **assignor-independent** — it stays idle under both eager and cooperative-sticky, because the
  cause is *partitions < consumers*, not the rebalance protocol.
- Kafka UI → Consumers → `risk-cg` shows members, their partition assignments, and **lag**.
- Kill an active member → a previously idle member immediately picks up the freed partition.
- **Pub/sub across groups (E4):** run `risk_worker` (`risk-cg`) and `audit_worker` (`audit-cg`)
  together — both log the **same** `txn.created` event for the same `txn_id`, each with its own
  offset, neither stealing from the other. Observed: `risk scored … partition=2` alongside
  `audit topic=txn.created partition=2` (and later `topic=txn.risk_scored partition=2`). Adding a
  new `group.id` gives a fresh, independent copy of the stream.

**Why it's helpful.** This is the core mechanism for **scaling stateless workers**: add instances,
Kafka redistributes load — no coordinator, no code change. Separate groups enable independent
fan-out (processing vs. auditing) over the same data.

**Gotchas.**
- More consumers than partitions = **wasted capacity**. To scale further you must add partitions
  (E8), which you can't later shrink.
- A rolling restart of N instances can trigger N rebalances — deploy carefully (see §1.3).

---

## 1.3 Rebalancing: eager vs cooperative-sticky (E2 follow-up)

**Concept.** A **rebalance** is how a group redistributes partitions when members join/leave (or
topic metadata changes). The **assignor** decides *how* that redistribution happens, and it has a
big effect on how much processing pauses.

**How it works.**

| | **Eager** (default `range,roundrobin`) | **Cooperative-sticky** |
|---|---|---|
| Protocol | Single-phase | Two-phase, **incremental** |
| On rebalance | **Every** member revokes **all** its partitions, then receives a fresh assignment | Only the partitions that actually **change owner** are revoked; the rest keep flowing |
| Pause | Stop-the-world for the whole group | Minimal — unaffected partitions never stop |
| Best for | Small/simple groups | Large or frequently-redeployed groups |

In `confluent-kafka`, because our `on_assign`/`on_revoke` callbacks only **log** (they never call
`assign()`), librdkafka performs the correct assignment automatically in **both** modes. *If* we
ever managed offsets inside the callbacks, cooperative mode would require
`incremental_assign()` / `incremental_unassign()` instead of `assign()`.

**How to test.** The assignor is env-driven (`KAFKA_PARTITION_ASSIGNMENT_STRATEGY`). Set it in
**each** worker terminal, then repeat the two-instance E2 flow:
```powershell
$env:KAFKA_PARTITION_ASSIGNMENT_STRATEGY = "cooperative-sticky"
python -m src.transaction_processor.services.risk_worker
```

**How to validate (observed contrast).**
- **Eager (E2):** when instance #2 joined, the existing owner revoked **all three** (`#0,#1,#2`)
  before getting a new subset — full pause.
- **Cooperative (E2 follow-up):** instance #1 started with `#0,#1,#2`; when #2 joined, #1 logged
  only `REVOKED -> #0` (keeping `#1`/`#2` running the whole time) and #2 ended `ASSIGNED -> #0`.
  **Only one partition moved.** The two-phase nature shows up as an interim `ASSIGNED -> <none>`
  before the final assignment settles.

**Why it's helpful.** In production, frequent rebalances (autoscaling, rolling deploys, pod
churn) cause latency spikes under eager assignment because *everything* stops. Cooperative-sticky
shrinks the blast radius to just the partitions that move, keeping the rest of the pipeline live.

**Gotchas.**
- All members of a group should agree on the assignor; mixing eager and cooperative members is
  not supported.
- Cooperative does **two** rebalance rounds to settle — expect a transient `<none>` assignment in
  the logs; that's normal, not a bug.

**Q&A — "Closing instances one at a time didn't reassign the freed partition under
cooperative-sticky. Is that expected?"** Partly. An orphaned partition is *always* reassigned to
a surviving consumer (it can never be left unconsumed). It *looks* like nothing happens when the
instance you close is an **idle standby** (owns 0 partitions → nothing to move). Once you close an
instance that actually owns a partition (so consumers ≤ partitions), a survivor logs exactly one
`ASSIGNED -> #X` while the others keep their partitions untouched (no revoke). Eager looked
different only because it revokes everything from everyone on each change — visible churn — whereas
cooperative moves just the orphan, which is easy to miss. To see it: keep producing load, close an
**active** owner, and watch a survivor print `ASSIGNED -> #X` within ~1s.

---

## 1.4 Config & command cheat-sheet

**Where config lives:** `common/config.py` reads env vars at import time (frozen `Settings`
dataclass); `common/kafka_client.py` builds producer/consumer config dicts; topic and
consumer-group names come only from `common/topics.py`.

**Partition assignment strategy (eager ↔ cooperative).**
```powershell
# Switch the consumer group to cooperative-sticky (set in EVERY worker terminal):
$env:KAFKA_PARTITION_ASSIGNMENT_STRATEGY = "cooperative-sticky"

# Inspect the current value:
$env:KAFKA_PARTITION_ASSIGNMENT_STRATEGY

# Revert to the default eager assignor (remove the override):
Remove-Item Env:KAFKA_PARTITION_ASSIGNMENT_STRATEGY
# ...or simply open a fresh terminal (env vars are per-session).
```
> Default when unset: `range,roundrobin` (eager). Env vars set with `$env:` only apply to the
> current PowerShell session and the processes it launches — set it **before** starting each
> worker.

**Other env knobs (see `common/config.py`).**
| Env var | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address |
| `KAFKA_TOPIC_PARTITIONS` | `3` | Partitions created by `topic_admin` |
| `KAFKA_REPLICATION_FACTOR` | `1` | Replicas per partition (1 = no HA locally) |
| `SETTLEMENT_DELAY_SECONDS` | `1.0` | Simulated settlement delay |
| `KAFKA_PARTITION_ASSIGNMENT_STRATEGY` | `range,roundrobin` | Consumer assignor (eager vs `cooperative-sticky`) |

**Everyday commands.**
```powershell
docker compose up -d                                             # start Kafka (KRaft) + Kafka UI
python -m src.transaction_processor.services.topic_admin         # create topics
python -m src.transaction_processor.services.risk_worker         # run a worker (repeat per terminal)
python -m src.transaction_processor.services.producer_simulator --count 60 --sleep-ms 50
# Kafka UI: http://localhost:8080  (partitions, offsets, consumer-group lag)
```

---

## 1.5 Offsets, commits & replay (E5)

**Concept.** An **offset** is a consumer's position within a partition. The *committed* offset is
**durable, server-side state** for a `group.id`, stored in Kafka's internal `__consumer_offsets`
topic — not in the consumer process. Because the log itself is retained, you can **rewind** a
group's committed offsets and **replay** history. This is a defining superpower of a log over a
traditional queue (which deletes messages on acknowledgement).

**How it works.**
- **Watermarks:** each partition has a **low** watermark (oldest offset still retained) and a
  **high** watermark (next offset to be written). **Lag = high − committed**.
- **Commit strategy:** we use manual commits (`enable.auto.commit=False`) committed *after*
  processing → at-least-once (see §1.2).
- **`auto.offset.reset` is a bootstrap-only fallback** — it only decides where a group starts when
  it has **no** committed offset. It does *not* trigger replay for a group that has already
  committed.
- **Replay = reset committed offsets.** `services/offset_admin.py` reads watermarks and commits new
  offsets for the group: `earliest` → low watermark (reprocess), `latest` → high watermark (skip
  backlog). It never `subscribe()`s, so it won't rebalance live members. **Stop the group's
  workers before resetting** — active members can overwrite the committed offset.

**How to test.**
```powershell
# inspect committed offsets, watermarks, and lag
python -m src.transaction_processor.services.offset_admin --group risk-cg --topic txn.created
# rewind to replay (workers stopped), or fast-forward to skip
python -m src.transaction_processor.services.offset_admin --group risk-cg --reset earliest
python -m src.transaction_processor.services.offset_admin --group risk-cg --reset latest
```

**How to validate (E5 observed).**
- Caught up: `committed == high`, `total_lag = 0`.
- `--reset earliest` → committed dropped to the **low watermarks `20 / 40 / 40` (not 0)** and
  `total_lag` jumped to **301**; restarting `risk_worker` replayed that history.
- `--reset latest` → committed returned to the **high watermarks**, `total_lag = 0`; a restart
  would process only new messages.

**Why it's helpful.** Replay is how you **recover from bugs** (fix logic, rewind, reprocess),
**backfill new consumers** (a fresh `group.id` reads from `earliest`), and **triage incidents**
(jump to `latest` to shed a stale backlog). Per-group offsets mean rewinding `risk-cg` never
disturbs `audit-cg`.

**Gotchas.**
- **You can only rewind as far back as the low watermark.** In E5, offsets `0–19`/`0–39` had
  already aged out via retention, so replay started at `20/40/40` — older data is gone for good.
- Replay under **at-least-once re-emits duplicates** downstream — safe reprocessing needs
  idempotency (Phase 3).
- Resetting offsets while the group has **live members** is unreliable; stop them first (or use a
  dedicated admin group).
- Alternatives: Kafka UI's consumer-group screen, or the canonical CLI
  `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute --group risk-cg --topic txn.created`.
  Both can also reset **to a timestamp** (e.g., "replay since 09:00"), which our tool could add via
  `--to-timestamp` later.

