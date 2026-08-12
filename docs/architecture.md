# Architecture — Deep Dive

This document provides an in-depth technical explanation of every design
decision in the ride-sharing Kafka pipeline.

---

## 1. System Overview

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │                        Kafka Cluster (3 Brokers)                   │
 │  ┌───────────┐    ┌───────────┐    ┌───────────┐                   │
 │  │  Broker 1  │    │  Broker 2  │    │  Broker 3  │                  │
 │  │ kafka1:9092│    │ kafka2:9093│    │ kafka3:9094│                  │
 │  └───────────┘    └───────────┘    └───────────┘                   │
 │         │                │                │                         │
 │         └────────────────┼────────────────┘                         │
 │                          │                                          │
 │              Topic: ride_trips                                      │
 │              6 Partitions × RF 3                                    │
 └──────────────────────────┬──────────────────────────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
     ┌────────▼────────┐        ┌─────────▼────────┐
     │    Producer      │        │    Consumer(s)    │
     │  (Idempotent)    │        │  (At-Most-Once)   │
     │  PID + SeqNum    │        │  Auto-Commit ON   │
     └─────────────────┘        └──────────────────┘
```

---

## 2. Topic Design

### 2.1 Partition Count — Why 6?

| Factor                  | Detail                                       |
|-------------------------|----------------------------------------------|
| Broker count            | 3 brokers in the cluster                     |
| Leaders per broker      | 6 ÷ 3 = **2 leaders each** (even load)      |
| Max parallel consumers  | Up to **6** in one consumer group            |
| Overhead                | Low — 6 is conservative; production may use 12–36 |

Each partition is an **ordered, immutable log**. More partitions mean more
I/O parallelism on the broker side and more consumer threads on the
application side.

### 2.2 Replication Factor — Why 3?

With RF = 3, every partition has **3 replicas** (1 leader + 2 followers).
Combined with `min.insync.replicas = 2`:

- A write succeeds only when **≥ 2 replicas** persist it.
- The cluster tolerates **1 broker failure** without data loss.
- If 2 brokers fail simultaneously, writes are rejected (not silently lost).

### 2.3 Partition Assignment

The producer keys messages by `trip_id`. Kafka hashes the key to determine
the target partition:

```
partition = hash(trip_id) % num_partitions
```

This guarantees that **all messages for the same trip always land in the
same partition**, preserving per-trip ordering.

---

## 3. Idempotent Producer — Exactly-Once to the Broker

### 3.1 The Duplicate Problem

Without idempotency, duplicates occur when:

```
Producer ──send──▶ Broker (writes message)
                       │
              ACK lost in network ✗
                       │
Producer ──retry──▶ Broker (writes AGAIN) ← DUPLICATE
```

### 3.2 How Idempotency Solves It

When `enable.idempotence = true`:

1. Broker assigns the producer a unique **Producer ID (PID)**.
2. Each message batch carries a **monotonic sequence number**.
3. Broker maintains a **dedup log**: `{PID → last_sequence_per_partition}`.
4. On retry, the broker sees `(PID=1, Seq=42)` already exists → **discards**.

```
Producer ──send(PID=1, Seq=42)──▶ Broker ✓ writes
                                       │
                              ACK lost ✗
                                       │
Producer ──retry(PID=1, Seq=42)──▶ Broker detects dup → discards → ACK
```

### 3.3 Required Settings

| Setting              | Value   | Why Required                                  |
|----------------------|---------|-----------------------------------------------|
| `enable_idempotence` | `True`  | Activates PID + SeqNum tracking               |
| `acks`               | `"all"` | Mandatory for idempotency (Kafka enforces)     |
| `max_in_flight`      | `≤ 5`   | Broker can reorder up to 5 in-flight batches   |
| `retries`            | `> 0`   | Must allow retries for dedup to be meaningful  |

---

## 4. At-Most-Once Consumer

### 4.1 Semantics Comparison

| Semantic         | Offset Commit                | Risk              |
|------------------|------------------------------|--------------------|
| At-most-once     | **Before** processing        | Message loss       |
| At-least-once    | **After** processing         | Duplicate processing |
| Exactly-once     | Transactional (read+process) | Highest latency    |

### 4.2 How Auto-Commit Creates At-Most-Once

```
 Time ──────────────────────────────────────────────▶

 poll()          auto-commit fires       processing
  │                    │                     │
  ▼                    ▼                     ▼
 [msg1, msg2, msg3]   offset=4 committed   handle(msg2)...
                                                │
                                            💥 CRASH
                                                │
                                           restart → offset=4
                                           → msg2, msg3 LOST
```

The key insight: `auto_commit_interval_ms = 1000` means offsets are
committed on a **background timer**, not gated by application processing.

### 4.3 Consumer Group & Partition Assignment

All consumers sharing `group_id = "ride-sharing-group"` form a
**consumer group**. Kafka's Group Coordinator assigns partitions:

| Consumers in group | Partitions per consumer |
|--------------------|-------------------------|
| 1                  | 6 (all partitions)      |
| 2                  | 3 each                  |
| 3                  | 2 each                  |
| 6                  | 1 each (maximum parallelism) |
| 7+                 | 1 idle consumer (no partition) |

---

## 5. Code Architecture

```
ride-sharing-assignment/
│
├── config.py            Single source of truth for all settings
│                        (SRP — no magic numbers in code)
│
├── create_topic.py      Topic provisioning (idempotent — safe to re-run)
│                        Uses KafkaAdminClient
│
├── producer.py          Fake data generation + idempotent delivery
│                        generate_trip() → KafkaProducer.send()
│
└── consumer.py          Message polling + at-most-once consumption
                         KafkaConsumer iterator → handle_message()
```

### Design Principles Applied

| Principle                    | Where Applied                                |
|------------------------------|----------------------------------------------|
| **Single Responsibility**    | Each module does one thing                   |
| **DRY**                      | All settings in `config.py`                  |
| **Separation of Concerns**   | Config / topic / produce / consume are independent |
| **Fail Fast**                | `sys.exit(1)` on unrecoverable errors        |
| **Logging over Printing**    | `logging` module with timestamps everywhere  |
| **Type Hints**               | All function signatures are typed             |
