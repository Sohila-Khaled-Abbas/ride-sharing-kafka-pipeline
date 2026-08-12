# ⚖️ Kafka Delivery Semantics & Idempotence Guide

This guide details the theoretical foundation, configuration matrix, and operational trade-offs of messaging semantics in Apache Kafka, with specific reference to this ride-sharing pipeline.

---

## 📊 Messaging Semantics Matrix

```mermaid
graph TD
    subgraph "Producer Layer"
        P1[Standard Producer<br/>acks=1, retries=3] -->|Risk: Duplicate on retry| K1[Kafka Topic]
        P2[Idempotent Producer<br/>enable_idempotence=True, acks=all] -->|Guarantee: Exactly-Once Delivery| K2[Kafka Topic]
    end

    subgraph "Consumer Layer"
        K2 -->|Commit BEFORE Process| C1[At-Most-Once Consumer<br/>enable_auto_commit=True<br/>Risk: Data Loss on crash]
        K2 -->|Commit AFTER Process| C2[At-Least-Once Consumer<br/>manual commitSync<br/>Risk: Duplicates on crash]
        K2 -->|Transactional Offset Store| C3[Exactly-Once Stream<br/>Read-Committed + TX<br/>Cost: Higher Latency]
    end
```

---

## 🔒 1. Producer Idempotence (KIP-98)

### Problem: Transient Network Failures Cause Duplicate Writes

Without idempotency, when a producer transmits a batch to a broker and the broker writes it to disk but the **ACK packet is dropped in the network**, the producer assumes failure and retries. This causes the broker to append the record a second time.

```mermaid
sequenceDiagram
    autonumber
    actor Producer
    participant Broker Leader
    participant Replica Broker

    Producer->>Broker Leader: Send Message (Trip-1001)
    Broker Leader->>Replica Broker: Replicate Batch
    Replica Broker-->>Broker Leader: Replica In-Sync ACK
    Broker Leader--xProducer: ACK Lost (Network Timeout)
    Note over Producer: Producer times out & initiates RETRY
    Producer->>Broker Leader: Resend Message (Trip-1001)
    Note over Broker Leader: Duplicate record appended to log!
```

---

### Solution: Idempotent Producer Protocol

When `enable_idempotence=True` is activated:

1. **InitProducerId**: On initialization, the producer receives an 8-byte **Producer ID (PID)** and an epoch from the cluster coordinator.
2. **Monotonic Sequence Numbers**: Every batch sent by the producer to a specific partition is assigned a zero-indexed sequence number (`0, 1, 2, ...`).
3. **Broker Deduplication State**: The broker leader tracks the highest sequence number written per `PID` for each partition. If a received sequence number equals or precedes the highest recorded sequence number, the broker **silently discards the duplicate** while still returning a success ACK.

```mermaid
sequenceDiagram
    autonumber
    actor Producer
    participant Broker Leader (PID=1042)
    participant Replica Broker

    Producer->>Broker Leader: Send Batch (PID=1042, Seq=0, Trip-1001)
    Broker Leader->>Replica Broker: Replicate Batch
    Replica Broker-->>Broker Leader: Replica In-Sync ACK
    Note over Broker Leader: Stored: PID 1042 -> LastSeq = 0
    Broker Leader--xProducer: ACK Lost (Network Timeout)
    Note over Producer: Producer Retries Batch
    Producer->>Broker Leader: Resend Batch (PID=1042, Seq=0, Trip-1001)
    Note over Broker Leader: Detected Seq <= LastSeq (0 <= 0)<br/>DISCARD DUPLICATE!
    Broker Leader-->>Producer: Return Success ACK
```

---

## 📨 2. At-Most-Once Consumption

### Mechanism

At-most-once reading ensures a record is processed **at most 1 time** (either 0 or 1). It guarantees that duplicates will **never** be processed by the consumer application.

```mermaid
sequenceDiagram
    autonumber
    actor Consumer
    participant AutoCommit Thread
    participant Broker Offsets Log

    Consumer->>Broker Offsets Log: poll() fetches records [Offset 0, 1, 2]
    AutoCommit Thread->>Broker Offsets Log: Commit Offset 3 (auto_commit_interval_ms=1000ms)
    Broker Offsets Log-->>AutoCommit Thread: Offset 3 Committed!
    Note over Consumer: Consumer begins processing Offset 0...
    Note over Consumer: Consumer crashes while processing Offset 1!
    Note over Consumer: Consumer restarts and fetches from Committed Offset (3)
    Note over Consumer: Offset 1 & 2 are SKIPPED (Lost, but never duplicated)
```

---

## ⚙️ Configuration Comparison Matrix

| Parameter | At-Most-Once (Our Setup) | At-Least-Once | Exactly-Once (EOS) |
|:---|:---:|:---:|:---:|
| `enable_auto_commit` | `True` | `False` | `False` |
| `auto_commit_interval_ms` | `1000` | N/A | N/A |
| `auto_offset_reset` | `latest` | `earliest` | `earliest` |
| `isolation_level` | `read_uncommitted` | `read_uncommitted` | `read_committed` |
| **Throughput** | ⚡⚡⚡ Highest | ⚡⚡ High | ⚡ Moderate |
| **Risk** | Message Loss on crash | Duplicate processing | Latency overhead |
