<div align="center">

# 🚕 Ride-Sharing Kafka Pipeline

**Real-time ride-sharing data streaming with idempotent production and at-most-once consumption**

[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.3.2-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

*A data engineering assignment demonstrating Kafka topic design, duplicate-free production, and controlled consumption semantics on a 3-broker cluster.*

</div>

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Requirements](#-requirements)
- [Quick Start](#-quick-start)
- [Component Deep Dive](#-component-deep-dive)
  - [Topic Design — High Throughput](#1%EF%B8%8F⃣-topic-design--high-throughput--parallel-consumption)
  - [Idempotent Producer — No Duplicates](#2%EF%B8%8F⃣-idempotent-producer--no-duplicates)
  - [At-Most-Once Consumer](#3%EF%B8%8F⃣-at-most-once-consumer)
- [Message Schema](#-message-schema)
- [Sample Output](#-sample-output)
- [Configuration Reference](#-configuration-reference)
- [Further Reading](#-further-reading)
- [License](#-license)

---

## 🎯 Overview

This project implements a **complete Kafka streaming pipeline** for a ride-sharing platform:

| Requirement | Solution | Script |
| :--- | :--- | :---: |
| 🏗️ Create topic + producer with fake data | Topic with 6 partitions, Faker-generated trips | `src/create_topic.py` / `src/producer.py` |
| 🔒 No duplicate messages | Idempotent producer (`enable.idempotence=True`) | `src/producer.py` |
| ⚡ High throughput + parallel consumption | 6 partitions across 3 brokers | `src/create_topic.py` |
| 📨 At-most-once reading semantics | Auto-commit before processing | `src/consumer.py` |

---

## 🏛️ Architecture

```
                          ┌──────────────────────────────────────────────┐
                          │          Kafka Cluster  (3 Brokers)         │
                          │                                              │
  ┌─────────────┐        │   Topic: ride_trips                          │        ┌──────────────┐
  │             │        │   ┌────┬────┬────┬────┬────┬────┐           │        │              │
  │  Producer   │───────▶│   │ P0 │ P1 │ P2 │ P3 │ P4 │ P5 │           │───────▶│  Consumer(s) │
  │ (Idempotent)│        │   └────┴────┴────┴────┴────┴────┘           │        │(At-Most-Once)│
  │             │        │     RF=3      min.insync.replicas=2          │        │              │
  └─────────────┘        │                                              │        └──────────────┘
        │                └──────────────────────────────────────────────┘              │
        │                                                                              │
   PID + SeqNum                                                                 Auto-Commit
   = No Dupes ✓                                                                = Never Reprocess ✓
```

> **Cluster Infrastructure**: 1 ZooKeeper + 3 Kafka Brokers + Kafka UI, managed via Docker Compose in the parent [`kafka-cluster/`](../) directory.

---

## 📁 Project Structure

```
ride-sharing-assignment/
│
├── 📄 README.md               ← You are here
├── 📄 LICENSE                  ← MIT License
├── 📄 CONTRIBUTING.md          ← Contribution guidelines
├── 📄 CHANGELOG.md             ← Release history
├── 📄 .gitignore               ← Git ignore rules
├── 📄 .editorconfig            ← Consistent formatting
├── 📄 requirements.txt         ← Python dependencies
│
├── 📂 src/                     ← Python source package
│   ├── 🐍 __init__.py          ← Package initialiser
│   ├── 🐍 config.py            ← Centralised settings (Single Source of Truth)
│   ├── 🐍 create_topic.py      ← Topic provisioning (idempotent — safe to re-run)
│   ├── 🐍 producer.py          ← Idempotent fake-data producer
│   └── 🐍 consumer.py          ← At-most-once consumer
│
└── 📂 docs/
    └── 📄 architecture.md      ← In-depth technical deep dive
```

---

## 📋 Requirements

| Tool | Version | Purpose |
| :--- | :--- | :--- |
| **Docker** & **Docker Compose** | Latest | Run the Kafka cluster |
| **Python** | ≥ 3.8 | Run producer/consumer scripts |
| **kafka-python-ng** | ≥ 2.2.2 | Kafka client library (Python 3.12+ compatible) |
| **Faker** | ≥ 28.0 | Generate realistic fake data |

---

## 🚀 Quick Start

### 1️⃣ Start the Kafka Cluster

```bash
# From the parent kafka-cluster/ directory
docker compose up -d
```

> Verify at **<http://localhost:9021>** (Kafka UI)

### 2️⃣ Install Python Dependencies

```bash
cd ride-sharing-assignment
pip install -r requirements.txt
```

### 3️⃣ Create the Topic

```bash
python -m src.create_topic
```

<details>
<summary>📋 Expected Output</summary>

```
18:30:00  INFO      Topic 'ride_trips' created successfully.
18:30:00  INFO        ├── Partitions         : 6
18:30:00  INFO        ├── Replication Factor  : 3
18:30:00  INFO        └── min.insync.replicas : 2
```

</details>

### 4️⃣ Run the Producer *(Terminal 1)*

```bash
python -m src.producer
```

<details>
<summary>📋 Expected Output</summary>

```
18:31:00  INFO      =======================================================
18:31:00  INFO        Ride-Sharing Idempotent Producer
18:31:00  INFO        Topic: ride_trips  |  Ctrl+C to stop
18:31:00  INFO      =======================================================
18:31:01  INFO      Sent: TRIP-1001 | DRV-52 → Nasr City ➜ Maadi | 185.50 EGP | completed
18:31:01  INFO      Delivered → partition 3, offset 0
18:31:02  INFO      Sent: TRIP-1002 | DRV-18 → Heliopolis ➜ Downtown | 95.00 EGP | in_progress
18:31:02  INFO      Delivered → partition 1, offset 0
```

</details>

### 5️⃣ Run the Consumer *(Terminal 2)*

```bash
python -m src.consumer
```

<details>
<summary>📋 Expected Output</summary>

```
18:32:00  INFO      =======================================================
18:32:00  INFO        Ride-Sharing At-Most-Once Consumer
18:32:00  INFO        Topic: ride_trips  |  Group: ride-sharing-group
18:32:00  INFO        Ctrl+C to stop
18:32:00  INFO      =======================================================
18:32:01  INFO      Received [P3|O0]: TRIP-1001 | DRV-52 | Nasr City → Maadi | 12.5 km | 185.50 EGP | completed
18:32:02  INFO      Received [P1|O0]: TRIP-1002 | DRV-18 | Heliopolis → Downtown | 8.3 km | 95.00 EGP | in_progress
```

</details>

> **💡 Tip**: Run multiple `consumer.py` instances — they auto-join the same consumer group and Kafka distributes partitions among them for **parallel consumption**.

---

## 🔬 Component Deep Dive

### 1️⃣ Topic Design — High Throughput & Parallel Consumption

```python
# config.py
TOPIC_NAME         = "ride_trips"
NUM_PARTITIONS     = 6       # 2 leaders per broker → even load
REPLICATION_FACTOR = 3       # Full replication across all brokers
TOPIC_CONFIGS      = {"min.insync.replicas": "2"}
```

#### Why These Values?

| Decision | Rationale |
| :--- | :--- |
| **6 partitions** | 3 brokers × 2 leaders = even distribution. Supports up to 6 parallel consumers. |
| **RF = 3** | Every partition exists on all 3 brokers — survives 1 broker failure. |
| **min.insync.replicas = 2** | Writes need 2+ replicas to ACK — prevents silent data loss. |

#### Partition Distribution Across Brokers

```
Broker 1 (kafka1)    Broker 2 (kafka2)    Broker 3 (kafka3)
┌──────┬──────┐      ┌──────┬──────┐      ┌──────┬──────┐
│  P0  │  P3  │      │  P1  │  P4  │      │  P2  │  P5  │
│leader│leader│      │leader│leader│      │leader│leader│
└──────┴──────┘      └──────┴──────┘      └──────┴──────┘
  + replicas           + replicas           + replicas
  of P1,P2,P4,P5       of P0,P2,P3,P5       of P0,P1,P3,P4
```

---

### 2️⃣ Idempotent Producer — No Duplicates

```python
# producer.py — Key Configuration
KafkaProducer(
    enable_idempotence=True,                    # ← Broker-side dedup
    acks="all",                                 # ← All ISR must confirm
    retries=5,                                  # ← Safe retries
    max_in_flight_requests_per_connection=5,     # ← Reordered by broker
)
```

#### How Broker-Side Deduplication Works

```
  Normal flow:                        Retry flow (ACK lost):

  Producer                            Producer
     │                                   │
     ├─ send(PID=1, Seq=42) ──▶ Broker   ├─ send(PID=1, Seq=42) ──▶ Broker ✓ writes
     │                         writes ✓  │                         ACK lost ✗
     │◀── ACK ────────────────┘          │
     │                                   ├─ retry(PID=1, Seq=42) ──▶ Broker
     │                                   │                    detects dup → discard
     │                                   │◀── ACK ───────────────────┘
```

> The broker maintains a **dedup log** `{PID → last_sequence}` per partition. Same `(PID, SeqNum)` = silently discarded. **Zero duplicates guaranteed.**

---

### 3️⃣ At-Most-Once Consumer

```python
# consumer.py — Key Configuration
KafkaConsumer(
    enable_auto_commit=True,            # ← Commit on background thread
    auto_commit_interval_ms=1000,       # ← Every 1 second
    auto_offset_reset="latest",         # ← Skip backlog on first join
)
```

#### Timeline — Why Messages Can Be Lost

```
Time ──────────────────────────────────────────────────────────▶

 poll()              auto-commit fires          processing msg2
  │                       │                          │
  ▼                       ▼                          ▼
 [msg1, msg2, msg3]    offset=4 committed        handle(msg2)...
                                                      │
                                                  💥 CRASH
                                                      │
                                              restart → offset=4
                                              → msg2, msg3 are LOST
```

#### Delivery Semantics Comparison

| Semantic | Offset Committed | Risk | Use Case |
| :--- | :--- | :--- | :--- |
| **At-most-once** ✅ | **Before** processing | Message loss | Analytics, telemetry |
| At-least-once | After processing | Duplicates | Billing, logs |
| Exactly-once | Transactional | Highest latency | Financial transactions |

---

## 📦 Message Schema

Each message follows this JSON structure:

```json
{
  "trip_id":      "TRIP-1001",
  "driver_id":    "DRV-52",
  "passenger_id": "PAS812",
  "pickup":       "Nasr City",
  "dropoff":      "Maadi",
  "distance_km":  12.5,
  "fare":         185.50,
  "status":       "completed",
  "timestamp":    "2026-08-11T20:30:00Z"
}
```

| Field | Type | Description |
| :--- | :---: | :--- |
| `trip_id` | `string` | Unique trip identifier (`TRIP-{N}`) — used as **message key** |
| `driver_id` | `string` | Driver identifier (`DRV-{1..100}`) |
| `passenger_id` | `string` | Passenger identifier (`PAS{100..999}`) |
| `pickup` | `string` | Pickup location (Cairo area) |
| `dropoff` | `string` | Drop-off location (different Cairo area) |
| `distance_km` | `float` | Trip distance in kilometres (1.0 – 35.0) |
| `fare` | `float` | Trip fare in EGP (25.0 – 350.0) |
| `status` | `string` | One of: `completed`, `in_progress`, `cancelled` |
| `timestamp` | `string` | ISO 8601 UTC timestamp |

> **Key Design**: `trip_id` is used as the Kafka message key → `hash(trip_id) % 6` determines the partition. All messages for the same trip land in the **same partition**, preserving per-trip ordering.

---

## 🖥️ Sample Output

### create_topic.py

```
21:30:00  INFO      Topic 'ride_trips' created successfully.
21:30:00  INFO        ├── Partitions         : 6
21:30:00  INFO        ├── Replication Factor  : 3
21:30:00  INFO        └── min.insync.replicas : 2
```

### producer.py

```
21:31:00  INFO      =======================================================
21:31:00  INFO        Ride-Sharing Idempotent Producer
21:31:00  INFO        Topic: ride_trips  |  Ctrl+C to stop
21:31:00  INFO      =======================================================
21:31:01  INFO      Sent: TRIP-1001 | DRV-52 → Nasr City ➜ Maadi | 185.50 EGP | completed
21:31:01  INFO      Delivered → partition 3, offset 0
21:31:02  INFO      Sent: TRIP-1002 | DRV-18 → Heliopolis ➜ Downtown | 95.00 EGP | in_progress
21:31:02  INFO      Delivered → partition 1, offset 0
21:31:03  INFO      Sent: TRIP-1003 | DRV-73 → Zamalek ➜ 6th October | 220.75 EGP | completed
21:31:03  INFO      Delivered → partition 5, offset 0
```

### consumer.py

```
21:32:00  INFO      =======================================================
21:32:00  INFO        Ride-Sharing At-Most-Once Consumer
21:32:00  INFO        Topic: ride_trips  |  Group: ride-sharing-group
21:32:00  INFO        Ctrl+C to stop
21:32:00  INFO      =======================================================
21:32:01  INFO      Received [P3|O0]: TRIP-1001 | DRV-52 | Nasr City → Maadi | 12.5 km | 185.50 EGP | completed
21:32:02  INFO      Received [P1|O0]: TRIP-1002 | DRV-18 | Heliopolis → Downtown | 8.3 km | 95.00 EGP | in_progress
```

---

## ⚙️ Configuration Reference

All tuneable settings live in [`config.py`](config.py):

| Constant | Default | Module(s) | Purpose |
| :--- | :---: | :--- | :--- |
| `BOOTSTRAP_SERVERS` | `localhost:9092,9093,9094` | All | Kafka broker addresses |
| `TOPIC_NAME` | `ride_trips` | All | Target topic name |
| `NUM_PARTITIONS` | `6` | `create_topic` | Partition count |
| `REPLICATION_FACTOR` | `3` | `create_topic` | Replica count |
| `PRODUCER_RETRIES` | `5` | `producer` | Max send retries |
| `PRODUCE_INTERVAL_SEC` | `1.0` | `producer` | Delay between messages |
| `CONSUMER_GROUP_ID` | `ride-sharing-group` | `consumer` | Consumer group name |
| `AUTO_COMMIT_INTERVAL_MS` | `1000` | `consumer` | Auto-commit frequency |

---

## 📚 Further Reading

| Topic | Link |
| :--- | :--- |
| Kafka Idempotent Producer | [KIP-98 — Exactly Once Delivery](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) |
| Consumer Delivery Semantics | [Kafka Docs — Consumer Configs](https://kafka.apache.org/documentation/#consumerconfigs) |
| kafka-python-ng | [PyPI — kafka-python-ng](https://pypi.org/project/kafka-python-ng/) |
| Faker Library | [Faker Documentation](https://faker.readthedocs.io/) |

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Built with ❤️ for the Data Engineering Diploma — Big Data Module**

*Data Pill · Session 5*

</div>
