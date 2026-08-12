# 🛠️ Operations & Troubleshooting Runbook

This runbook contains operational procedures, scaling instructions, monitoring steps, and disaster recovery commands for the **Ride-Sharing Kafka Pipeline**.

---

## 🚀 Standard Operations Workflow

```mermaid
flowchart TD
    A[Start Docker Cluster<br/>docker compose up -d] --> B[Run Automated Tests<br/>python -m unittest discover tests -v]
    B --> C[Provision Kafka Topic<br/>python -m src.create_topic]
    C --> D[Start Idempotent Producer<br/>python -m src.producer]
    C --> E[Start Parallel Consumers<br/>python -m src.consumer]
    D --> F[Monitor via Kafka UI<br/>http://localhost:9021]
    E --> F
```

---

## 👥 Scaling to Parallel Consumers

Because `ride_trips` is partitioned into **6 partitions**, you can run up to **6 parallel consumer instances** under the same consumer group `ride-sharing-group`.

```mermaid
graph TD
    subgraph "Topic: ride_trips (6 Partitions)"
        P0[Partition 0]
        P1[Partition 1]
        P2[Partition 2]
        P3[Partition 3]
        P4[Partition 4]
        P5[Partition 5]
    end

    subgraph "Consumer Group: ride-sharing-group (3 Consumers)"
        C1[Consumer 1<br/>Processes P0, P3]
        C2[Consumer 2<br/>Processes P1, P4]
        C3[Consumer 3<br/>Processes P2, P5]
    end

    P0 --> C1
    P3 --> C1
    P1 --> C2
    P4 --> C2
    P2 --> C3
    P5 --> C3
```

### Command: Launching 3 Parallel Consumers in Separate Terminals

```bash
# Terminal 1:
python -m src.consumer

# Terminal 2:
python -m src.consumer

# Terminal 3:
python -m src.consumer
```

Kafka's Group Coordinator will automatically trigger a **group rebalance** and assign 2 partitions to each consumer instance.

---

## 🔍 Verification & Inspection via CLI

### 1. Inspect Topic Configuration & Partitions

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 --describe --topic ride_trips
```

**Expected Output:**
```text
Topic: ride_trips	TopicId: ...	PartitionCount: 6	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: ride_trips	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: ride_trips	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: ride_trips	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: ride_trips	Partition: 3	Leader: 1	Replicas: 1,3,2	Isr: 1,3,2
	Topic: ride_trips	Partition: 4	Leader: 2	Replicas: 2,1,3	Isr: 2,1,3
	Topic: ride_trips	Partition: 5	Leader: 3	Replicas: 3,2,1	Isr: 3,2,1
```

### 2. Inspect Consumer Group Lag & Offsets

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --describe --group ride-sharing-group
```

---

## 🩺 Troubleshooting Guide

| Symptom | Probable Cause | Remediation |
|:---|:---|:---|
| `NoBrokersAvailable` | Docker ports not reachable or IPv6 resolution issue | Ensure `BOOTSTRAP_SERVERS` uses `127.0.0.1` IPv4 and containers are running via `docker ps`. |
| `NotEnoughReplicasException` | Less than 2 brokers in ISR | Start failed broker containers: `docker compose up -d`. |
| Consumer receiving no data | Cold start with `auto_offset_reset="latest"` | Start the producer after the consumer is running, or switch `auto_offset_reset` to `earliest`. |
| `TopicExistsException` | Topic already provisioned | Expected behavior. `src/create_topic.py` gracefully catches and skips this error. |
