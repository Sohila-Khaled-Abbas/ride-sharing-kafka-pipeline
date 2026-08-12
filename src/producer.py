"""
producer.py — Idempotent Kafka Producer
========================================
Generates fake ride-sharing trip records and publishes them
to the ``ride_trips`` topic **without duplicates**.

Idempotency mechanism (Kafka ≥ 0.11):
    1. The broker assigns the producer a **Producer ID (PID)**.
    2. Every message batch carries a **monotonic sequence number**.
    3. On retry the broker sees the same (PID, SeqNum) and **discards
       the duplicate** silently — the message appears exactly once.

Key producer flags:
    ┌──────────────────────────────────────┬────────────────────┐
    │ enable_idempotence = True            │ Broker-side dedup  │
    │ acks = "all"                         │ All ISR must ACK   │
    │ retries = 5                          │ Safe retries       │
    │ max_in_flight_requests_per_conn = 5  │ Reordered by broker│
    └──────────────────────────────────────┴────────────────────┘

Usage:
    python -m src.producer
"""

from __future__ import annotations

import json
import logging
import random
import sys
import time
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer
from kafka.producer.future import RecordMetadata

from src.config import (
    BOOTSTRAP_SERVERS,
    CAIRO_AREAS,
    DISTANCE_RANGE,
    DRIVER_ID_RANGE,
    FARE_RANGE,
    KAFKA_API_VERSION,
    PASSENGER_ID_RANGE,
    PRODUCE_INTERVAL_SEC,
    PRODUCER_MAX_IN_FLIGHT,
    PRODUCER_RETRIES,
    STARTING_TRIP_NUMBER,
    TOPIC_NAME,
    TRIP_STATUSES,
)

# ─── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─── Data Generation ─────────────────────────────────────────
def generate_trip(trip_number: int) -> dict[str, Any]:
    """Build a single fake ride-sharing trip record.

    Returns a dict matching the assignment schema::

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
    """
    pickup = random.choice(CAIRO_AREAS)
    dropoff = random.choice([a for a in CAIRO_AREAS if a != pickup])

    return {
        "trip_id": f"TRIP-{trip_number}",
        "driver_id": f"DRV-{random.randint(*DRIVER_ID_RANGE)}",
        "passenger_id": f"PAS{random.randint(*PASSENGER_ID_RANGE)}",
        "pickup": pickup,
        "dropoff": dropoff,
        "distance_km": round(random.uniform(*DISTANCE_RANGE), 1),
        "fare": round(random.uniform(*FARE_RANGE), 2),
        "status": random.choice(TRIP_STATUSES),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ─── Producer Factory ────────────────────────────────────────
def create_producer() -> KafkaProducer:
    """Instantiate an **idempotent** KafkaProducer.

    Idempotency prevents duplicates even if the producer retries
    a send that the broker already persisted (ACK lost in transit).
    """
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        api_version=KAFKA_API_VERSION,
        # --- Idempotency (no duplicates) --------------------------
        enable_idempotence=True,
        acks="all",
        retries=PRODUCER_RETRIES,
        max_in_flight_requests_per_connection=PRODUCER_MAX_IN_FLIGHT,
        # --- Serialisation ----------------------------------------
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


# ─── Delivery Callbacks ──────────────────────────────────────
def _on_success(metadata: RecordMetadata) -> None:
    logger.info(
        "Delivered → partition %d, offset %d",
        metadata.partition,
        metadata.offset,
    )


def _on_error(exc: Exception) -> None:
    logger.error("Delivery failed: %s", exc)


# ─── Main Loop ───────────────────────────────────────────────
def run() -> None:
    """Produce one fake trip per second until interrupted."""
    producer = create_producer()
    trip_number = STARTING_TRIP_NUMBER

    logger.info("=" * 55)
    logger.info("  Ride-Sharing Idempotent Producer")
    logger.info("  Topic: %s  |  Ctrl+C to stop", TOPIC_NAME)
    logger.info("=" * 55)

    try:
        while True:
            trip = generate_trip(trip_number)

            # Key = trip_id → deterministic partition assignment
            future = producer.send(TOPIC_NAME, key=trip["trip_id"], value=trip)
            future.add_callback(_on_success).add_errback(_on_error)

            logger.info(
                "Sent: %s | %s → %s ➜ %s | %.2f EGP | %s",
                trip["trip_id"],
                trip["driver_id"],
                trip["pickup"],
                trip["dropoff"],
                trip["fare"],
                trip["status"],
            )

            trip_number += 1
            time.sleep(PRODUCE_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception:
        logger.exception("Unexpected producer error.")
        sys.exit(1)
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer shut down cleanly.")


# ─── Entry Point ─────────────────────────────────────────────
if __name__ == "__main__":
    run()
