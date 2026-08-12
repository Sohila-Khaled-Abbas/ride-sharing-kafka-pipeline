"""
consumer.py — At-Most-Once Kafka Consumer
==========================================
Reads ride-sharing trip records from the ``ride_trips`` topic
using **at-most-once** delivery semantics.

How at-most-once works:
    1. Consumer polls a batch of messages.
    2. The auto-commit background thread commits the offset
       (``auto_commit_interval_ms = 1000``).
    3. Application begins processing the batch.
    4. If the consumer crashes **after** the commit but **before**
       processing finishes → those messages are lost on restart.

Result: every message is delivered **zero or one** time — never twice.

Key consumer flags:
    ┌─────────────────────────────────┬──────────────────────────┐
    │ enable_auto_commit = True       │ Background offset commit │
    │ auto_commit_interval_ms = 1000  │ Commit before processing │
    │ auto_offset_reset = "latest"    │ Skip old backlog         │
    └─────────────────────────────────┴──────────────────────────┘

Usage:
    python -m src.consumer
"""

from __future__ import annotations

import json
import logging
import sys

from kafka import KafkaConsumer

from src.config import (
    AUTO_COMMIT_INTERVAL_MS,
    BOOTSTRAP_SERVERS,
    CONSUMER_GROUP_ID,
    KAFKA_API_VERSION,
    TOPIC_NAME,
)

# ─── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─── Consumer Factory ────────────────────────────────────────
def create_consumer() -> KafkaConsumer:
    """Instantiate a KafkaConsumer with at-most-once semantics.

    • ``enable_auto_commit=True`` — offsets are committed on a
      background thread, independently of message processing.
    • ``auto_offset_reset="latest"`` — on first join the consumer
      starts from the newest message, skipping any backlog.
    """
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        api_version=KAFKA_API_VERSION,
        group_id=CONSUMER_GROUP_ID,
        # --- At-Most-Once Semantics -------------------------------
        enable_auto_commit=True,
        auto_commit_interval_ms=AUTO_COMMIT_INTERVAL_MS,
        auto_offset_reset="latest",
        # --- Deserialisation --------------------------------------
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )


# ─── Message Handler ─────────────────────────────────────────
def handle_message(message) -> None:
    """Process and log a single consumed message."""
    trip = message.value
    logger.info(
        "Received [P%d|O%d]: %s | %s | %s → %s | %.1f km | %.2f EGP | %s",
        message.partition,
        message.offset,
        trip["trip_id"],
        trip["driver_id"],
        trip["pickup"],
        trip["dropoff"],
        trip["distance_km"],
        trip["fare"],
        trip["status"],
    )


# ─── Main Loop ───────────────────────────────────────────────
def run() -> None:
    """Poll and process messages until interrupted."""
    consumer = create_consumer()

    logger.info("=" * 55)
    logger.info("  Ride-Sharing At-Most-Once Consumer")
    logger.info("  Topic: %s  |  Group: %s", TOPIC_NAME, CONSUMER_GROUP_ID)
    logger.info("  Ctrl+C to stop")
    logger.info("=" * 55)

    try:
        for message in consumer:
            handle_message(message)

    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception:
        logger.exception("Unexpected consumer error.")
        sys.exit(1)
    finally:
        consumer.close()
        logger.info("Consumer shut down cleanly.")


# ─── Entry Point ─────────────────────────────────────────────
if __name__ == "__main__":
    run()
