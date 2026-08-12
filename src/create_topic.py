"""
create_topic.py — Kafka Topic Provisioning
===========================================
Creates the ``ride_trips`` topic with settings optimised for
high throughput and parallel consumption.

Why 6 partitions?
    • 3 brokers × 2 leaders each = even load distribution
    • Up to 6 consumers can read in parallel (1 per partition)

Why RF = 3 & min.insync.replicas = 2?
    • Every partition exists on all 3 brokers (full replication)
    • A write is acknowledged only when ≥ 2 replicas confirm,
      tolerating 1 broker failure without data loss

Usage:
    python -m src.create_topic
"""

from __future__ import annotations

import logging
import sys

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from src.config import (
    BOOTSTRAP_SERVERS,
    KAFKA_API_VERSION,
    NUM_PARTITIONS,
    REPLICATION_FACTOR,
    TOPIC_CONFIGS,
    TOPIC_NAME,
)

# ─── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─── Core Logic ──────────────────────────────────────────────
def build_topic_spec() -> NewTopic:
    """Return a ``NewTopic`` object from centralised config."""
    return NewTopic(
        name=TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        topic_configs=TOPIC_CONFIGS,
    )


def create_topic() -> None:
    """Provision the topic on the Kafka cluster (idempotent)."""
    admin: KafkaAdminClient = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        api_version=KAFKA_API_VERSION,
        client_id="ride-sharing-admin",
    )
    try:
        admin.create_topics(
            new_topics=[build_topic_spec()],
            validate_only=False,
        )
        logger.info("Topic '%s' created successfully.", TOPIC_NAME)
        logger.info("  ├── Partitions         : %d", NUM_PARTITIONS)
        logger.info("  ├── Replication Factor  : %d", REPLICATION_FACTOR)
        logger.info(
            "  └── min.insync.replicas : %s",
            TOPIC_CONFIGS["min.insync.replicas"],
        )
    except TopicAlreadyExistsError:
        logger.info("Topic '%s' already exists — skipping.", TOPIC_NAME)
    except Exception:
        logger.exception("Failed to create topic '%s'.", TOPIC_NAME)
        sys.exit(1)
    finally:
        admin.close()


# ─── Entry Point ─────────────────────────────────────────────
if __name__ == "__main__":
    create_topic()
