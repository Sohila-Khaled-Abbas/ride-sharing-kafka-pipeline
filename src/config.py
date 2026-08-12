"""
config.py — Centralised Configuration
======================================
Single source of truth for all Kafka connection settings,
topic parameters, and application constants.

Changing a value here propagates to every module automatically.
"""

# ─── Kafka Cluster ───────────────────────────────────────────
BOOTSTRAP_SERVERS: list[str] = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094",
]

# ─── Topic Settings ──────────────────────────────────────────
TOPIC_NAME: str = "ride_trips"
NUM_PARTITIONS: int = 6            # 2 per broker → high parallelism
REPLICATION_FACTOR: int = 3        # Full replication across the cluster
TOPIC_CONFIGS: dict[str, str] = {
    "min.insync.replicas": "2",    # Writes need 2+ replica ACKs
}

# ─── Producer Settings ───────────────────────────────────────
PRODUCER_RETRIES: int = 5
PRODUCER_MAX_IN_FLIGHT: int = 5    # Safe with idempotency (Kafka ≥ 0.11)
PRODUCE_INTERVAL_SEC: float = 1.0  # Delay between messages
STARTING_TRIP_NUMBER: int = 1001

# ─── Consumer Settings ───────────────────────────────────────
CONSUMER_GROUP_ID: str = "ride-sharing-group"
AUTO_COMMIT_INTERVAL_MS: int = 1000  # Commit every 1 s (at-most-once)

# ─── Fake Data Pool ──────────────────────────────────────────
CAIRO_AREAS: list[str] = [
    "Nasr City", "Maadi", "Heliopolis", "Downtown", "Zamalek",
    "6th October", "New Cairo", "Dokki", "Mohandessin", "Giza",
    "Shubra", "Ain Shams", "Rehab City", "Tagamoa", "Sheikh Zayed",
]

TRIP_STATUSES: list[str] = ["completed", "in_progress", "cancelled"]

DISTANCE_RANGE: tuple[float, float] = (1.0, 35.0)
FARE_RANGE: tuple[float, float] = (25.0, 350.0)
DRIVER_ID_RANGE: tuple[int, int] = (1, 100)
PASSENGER_ID_RANGE: tuple[int, int] = (100, 999)
