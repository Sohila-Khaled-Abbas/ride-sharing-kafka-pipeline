"""
tests/test_pipeline.py
======================
Unit and integration tests for the ride-sharing Kafka pipeline.
Validates:
    1. Configuration integrity and completeness
    2. Topic provisioning specification
    3. Data generator schema and value boundaries
    4. Serialization and deserialization roundtrip
"""

from __future__ import annotations

import json
import unittest
from datetime import datetime

from src.config import (
    AUTO_COMMIT_INTERVAL_MS,
    BOOTSTRAP_SERVERS,
    CAIRO_AREAS,
    CONSUMER_GROUP_ID,
    DISTANCE_RANGE,
    DRIVER_ID_RANGE,
    FARE_RANGE,
    NUM_PARTITIONS,
    PASSENGER_ID_RANGE,
    PRODUCE_INTERVAL_SEC,
    PRODUCER_MAX_IN_FLIGHT,
    PRODUCER_RETRIES,
    REPLICATION_FACTOR,
    STARTING_TRIP_NUMBER,
    TOPIC_CONFIGS,
    TOPIC_NAME,
    TRIP_STATUSES,
)
from src.create_topic import build_topic_spec
from src.producer import generate_trip


class TestConfiguration(unittest.TestCase):
    """Test that all configuration variables meet architectural requirements."""

    def test_topic_partitions_and_replication(self):
        self.assertEqual(NUM_PARTITIONS, 6, "Topic must have 6 partitions for 3-broker parallelism")
        self.assertEqual(REPLICATION_FACTOR, 3, "Topic must replicate across all 3 brokers")
        self.assertEqual(TOPIC_CONFIGS.get("min.insync.replicas"), "2", "min.insync.replicas must be 2")

    def test_bootstrap_servers_cluster(self):
        self.assertEqual(len(BOOTSTRAP_SERVERS), 3, "Must connect to all 3 brokers")
        self.assertIn("127.0.0.1:9092", BOOTSTRAP_SERVERS)
        self.assertIn("127.0.0.1:9093", BOOTSTRAP_SERVERS)
        self.assertIn("127.0.0.1:9094", BOOTSTRAP_SERVERS)

    def test_producer_idempotence_settings(self):
        self.assertGreater(PRODUCER_RETRIES, 0, "Retries must be enabled for idempotency")
        self.assertLessEqual(PRODUCER_MAX_IN_FLIGHT, 5, "Max in-flight requests must be <= 5 for idempotency")

    def test_consumer_at_most_once_settings(self):
        self.assertEqual(CONSUMER_GROUP_ID, "ride-sharing-group")
        self.assertEqual(AUTO_COMMIT_INTERVAL_MS, 1000)


class TestTopicProvisioning(unittest.TestCase):
    """Test topic specification generation."""

    def test_build_topic_spec(self):
        spec = build_topic_spec()
        self.assertEqual(spec.name, TOPIC_NAME)
        self.assertEqual(spec.num_partitions, 6)
        self.assertEqual(spec.replication_factor, 3)
        self.assertEqual(spec.topic_configs["min.insync.replicas"], "2")


class TestDataGenerator(unittest.TestCase):
    """Test fake trip generation against the schema."""

    def test_generate_trip_schema(self):
        trip = generate_trip(1001)

        required_keys = {
            "trip_id",
            "driver_id",
            "passenger_id",
            "pickup",
            "dropoff",
            "distance_km",
            "fare",
            "status",
            "timestamp",
        }
        self.assertEqual(set(trip.keys()), required_keys, f"Missing or extra keys in trip schema: {trip.keys()}")

    def test_trip_id_format(self):
        trip = generate_trip(1042)
        self.assertEqual(trip["trip_id"], "TRIP-1042")

    def test_driver_id_format(self):
        trip = generate_trip(1)
        self.assertTrue(trip["driver_id"].startswith("DRV-"))
        driver_num = int(trip["driver_id"].split("-")[1])
        self.assertTrue(DRIVER_ID_RANGE[0] <= driver_num <= DRIVER_ID_RANGE[1])

    def test_passenger_id_format(self):
        trip = generate_trip(1)
        self.assertTrue(trip["passenger_id"].startswith("PAS"))
        pas_num = int(trip["passenger_id"][3:])
        self.assertTrue(PASSENGER_ID_RANGE[0] <= pas_num <= PASSENGER_ID_RANGE[1])

    def test_pickup_and_dropoff_different(self):
        for _ in range(50):
            trip = generate_trip(1)
            self.assertIn(trip["pickup"], CAIRO_AREAS)
            self.assertIn(trip["dropoff"], CAIRO_AREAS)
            self.assertNotEqual(trip["pickup"], trip["dropoff"], "Pickup and dropoff must be different areas")

    def test_distance_and_fare_ranges(self):
        for _ in range(50):
            trip = generate_trip(1)
            self.assertTrue(DISTANCE_RANGE[0] <= trip["distance_km"] <= DISTANCE_RANGE[1])
            self.assertTrue(FARE_RANGE[0] <= trip["fare"] <= FARE_RANGE[1])
            self.assertIn(trip["status"], TRIP_STATUSES)

    def test_timestamp_iso_format(self):
        trip = generate_trip(1)
        dt = datetime.strptime(trip["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        self.assertIsNotNone(dt)


class TestSerialization(unittest.TestCase):
    """Test JSON serialization and deserialization."""

    def test_json_roundtrip(self):
        trip = generate_trip(1001)
        serialized = json.dumps(trip).encode("utf-8")
        self.assertIsInstance(serialized, bytes)

        deserialized = json.loads(serialized.decode("utf-8"))
        self.assertEqual(deserialized, trip)


if __name__ == "__main__":
    unittest.main()
