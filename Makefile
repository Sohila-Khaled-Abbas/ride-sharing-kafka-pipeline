# ─────────────────────────────────────────────────────────────
# Ride-Sharing Kafka Pipeline — Makefile
# ─────────────────────────────────────────────────────────────
# Usage:
#   make help         - Display available targets
#   make install      - Install project dependencies
#   make test         - Run automated test suite
#   make run          - Execute end-to-end automated pipeline
#   make topic        - Provision the Kafka topic
#   make producer     - Run the idempotent producer
#   make consumer     - Run the at-most-once consumer
# ─────────────────────────────────────────────────────────────

.PHONY: help install test run topic producer consumer clean

PYTHON ?= python

help:
	@echo "🚖 Ride-Sharing Kafka Pipeline - Automation Targets:"
	@echo "  make install    - Install requirements"
	@echo "  make test       - Run all 13 unit tests"
	@echo "  make run        - Execute automated end-to-end pipeline"
	@echo "  make topic      - Provision Kafka topic"
	@echo "  make producer   - Start idempotent producer"
	@echo "  make consumer   - Start at-most-once consumer"

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	$(PYTHON) -m unittest discover tests -v

run:
	$(PYTHON) -m src.pipeline_runner --num-events 15

topic:
	$(PYTHON) -m src.create_topic

producer:
	$(PYTHON) -m src.producer

consumer:
	$(PYTHON) -m src.consumer

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
