#!/usr/bin/env bash
# ==============================================================================
# run_pipeline.sh — Automated Pipeline Runner (Linux / macOS)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "================================================================="
echo "  🚖 Automated Ride-Sharing Kafka Pipeline (Bash Runner)"
echo "================================================================="

echo "Step 1: Installing dependencies..."
python3 -m pip install -q -r requirements.txt

echo "Step 2: Executing automated test suite..."
python3 -m unittest discover tests -v

echo "Step 3: Launching automated pipeline..."
python3 -m src.pipeline_runner --num-events 15

echo "✅ Automation completed successfully!"
