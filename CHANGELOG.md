# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] — 2026-08-12

### Added

- **Topic provisioning** (`create_topic.py`) — 6 partitions, RF=3, min.insync.replicas=2
- **Idempotent producer** (`producer.py`) — fake ride data with broker-side deduplication
- **At-most-once consumer** (`consumer.py`) — auto-commit before processing
- **Centralised config** (`config.py`) — single source of truth for all settings
- **Architecture docs** (`docs/architecture.md`) — deep-dive technical documentation
- GitHub project files: `.gitignore`, `.editorconfig`, `LICENSE`, `CONTRIBUTING.md`
