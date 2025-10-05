#!/usr/bin/env bash
set -euo pipefail

# Seeds a few JSON messages to the 'events' topic using the bitnami/kafka container.

echo "Starting seed-kafka one-shot container..."
docker compose run --rm seed-kafka
echo "Done."
