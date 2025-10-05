#!/usr/bin/env bash
set -euo pipefail

BROKER=${BROKER:-localhost:9092}
TOPIC=${TOPIC:-events}

msgs='[
 {"user":{"id":1,"email":"a@example.com"},"event":{"ts":"2025-10-05T12:00:00Z","type":"signup","value":1}},
 {"user":{"id":2,"email":"b@example.com"},"event":{"ts":"2025-10-05T12:01:00Z","type":"purchase","value":23.5}},
 {"user":{"id":3,"email":null},"event":{"ts":"2025-10-05T12:02:00Z","type":"page_view","value":null}}
]'

for row in $(echo "$msgs" | jq -c '.[]'); do
  echo "$row" | kafka-console-producer --broker-list "$BROKER" --topic "$TOPIC" >/dev/null
  echo "Produced: $row"
  sleep 0.1
done
