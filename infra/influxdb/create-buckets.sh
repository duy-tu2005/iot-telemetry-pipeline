#!/bin/sh
set -eu

ensure_bucket() {
  bucket_name="$1"
  retention="$2"

  bucket_id="$(
    influx bucket list \
      --host "$INFLUX_HOST" \
      --org "$INFLUX_ORG" \
      --token "$INFLUX_TOKEN" \
      --name "$bucket_name" \
      --hide-headers 2>/dev/null \
      | awk 'NR == 1 { print $1 }'
  )"

  if [ -n "$bucket_id" ]; then
    influx bucket update \
      --host "$INFLUX_HOST" \
      --token "$INFLUX_TOKEN" \
      --id "$bucket_id" \
      --retention "$retention" >/dev/null
    echo "Bucket '$bucket_name' already exists; retention set to $retention."
  else
    influx bucket create \
      --host "$INFLUX_HOST" \
      --org "$INFLUX_ORG" \
      --token "$INFLUX_TOKEN" \
      --name "$bucket_name" \
      --retention "$retention" >/dev/null
    echo "Created bucket '$bucket_name' with retention $retention."
  fi
}

# Reapplying the same retention is safe, so this init job is idempotent both on
# a fresh volume and after subsequent `docker compose up` runs.
ensure_bucket "$INFLUX_RAW_BUCKET" 30d
ensure_bucket "$INFLUX_PROCESSED_BUCKET" 180d
