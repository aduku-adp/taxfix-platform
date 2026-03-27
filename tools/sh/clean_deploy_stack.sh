#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLS_DIR="$ROOT_DIR/tools"
AIRFLOW_IMAGE_NAME="${AIRFLOW_IMAGE_NAME:-taxfix-airflow:local}"

cd "$ROOT_DIR"
export AIRFLOW_IMAGE_NAME

# Clean postgres
echo "Clean the stack"
rm -rf "$ROOT_DIR/dbs/pg_data"
mkdir -p "$ROOT_DIR/dbs/pg_data"

rm -rf "$ROOT_DIR/dbs/duckdb_data"
mkdir -p "$ROOT_DIR/dbs/duckdb_data"

# Build airflow image
echo "Build airflow image"
"$TOOLS_DIR/sh/build_airflow.sh"

# Initialize airflow
echo "Initialize airflow"
docker compose up airflow-init
docker compose down --volumes --remove-orphans

# Depoy the stack
echo "Depoy the stack"
docker compose up -d

echo "Airflow deployed. UI: http://localhost:8080"
