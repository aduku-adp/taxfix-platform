#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLS_DIR="$ROOT_DIR/tools"
AIRFLOW_IMAGE_NAME="${AIRFLOW_IMAGE_NAME:-taxfix-airflow:local}"

cd "$ROOT_DIR"
export AIRFLOW_IMAGE_NAME

"$TOOLS_DIR/sh/build_airflow.sh"
docker compose down --volumes --remove-orphans
docker compose up


echo "Airflow deployed. UI: http://localhost:8080"
