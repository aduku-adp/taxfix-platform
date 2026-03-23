#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE_NAME="${AIRFLOW_IMAGE_NAME:-taxfix-airflow:local}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-$ROOT_DIR/tools/Dockerfile}"
BUILD_CONTEXT="${BUILD_CONTEXT:-$ROOT_DIR}"

echo "Building Airflow image:"
echo "  image      : $IMAGE_NAME"
echo "  dockerfile : $DOCKERFILE_PATH"
echo "  context    : $BUILD_CONTEXT"

docker build \
  -f "$DOCKERFILE_PATH" \
  -t "$IMAGE_NAME" \
  "$BUILD_CONTEXT"

echo "Built image $IMAGE_NAME"
