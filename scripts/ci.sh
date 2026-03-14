#!/usr/bin/env bash
# Local CI: build dev image, configure, build, test.
# Requires Docker. CMake >=3.23 inside the container (Ubuntu 24.04 provides 3.28).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="rag-env:v0.0.1"

echo "[ci] building dev image..."
docker build -f docker/dev.Dockerfile -t "$IMAGE" "$REPO_ROOT"

echo "[ci] configure + build + test..."
docker run --rm \
  -v "$REPO_ROOT":/workspace \
  -w /workspace \
  "$IMAGE" \
  bash -c "
    cmake --preset release &&
    cmake --build --preset release --parallel &&
    ctest --test-dir build/release --output-on-failure
  "

echo "[ci] all checks passed."
