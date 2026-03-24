#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
HDFS_DOCS_DIR="${1:-/data}"
HDFS_INPUT_DIR="${2:-/input/data}"

python3 "$APP_DIR/prepare_data.py"

hdfs dfs -mkdir -p /data /input
hdfs dfs -rm -r -f "$HDFS_DOCS_DIR" || true
hdfs dfs -rm -r -f "$HDFS_INPUT_DIR" || true

hdfs dfs -mkdir -p "$HDFS_DOCS_DIR"
hdfs dfs -put -f "$APP_DIR"/data/*.txt "$HDFS_DOCS_DIR"/

PART_FILE="$(find "$APP_DIR/prepared_input" -type f -name 'part-*' | head -n 1)"
if [ -z "${PART_FILE:-}" ]; then
  echo "prepared input part file not found" >&2
  exit 1
fi

hdfs dfs -mkdir -p "$HDFS_INPUT_DIR"
hdfs dfs -put -f "$PART_FILE" "$HDFS_INPUT_DIR"/part-00000

echo "Documents uploaded to $HDFS_DOCS_DIR"
echo "Prepared input uploaded to $HDFS_INPUT_DIR"