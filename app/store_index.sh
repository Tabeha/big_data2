#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP_DIR="$APP_DIR/tmp_store"

mkdir -p "$TMP_DIR"
rm -f "$TMP_DIR"/stage1.tsv "$TMP_DIR"/stage2.tsv

hdfs dfs -text /indexer/stage1/part-* > "$TMP_DIR/stage1.tsv"
hdfs dfs -text /indexer/stage2/part-* > "$TMP_DIR/stage2.tsv"

python3 "$APP_DIR/app.py" "$TMP_DIR/stage1.tsv" "$TMP_DIR/stage2.tsv"