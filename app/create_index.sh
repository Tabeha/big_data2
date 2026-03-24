#!/bin/bash
set -euo pipefail

INPUT_PATH="${1:-/input/data}"
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
MAPREDUCE_DIR="$APP_DIR/mapreduce"
STREAMING_JAR="/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"

if [ ! -f "$STREAMING_JAR" ]; then
  echo "Could not find hadoop-streaming jar at $STREAMING_JAR" >&2
  exit 1
fi

hdfs dfs -rm -r -f /indexer || true
hdfs dfs -rm -r -f /tmp/indexer || true
hdfs dfs -mkdir -p /indexer /tmp/indexer

hadoop jar "$STREAMING_JAR" \
  -files "$MAPREDUCE_DIR/mapper1.py,$MAPREDUCE_DIR/reducer1.py" \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT_PATH" \
  -output /tmp/indexer/stage1

hadoop jar "$STREAMING_JAR" \
  -files "$MAPREDUCE_DIR/mapper2.py,$MAPREDUCE_DIR/reducer2.py" \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -input /tmp/indexer/stage1 \
  -output /indexer/stage2

hdfs dfs -mkdir -p /indexer/stage1
hdfs dfs -cp /tmp/indexer/stage1/part-* /indexer/stage1/

echo "Index created in HDFS"
echo "Stage1: /indexer/stage1"
echo "Stage2: /indexer/stage2"