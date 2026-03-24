#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export PATH="$SPARK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

QUERY="${*:-dogs}"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.ui.showConsoleProgress=false \
  "$APP_DIR/query.py" "$QUERY" 2>/dev/null | grep -E '^[0-9]+[[:space:]]'