#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export PATH="$SPARK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

cd "$APP_DIR"

echo "=== Waiting for Cassandra ==="
until python3 - <<'PY'
from cassandra.cluster import Cluster

try:
    cluster = Cluster(["cassandra"])
    session = cluster.connect()
    session.execute("SELECT release_version FROM system.local")
    cluster.shutdown()
    raise SystemExit(0)
except Exception:
    raise SystemExit(1)
PY
do
  echo "Waiting for Cassandra..."
  sleep 5
done

echo "=== Step 1: Prepare data ==="
bash prepare_data.sh

echo "=== Step 2: Build and store index ==="
bash index.sh

echo "=== Waiting for search_engine keyspace/tables ==="
for _ in $(seq 1 24); do
  if python3 - <<'PY'
from cassandra.cluster import Cluster

cluster = Cluster(["cassandra"])
try:
    session = cluster.connect()

    ks = list(session.execute(
        "SELECT keyspace_name FROM system_schema.keyspaces "
        "WHERE keyspace_name='search_engine'"
    ))

    tables = list(session.execute(
        "SELECT table_name FROM system_schema.tables "
        "WHERE keyspace_name='search_engine'"
    ))

    needed = {"documents", "terms", "postings", "corpus_stats"}
    existing = {row.table_name for row in tables}

    ok = bool(ks) and needed.issubset(existing)
    print("keyspace_exists =", bool(ks))
    print("tables =", sorted(existing))
    raise SystemExit(0 if ok else 1)
finally:
    cluster.shutdown()
PY
  then
    break
  fi

  echo "Waiting for search_engine keyspace/tables..."
  sleep 5
done

echo "=== Step 3: Run demo queries ==="
bash search.sh "computer science"
bash search.sh "art history"
bash search.sh "machine learning"

echo "=== Pipeline finished successfully ==="