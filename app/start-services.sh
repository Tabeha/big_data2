#!/bin/bash
set -euo pipefail

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop-3.2.1}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

if command -v service >/dev/null 2>&1; then
  service ssh start || true
fi

if [ -x "$HADOOP_HOME/bin/hdfs" ]; then
  hdfs namenode -format -force -nonInteractive || true
fi

if [ -x "$HADOOP_HOME/sbin/start-dfs.sh" ]; then
  "$HADOOP_HOME/sbin/start-dfs.sh" || true
else
  hdfs --daemon start namenode || true
  hdfs --daemon start datanode || true
fi

if [ -x "$HADOOP_HOME/sbin/start-yarn.sh" ]; then
  "$HADOOP_HOME/sbin/start-yarn.sh" || true
else
  yarn --daemon start resourcemanager || true
  yarn --daemon start nodemanager || true
fi

hdfs dfsadmin -safemode wait || true
hdfs dfs -mkdir -p /tmp /user/root || true