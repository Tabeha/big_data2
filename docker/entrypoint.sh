#!/bin/bash
set -euo pipefail

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin"

export HDFS_NAMENODE_USER="${HDFS_NAMENODE_USER:-root}"
export HDFS_DATANODE_USER="${HDFS_DATANODE_USER:-root}"
export HDFS_SECONDARYNAMENODE_USER="${HDFS_SECONDARYNAMENODE_USER:-root}"
export YARN_RESOURCEMANAGER_USER="${YARN_RESOURCEMANAGER_USER:-root}"
export YARN_NODEMANAGER_USER="${YARN_NODEMANAGER_USER:-root}"

service ssh start

if [ "${CONTAINER_ROLE}" = "master" ]; then
    if [ ! -d "/hadoop/dfs/name/current" ]; then
        hdfs namenode -format -force -nonInteractive
    fi

    start-dfs.sh
    start-yarn.sh

    until hdfs dfs -ls / >/dev/null 2>&1; do
        echo "Waiting for HDFS..."
        sleep 3
    done

    until yarn node -list >/dev/null 2>&1; do
        echo "Waiting for YARN..."
        sleep 3
    done

    hdfs dfs -mkdir -p /tmp || true
    hdfs dfs -mkdir -p /user/root || true

    cd /workspace/app
    bash app.sh

    tail -f /dev/null
else
    hdfs --daemon start datanode
    yarn --daemon start nodemanager
    tail -f /dev/null
fi