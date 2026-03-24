FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV HADOOP_VERSION=3.3.6
ENV SPARK_VERSION=3.5.1
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin

RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3 \
    python3-pip \
    openssh-server \
    rsync \
    curl \
    wget \
    procps \
    netcat \
    vim \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/run/sshd && \
    ssh-keygen -A

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN mkdir -p /root/.ssh && \
    ssh-keygen -t rsa -P "" -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 600 /root/.ssh/authorized_keys

COPY docker/hadoop/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY docker/hadoop/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
COPY docker/hadoop/mapred-site.xml /opt/hadoop/etc/hadoop/mapred-site.xml
COPY docker/hadoop/yarn-site.xml /opt/hadoop/etc/hadoop/yarn-site.xml
COPY docker/hadoop/workers /opt/hadoop/etc/hadoop/workers
COPY docker/entrypoint.sh /entrypoint.sh
COPY docker/hadoop/hadoop-env.sh /opt/hadoop/etc/hadoop/hadoop-env.sh

RUN chmod +x /entrypoint.sh

RUN pip3 install --no-cache-dir \
    pyspark==3.5.1 \
    cassandra-driver==3.29.2

WORKDIR /workspace/app

ENTRYPOINT ["/entrypoint.sh"]