#!/usr/bin/env bash
# Spark environment variables

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/bigdata/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_MASTER_HOST=master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_MEMORY=16g
export SPARK_WORKER_CORES=8
export SPARK_LOG_DIR=/opt/bigdata/spark/logs
export SPARK_PID_DIR=/opt/bigdata/spark/pids
