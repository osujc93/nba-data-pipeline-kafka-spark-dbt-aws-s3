#!/bin/bash

export SPARK_MODE=history

export SPARK_COMMON_JAVA_OPTS="-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties \
  -Djava.security.properties=/usr/lib/jvm/java-17-openjdk-amd64/conf/security/java.security \
  -Djava.security.policy=/usr/lib/jvm/java-17-openjdk-amd64/conf/security/java.policy \
  -Djava.security.egd=file:/dev/urandom"

export SPARK_DAEMON_JAVA_OPTS="${SPARK_COMMON_JAVA_OPTS}"
export SPARK_DRIVER_JAVA_OPTS="${SPARK_COMMON_JAVA_OPTS}"
export SPARK_EXECUTOR_JAVA_OPTS="${SPARK_COMMON_JAVA_OPTS}"

export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=s3a://nelodatawarehouse93/spark-events \
  -Dspark.history.ui.port=18080 \
  -Dspark.history.ui.acls.enable=true \
  -Dspark.history.ui.admin.acls=spark \
  -Dspark.history.ui.admin.acls.groups=hadoop \
  -Dspark.rpc.message.maxSize=20480 \
  -Dspark.network.timeout=300000"

export SPARK_HOME=/opt/spark
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/opt/spark/conf}"
export SPARK_WORK_DIR=/opt/spark/work
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

