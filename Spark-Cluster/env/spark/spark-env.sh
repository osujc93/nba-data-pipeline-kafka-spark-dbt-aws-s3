#!/bin/bash

# Common Java Options
export SPARK_COMMON_JAVA_OPTS="-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties \
  -Djava.security.properties=/usr/lib/jvm/java-17-openjdk-amd64/conf/security/java.security \
  -Djava.security.policy=/usr/lib/jvm/java-17-openjdk-amd64/conf/security/java.policy \
  -Djava.security.egd=file:/dev/urandom \
  -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=70 \
  --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED"

export SPARK_DAEMON_JAVA_OPTS="${SPARK_COMMON_JAVA_OPTS}"
export SPARK_DRIVER_JAVA_OPTS="${SPARK_COMMON_JAVA_OPTS}"
export SPARK_EXECUTOR_JAVA_OPTS="${SPARK_COMMON_JAVA_OPTS}"

export SPARK_LOCAL_DIRS=/tmp/spark-temp

export SPARK_CLASSPATH=$SPARK_CLASSPATH:/opt/spark/jars/xgboost4j-spark_2.12-2.1.3.jar
export SPARK_CLASSPATH=$SPARK_CLASSPATH:/opt/spark/jars/spark-nlp_2.12-5.5.2.jar

export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

export SPARK_EXECUTOR_CORES=1
export SPARK_JAVA_OPTS="${SPARK_COMMON_JAVA_OPTS}"

export XGBOOST_NUM_THREADS=1
export XGBOOST_TREE_METHOD=hist

export SPARK_HOME="/opt/spark"
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/opt/spark/conf}"
