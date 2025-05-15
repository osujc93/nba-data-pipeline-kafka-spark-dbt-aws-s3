#!/bin/bash

set -x

attempt_setup_fake_passwd_entry() {
  local myuid
  myuid="$(id -u)"
  if ! getent passwd "$myuid" &> /dev/null; then
    local wrapper
    for wrapper in {/usr,}/lib{/*,}/libnss_wrapper.so; do
      if [ -s "$wrapper" ]; then
        NSS_WRAPPER_PASSWD="$(mktemp)"
        NSS_WRAPPER_GROUP="$(mktemp)"
        export LD_PRELOAD="$wrapper" NSS_WRAPPER_PASSWD NSS_WRAPPER_GROUP
        local mygid
        mygid="$(id -g)"
        printf 'spark:x:%s:%s:%s:/bin/false\n' "$myuid" "$mygid" "${SPARK_USER_NAME:-anonymous uid}" > "$NSS_WRAPPER_PASSWD"
        printf 'spark:x:%s:\n' "$mygid" > "$NSS_WRAPPER_GROUP"
        break
      fi
    done
  fi
}

if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 > /dev/null | grep 'java.home' | awk '{print $3}')
  export JAVA_HOME
fi

if [ -z "$SPARK_CONF_DIR" ]; then
  export SPARK_CONF_DIR="/opt/spark/conf"
fi

if [ -f "$SPARK_CONF_DIR/spark-env.sh" ]; then
  source "$SPARK_CONF_DIR/spark-env.sh"
fi

SPARK_CLASSPATH="$SPARK_CLASSPATH:${SPARK_HOME}/jars/*"
[ -n "${PYSPARK_PYTHON}" ] && export PYSPARK_PYTHON
[ -n "${PYSPARK_DRIVER_PYTHON}" ] && export PYSPARK_DRIVER_PYTHON

if [ -n "${HADOOP_HOME}" ] && [ -z "${SPARK_DIST_CLASSPATH}" ]; then
  export SPARK_DIST_CLASSPATH="$(${HADOOP_HOME}/bin/hadoop classpath)"
fi

if [ -n "${HADOOP_CONF_DIR}" ]; then
  SPARK_CLASSPATH="$HADOOP_CONF_DIR:$SPARK_CLASSPATH"
fi

if [ -n "${SPARK_CONF_DIR}" ]; then
  SPARK_CLASSPATH="$SPARK_CONF_DIR:$SPARK_CLASSPATH"
else
  SPARK_CLASSPATH="$SPARK_HOME/conf:$SPARK_CLASSPATH"
fi

SPARK_CLASSPATH="$SPARK_CLASSPATH:$PWD"
export SPARK_CLASSPATH

switch_spark_if_root() {
  if [ "$(id -u)" -eq 0 ]; then
    echo gosu sparkuser
  fi
}

case "$SPARK_MODE" in
  history)
    echo "[entrypoint.sh] Starting Spark History Server..."
    attempt_setup_fake_passwd_entry
    exec $(switch_spark_if_root) \
      "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.history.HistoryServer \
      --properties-file "${SPARK_CONF_DIR}/spark-defaults.conf"
    ;;
  *)
    echo "[entrypoint.sh] No recognized Spark mode ($SPARK_MODE). Arguments: $@"
    exec "$@"
    ;;
esac
