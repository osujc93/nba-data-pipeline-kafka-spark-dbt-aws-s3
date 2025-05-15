#!/bin/bash

set -x

attempt_setup_fake_passwd_entry() {
  local myuid
  myuid="$(id -u)"
  if ! getent passwd "$myuid" &> /dev/null; then
    for wrapper in {/usr,}/lib{/*,}/libnss_wrapper.so; do
      if [ -s "$wrapper" ]; then
        NSS_WRAPPER_PASSWD="$(mktemp)"
        NSS_WRAPPER_GROUP="$(mktemp)"
        export LD_PRELOAD="$wrapper" NSS_WRAPPER_PASSWD NSS_WRAPPER_GROUP
        local mygid
        mygid="$(id -g)"
        printf 'spark:x:%s:%s:anonymous uid:/bin/false\n' "$myuid" "$mygid" > "$NSS_WRAPPER_PASSWD"
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

if [ -n "$SPARK_MODE" ]; then
  set -- "$SPARK_MODE" "$@"
fi

switch_spark_if_root() {
  if [ "$(id -u)" -eq 0 ]; then
    echo gosu sparkuser
  fi
}

case "$1" in

  history)
    shift
    CMD=(
      "${SPARK_HOME}/bin/spark-class"
      org.apache.spark.deploy.history.HistoryServer
      --properties-file "${SPARK_CONF_DIR}/spark-defaults.conf"
      "$@"
    )
    attempt_setup_fake_passwd_entry
    echo "[entrypoint.sh] Starting Spark History Server..."
    exec $(switch_spark_if_root) "${CMD[@]}"
    ;;

  master)
    shift
    CMD=(
      "${SPARK_HOME}/bin/spark-class"
      org.apache.spark.deploy.master.Master
      --ip "${SPARK_MASTER_HOST:-0.0.0.0}"
      --port "${SPARK_MASTER_PORT:-7077}"
      --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"
      "$@"
    )
    attempt_setup_fake_passwd_entry
    echo "[entrypoint.sh] Starting Spark Master..."
    exec $(switch_spark_if_root) "${CMD[@]}"
    ;;

  worker)
    shift

    if [ "${SPARK_SHUFFLE_SERVICE_ENABLED}" = "true" ]; then
      echo "[entrypoint.sh] Starting external shuffle service in background..."
      "${SPARK_HOME}/sbin/start-shuffle-service.sh"
    fi

    CMD=(
      "${SPARK_HOME}/bin/spark-class"
      org.apache.spark.deploy.worker.Worker
      "${SPARK_MASTER:-spark://spark-master1:7077}"
      --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}"
      "$@"
    )
    attempt_setup_fake_passwd_entry
    echo "[entrypoint.sh] Starting Spark Worker..."
    exec $(switch_spark_if_root) "${CMD[@]}"
    ;;

  driver|executor)
    shift
    attempt_setup_fake_passwd_entry
    echo "[entrypoint.sh] Starting Spark in $1 mode with leftover args: $@"
    exec $(switch_spark_if_root) /opt/spark/bin/spark-class "org.apache.spark.deploy.$1" "$@"
    ;;

  *)
    echo "[entrypoint.sh] No recognized Spark role. Arguments: $@"
    exec "$@"
    ;;
esac
