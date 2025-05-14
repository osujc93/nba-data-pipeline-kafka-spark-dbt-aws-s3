#!/usr/bin/env bash

# server.sh - ZooKeeper Server Control Script

# Determine the directory of this script
ZOOBIN="${BASH_SOURCE[0]}"
ZOOBIN="$(dirname "${ZOOBIN}")"
ZOOBINDIR="$(cd "${ZOOBIN}"; pwd)"

# Source the environment variables from zkenv.sh
ENV_SCRIPT="${ZOOBINDIR}/zkEnv.sh"

if [ -f "$ENV_SCRIPT" ]; then
    . "$ENV_SCRIPT"
else
    echo "Cannot find zkenv.sh" >&2
    exit 1
fi

# Default JMX settings
: "${JMXLOCALONLY:=false}"
: "${JMXDISABLE:=false}"

if [ "$JMXDISABLE" = "false" ]; then
    echo "ZooKeeper JMX enabled by default" >&2
    if [ -z "$JMXPORT" ]; then
        ZOOMAIN="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=$JMXLOCALONLY org.apache.zookeeper.server.quorum.QuorumPeerMain"
    else
        : "${JMXAUTH:=false}"
        : "${JMXSSL:=false}"
        : "${JMXLOG4J:=true}"
        echo "ZooKeeper remote JMX Port set to $JMXPORT" >&2
        echo "ZooKeeper remote JMX authenticate set to $JMXAUTH" >&2
        echo "ZooKeeper remote JMX ssl set to $JMXSSL" >&2
        echo "ZooKeeper remote JMX log4j set to $JMXLOG4J" >&2
        if [ -z "$JMXHOSTNAME" ]; then
            ZOOMAIN="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$JMXPORT \
            -Dcom.sun.management.jmxremote.authenticate=$JMXAUTH \
            -Dcom.sun.management.jmxremote.ssl=$JMXSSL \
            -Dzookeeper.jmx.log4j.disable=$JMXLOG4J org.apache.zookeeper.server.quorum.QuorumPeerMain"
        else
            echo "ZooKeeper remote JMX Hostname set to $JMXHOSTNAME" >&2
            ZOOMAIN="-Dcom.sun.management.jmxremote -Djava.rmi.server.hostname=$JMXHOSTNAME \
            -Dcom.sun.management.jmxremote.port=$JMXPORT \
            -Dcom.sun.management.jmxremote.authenticate=$JMXAUTH \
            -Dcom.sun.management.jmxremote.ssl=$JMXSSL \
            -Dzookeeper.jmx.log4j.disable=$JMXLOG4J org.apache.zookeeper.server.quorum.QuorumPeerMain"
        fi
    fi
else
    echo "JMX disabled by user request" >&2
    ZOOMAIN="org.apache.zookeeper.server.quorum.QuorumPeerMain"
fi

# Append any additional JVMFLAGS
if [ -n "$SERVER_JVMFLAGS" ]; then
    JVMFLAGS="$SERVER_JVMFLAGS $JVMFLAGS"
fi

# Handle config file argument
if [ -n "$2" ]; then
    ZOOCFG="$ZOOCFGDIR/$2"
fi

# Allow full path for config
if [ "$(dirname "$ZOOCFG")" != "$ZOOCFGDIR" ]; then
    ZOOCFG="$2"
fi

# Handle cygwin
if [ "$cygwin" = "true" ]; then
    if command -v cygpath >/dev/null 2>&1; then
        ZOOCFG=$(cygpath -wp "$ZOOCFG")
    fi
    KILL="/bin/kill"
else
    KILL="kill"
fi

# Validate ZOOCFG
if [ -z "$ZOOCFG" ]; then
    echo "ZooKeeper configuration file is not set." >&2
    exit 1
fi

if [ ! -f "$ZOOCFG" ]; then
    echo "ZooKeeper configuration file '$ZOOCFG' does not exist." >&2
    exit 1
fi

echo "Using config: $ZOOCFG" >&2

# Determine data directories
GREP=grep
case "$OSTYPE" in
    *solaris*) GREP=/usr/xpg4/bin/grep ;;
esac

ZOO_DATADIR=$( $GREP "^[[:space:]]*dataDir" "$ZOOCFG" | sed -e 's/.*=//;s/^[[:space:]]*//;s/[[:space:]]*$//' )
ZOO_DATALOGDIR=$( $GREP "^[[:space:]]*dataLogDir" "$ZOOCFG" | sed -e 's/.*=//;s/^[[:space:]]*//;s/[[:space:]]*$//' )

# Optional autocreate
if [ -n "$ZOO_DATADIR_AUTOCREATE_DISABLE" ]; then
    if [ ! -d "$ZOO_DATADIR/version-2" ]; then
        echo "ZooKeeper data directory is missing at $ZOO_DATADIR. Fix the path or run initialize."
        exit 1
    fi

    if [ -n "$ZOO_DATALOGDIR" ] && [ ! -d "$ZOO_DATALOGDIR/version-2" ]; then
        echo "ZooKeeper txnlog directory is missing at $ZOO_DATALOGDIR. Fix path or run initialize."
        exit 1
    fi
    ZOO_DATADIR_AUTOCREATE="-Dzookeeper.datadir.autocreate=false"
fi

# PID file handling
ZOOPIDFILE=${ZOOPIDFILE:-"$ZOO_DATADIR/zookeeper_server.pid"}
mkdir -p "$(dirname "$ZOOPIDFILE")"

# Ensure log directory
if [ ! -d "$ZOO_LOG_DIR" ]; then
    mkdir -p "$ZOO_LOG_DIR"
fi

ZOOL_LOG_FILE=${ZOO_LOG_FILE:-"zookeeper-$USER-server-$HOSTNAME.log"}
_ZOO_DAEMON_OUT="$ZOO_LOG_DIR/zookeeper.out"

check_zookeeper_running() {
    if [ -f "$ZOOPIDFILE" ]; then
        existing_pid=$(cat "$ZOOPIDFILE")
        if kill -0 "$existing_pid" > /dev/null 2>&1; then
            return 0
        else
            return 1
        fi
    else
        return 1
    fi
}

wait_for_zookeeper() {
    local retries=10
    local wait_time=3
    echo "Waiting for ZooKeeper server to initialize..."
    for ((i=1;i<=retries;i++)); do
        if check_zookeeper_running; then
            echo "ZooKeeper is running (PID: $(cat "$ZOOPIDFILE"))."
            return 0
        fi
        echo "Attempt $i/$retries: ZooKeeper not yet running. Waiting ${wait_time} seconds..."
        sleep "$wait_time"
    done
    echo "ZooKeeper failed to start within expected time." >&2
    return 1
}

case "$1" in
    start)
        echo -n "Starting ZooKeeper ... "
        if check_zookeeper_running; then
            echo "ZooKeeper already running as process $(cat "$ZOOPIDFILE")."
            exit 1
        fi

        nohup "$JAVA" $ZOO_DATADIR_AUTOCREATE "-Dzookeeper.log.dir=${ZOO_LOG_DIR}" \
        "-Dzookeeper.log.file=${ZOOL_LOG_FILE}" \
        -XX:+HeapDumpOnOutOfMemoryError -XX:OnOutOfMemoryError='kill -9 %p' \
        -cp "$CLASSPATH" $JVMFLAGS $ZOOMAIN "$ZOOCFG" > "$_ZOO_DAEMON_OUT" 2>&1 < /dev/null &

        pid=$!
        if [ -n "$pid" ]; then
            echo "$pid" > "$ZOOPIDFILE"
            if wait_for_zookeeper; then
                echo "STARTED (PID: $pid)"
            else
                echo "FAILED TO START"
                exit 1
            fi
        else
            echo "FAILED TO START"
            exit 1
        fi
        ;;
    start-foreground)
        exec "$JAVA" $ZOO_DATADIR_AUTOCREATE "-Dzookeeper.log.dir=${ZOO_LOG_DIR}" \
        "-Dzookeeper.log.file=${ZOOL_LOG_FILE}" \
        -XX:+HeapDumpOnOutOfMemoryError -XX:OnOutOfMemoryError='kill -9 %p' \
        -cp "$CLASSPATH" $JVMFLAGS $ZOOMAIN "$ZOOCFG"
        ;;
    print-cmd)
        echo "\"$JAVA\" $ZOO_DATADIR_AUTOCREATE -Dzookeeper.log.dir=\"${ZOO_LOG_DIR}\" \
        -Dzookeeper.log.file=\"${ZOOL_LOG_FILE}\" \
        -XX:+HeapDumpOnOutOfMemoryError -XX:OnOutOfMemoryError='kill -9 %p' \
        -cp \"$CLASSPATH\" $JVMFLAGS $ZOOMAIN \"$ZOOCFG\" > \"$_ZOO_DAEMON_OUT\" 2>&1 < /dev/null"
        ;;
    stop)
        echo -n "Stopping ZooKeeper ... "
        if [ ! -f "$ZOOPIDFILE" ]; then
            echo "No ZooKeeper to stop (could not find file $ZOOPIDFILE)"
        else
            pid=$(cat "$ZOOPIDFILE")
            if kill -0 "$pid" > /dev/null 2>&1; then
                $KILL "$pid"
                echo "Sent SIGTERM to ZooKeeper PID: $pid"
                sleep 2
                if kill -0 "$pid" > /dev/null 2>&1; then
                    echo "ZooKeeper is still running. Sending SIGKILL."
                    $KILL -9 "$pid"
                else
                    echo "ZooKeeper STOPPED"
                fi
            else
                echo "ZooKeeper process with PID $pid is not running. Removing stale PID file."
            fi
            rm -f "$ZOOPIDFILE"
        fi
        exit 0
        ;;
    version)
        "$JAVA" -cp "$CLASSPATH" org.apache.zookeeper.version.VersionInfoMain 2> /dev/null
        ;;
    restart)
        shift
        "$0" stop "$@"
        sleep 3
        "$0" start "$@"
        ;;
    status)
        # Attempt to parse from zoo.cfg
        secureClientPort=$(grep "^[[:space:]]*secureClientPort[^[:alpha:]]" "$ZOOCFG" | sed -e 's/.*=//')
        secureClientPortAddress=$(grep "^[[:space:]]*secureClientPortAddress[^[:alpha:]]" "$ZOOCFG" | sed -e 's/.*=//')

        # default to false
        isSSL="false"

        if [ -n "$secureClientPort" ] && [ "$secureClientPort" -ne 0 ]; then
            isSSL="true"
            clientPort="$secureClientPort"
            clientPortAddress="${secureClientPortAddress:-localhost}"
        else
            clientPort=$(grep "^[[:space:]]*clientPort[^[:alpha:]]" "$ZOOCFG" | sed -e 's/.*=//')
            clientPortAddress=$(grep "^[[:space:]]*clientPortAddress[^[:alpha:]]" "$ZOOCFG" | sed -e 's/.*=//')

            # -------------------------------
            # (CHANGED HERE) Force 0.0.0.0 so the 4LW test
            # can succeed in Docker containers
            # -------------------------------
            if [ -z "$clientPortAddress" ]; then
                clientPortAddress="0.0.0.0"
            fi
        fi

        echo "Client port found: $clientPort. Client address: $clientPortAddress. Client SSL: $isSSL."

        STAT=$( "$JAVA" "-Dzookeeper.log.dir=${ZOO_LOG_DIR}" "-Dzookeeper.log.file=${ZOOL_LOG_FILE}" \
              -cp "$CLASSPATH" $CLIENT_JVMFLAGS $JVMFLAGS org.apache.zookeeper.client.FourLetterWordMain \
              "$clientPortAddress" "$clientPort" srvr "$isSSL" 2> /dev/null | grep Mode )
        if [ -z "$STAT" ]; then
            if [ "$isSSL" = "true" ]; then
                echo
                echo "Note: Used secureClientPort ($secureClientPort) but failed. Possibly missing your SSL config."
                echo
            fi
            echo "Error contacting service. It is probably not running."
            exit 1
        else
            echo "$STAT"
            exit 0
        fi
        ;;
    *)
        echo "Usage: $0 [--config <conf-dir>] {start|start-foreground|stop|version|restart|status|print-cmd}" >&2
        ;;
esac
