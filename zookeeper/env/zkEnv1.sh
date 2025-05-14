#!/bin/bash

# Set JAVA_HOME
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# JVM heap size for ZooKeeper
export ZOOMAXHEAP=4G

# Directory to store ZooKeeper logs
export ZOO_LOG_DIR="/logs"

# Determine ZooKeeper bind directory
ZOOBINDIR="${ZOOBINDIR:-/apache-zookeeper-3.8.4-bin}"
ZOOKEEPER_PREFIX="${ZOOBINDIR}"

# Handle --config option
if [ $# -gt 1 ]; then
    if [ "--config" = "$1" ]; then
        shift
        confdir="$1"
        shift
        ZOOCFGDIR="$confdir"
    fi
fi

# Set ZOOCFGDIR based on existence
if [ -z "$ZOOCFGDIR" ]; then
  if [ -e "${ZOOKEEPER_PREFIX}/lib" ]; then
    ZOOCFGDIR="/conf"
  else
    ZOOCFGDIR="/conf"
  fi
fi

# Set default ZooKeeper configuration file
if [ -z "$ZOOCFG" ]; then
    ZOOCFG="zoo.cfg"
fi
ZOOCFG="$ZOOCFGDIR/$ZOOCFG"

# Set default log directory if not set
if [ -z "${ZOO_LOG_DIR}" ]; then
    ZOO_LOG_DIR="/logs"
fi

# Detect Java executable
if [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]]; then
    JAVA="$JAVA_HOME/bin/java"
elif type -p java >/dev/null 2>&1; then
    JAVA="$(type -p java)"
else
    echo "Error: JAVA_HOME is not set and java could not be found in PATH." 1>&2
    exit 1
fi

# Initialize CLASSPATH with the ZooKeeper configuration directory
CLASSPATH="/apache-zookeeper-3.8.4-bin/lib"

CLASSPATH="/apache-zookeeper-3.8.4-bin/lib/*:${CLASSPATH}"

# Add Log4j JMX support
CLASSPATH="/apache-zookeeper-3.8.4-bin/lib/log4j-api-2.24.1.jar:/apache-zookeeper-3.8.4-bin/lib/log4j-core-2.24.1.jar:/apache-zookeeper-3.8.4-bin/lib/log4j-1.2-api-2.24.1.jar:/apache-zookeeper-3.8.4-bin/lib/log4j-slf4j2-impl-2.24.1.jar:/apache-zookeeper-3.8.4-bin/lib/slf4j-api-2.0.16.jar:${ZOOCFGDIR}:${CLASSPATH}"

# Export the Classpath
export CLASSPATH

# Export essential environment variables
export PATH="${JAVA_HOME}/bin:${PATH}"

# JVM configuration (removed all SASL/SSL-related flags)
JVMFLAGS="-Xms2G -Xmx4G -XX:+UseG1GC -Xlog:gc* \
-Dzookeeper.debug=true \
-Dzookeeper.trace=true \
-Djava.net.preferIPv4Stack=true \
-Djava.net.preferIPv6Addresses=false \
-Dlog4j.configuration=file:/conf/log4j.properties \
-ea \
-Djava.security.egd=file:/dev/urandom \
-Dio.netty.noKeySetOptimization=true \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.management/sun.management=ALL-UNNAMED \
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED \
--add-opens=java.base/sun.security.ssl=ALL-UNNAMED \
--add-opens=java.base/sun.security.x509=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.security=ALL-UNNAMED \
--add-opens=java.base/sun.security.util=ALL-UNNAMED \
--add-opens=java.base/javax.net.ssl=ALL-UNNAMED \
--add-opens=java.base/java.nio.channels=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.nio.charset=ALL-UNNAMED \
--add-opens=java.base/java.nio.file=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/javax.security.auth.callback=ALL-UNNAMED \
--add-opens=java.base/javax.security.auth.login=ALL-UNNAMED \
--add-opens=java.base/javax.security.auth=ALL-UNNAMED \
--add-opens=java.base/javax.net=ALL-UNNAMED \
--add-opens=java.base/sun.net.util=ALL-UNNAMED \
--add-opens=java.base/sun.nio.fs=ALL-UNNAMED \
--add-opens=java.base/java.security.spec=ALL-UNNAMED \
--add-opens=java.base/java.security.interfaces=ALL-UNNAMED \
--add-opens=java.base/sun.security.provider=ALL-UNNAMED \
--add-opens=java.base/sun.security.rsa=ALL-UNNAMED \
--add-exports=java.base/sun.security.ssl=ALL-UNNAMED \
--add-exports=java.base/sun.security.x509=ALL-UNNAMED \
--add-exports=java.base/sun.security.util=ALL-UNNAMED \
--add-exports=java.base/sun.security.rsa=ALL-UNNAMED \
--add-opens=java.base/sun.security.pkcs=ALL-UNNAMED \
--add-modules=java.security.jgss,java.security.sasl,jdk.security.auth,java.naming"

# Detect if running in Cygwin
cygwin=false
case "$(uname)" in
    CYGWIN*) cygwin=true;;
esac
export cygwin

export JVMFLAGS

# Export the JVMFLAGS to JAVA_OPTS so that ZooKeeper uses them
export JAVA_OPTS="$JVMFLAGS"

# Optional: Verify JAVA_OPTS
echo "JAVA_OPTS set to: $JAVA_OPTS"
