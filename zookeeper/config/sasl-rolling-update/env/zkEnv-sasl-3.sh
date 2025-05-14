#!/bin/bash

# Determine JAAS configuration based on execution context
if [ "$ZK_IS_CLIENT" = "true" ]; then
    JAAS_CONF="/opt/bitnami/zookeeper/conf/jaas_client.conf"
    KEYTAB="/root/.ssh/zookeeper_keytabs/zookeeper-quorum-client3.keytab"
    PRINCIPAL="zookeeper-quorum-client3@EXAMPLE.COM"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Configuring ZooKeeper client JAAS settings."
else
    # Separate principals and keytabs for server and quorum
    JAAS_CONF="/opt/bitnami/zookeeper/conf/jaas_server.conf"
    KEYTAB_SERVER="/root/.ssh/zookeeper_keytabs/zookeeper-server3.keytab"
    PRINCIPAL_SERVER="zookeeper-server/zookeeper-server3@EXAMPLE.COM"
    
    KEYTAB_QUORUM="/root/.ssh/zookeeper_keytabs/zookeeper-quorum3.keytab"
    PRINCIPAL_QUORUM="zookeeper-quorum-server/zookeeper-quorum-server3@EXAMPLE.COM"
    
    PRINCIPAL_LEARNER="zookeeper-quorum-learner/zookeeper-quorum-learner3@EXAMPLE.COM"
    
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Configuring ZooKeeper server and quorum JAAS settings."
fi

# Set JAVA_HOME
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# JVM heap size for ZooKeeper
export ZOOMAXHEAP=4G

# Directory to store ZooKeeper logs
export ZOO_LOG_DIR="/var/log/zookeeper"

# Add essential libraries for SASL/GSSAPI
export LD_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH}"

# Determine ZooKeeper bind directory
ZOOBINDIR="${ZOOBINDIR:-/opt/bitnami/zookeeper/bin}"
ZOOKEEPER_PREFIX="${ZOOBINDIR}/.."

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
  if [ -e "${ZOOKEEPER_PREFIX}/conf" ]; then
    ZOOCFGDIR="${ZOOKEEPER_PREFIX}/conf"
  else
    ZOOCFGDIR="${ZOOKEEPER_PREFIX}/etc/zookeeper"
  fi
fi

# Source additional environment variables if present
if [ -f "${ZOOCFGDIR}/zookeeper-env.sh" ]; then
  . "${ZOOCFGDIR}/zookeeper-env.sh"
fi

# Set default ZooKeeper configuration file
if [ -z "$ZOOCFG" ]; then
    ZOOCFG="zoo.cfg"
fi
ZOOCFG="$ZOOCFGDIR/$ZOOCFG"

# Set default log directory if not set
if [ -z "${ZOO_LOG_DIR}" ]; then
    ZOO_LOG_DIR="${ZOOKEEPER_PREFIX}/logs"
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
CLASSPATH="${ZOOCFGDIR}"

# Add all necessary JARs from the ZooKeeper lib directory
for i in "${ZOOKEEPER_PREFIX}/lib/"*.jar; do
    CLASSPATH="${i}:${CLASSPATH}"
done

# Include essential libraries from /usr/lib/x86_64-linux-gnu for SASL/GSSAPI
for lib in /usr/lib/x86_64-linux-gnu/*.jar; do
    if [ -f "$lib" ]; then
        CLASSPATH="${lib}:${CLASSPATH}"
    fi
done

# Add Log4j JMX support
CLASSPATH="${ZOOKEEPER_PREFIX}/lib/log4j-api-2.24.1.jar:${ZOOKEEPER_PREFIX}/lib/log4j-core-2.24.1.jar:${ZOOKEEPER_PREFIX}/lib/log4j-1.2-api-2.24.1.jar:${ZOOKEEPER_PREFIX}/lib/log4j-slf4j2-impl-2.24.1.jar:${ZOOKEEPER_PREFIX}/lib/slf4j-api-2.0.16.jar:${ZOOCFGDIR}:${CLASSPATH}"

# Export the Classpath
export CLASSPATH

# Export essential environment variables
export PATH="${JAVA_HOME}/bin:${PATH}"
export KRB5_CONFIG=/etc/krb5.conf

# Set up server-specific environment variables
if [ "$ZK_IS_CLIENT" != "true" ]; then
    export KRB5_SERVER_KTNAME="$KEYTAB_SERVER"
    export KRB5_QUORUM_KTNAME="$KEYTAB_QUORUM"
    export ZK_QUORUM_SERVICE_PRINCIPAL="$PRINCIPAL_QUORUM"
fi

# SSL/TLS Configuration

JVMFLAGS="-Xms2G -Xmx4G -XX:+UseG1GC -Xlog:gc* \
-Dzookeeper.debug=true \
-Djava.net.preferIPv4Stack=true \
-Djava.net.preferIPv6Addresses=false \
-Dzookeeper.globalOutstandingLimit=50000 \
-Dzookeeper.preAllocSize=67108864 \
-Dzookeeper.snapCount=100000 \
-Dzookeeper.maxCnxns=100 \
-Dzookeeper.maxClientCnxns=100 \
-Dzookeeper.autopurge.snapRetainCount=3 \
-Dzookeeper.autopurge.purgeInterval=1 \
-Dzookeeper.connectionMaxTokens=10000 \
-Dzookeeper.connectionTokenFillTime=1000 \
-Dzookeeper.connectionTokenFillCount=10 \
-Dzookeeper.connectionFreezeTime=5000 \
-Dzookeeper.connectionDropIncrease=20 \
-Dzookeeper.connectionDropDecrease=10 \
-Dzookeeper.connectionDecreaseRatio=0.5 \
-Dzookeeper.advancedFlowControlEnabled=true \
-Dzookeeper.audit.enable=true \
-Dzookeeper.audit.log.dir=/var/log/zookeeper/audit \
-Dzookeeper.netty.server.earlyDropSecureConnectionHandshakes=true \
-Dzookeeper.learner.closeSocketAsync=true \
-Dzookeeper.leader.closeSocketAsync=true \
-Dzookeeper.initLimit=10 \
-Dzookeeper.syncLimit=5 \
-Dzookeeper.maxTimeToWaitForEpoch=5000 \
-Dzookeeper.connectToLearnerMasterLimit=5 \
-Dzookeeper.cnxTimeout=60000 \
-Dzookeeper.quorumCnxnTimeoutMs=7000 \
-Dzookeeper.tcpKeepAlive=true \
-Dzookeeper.clientTcpKeepAlive=true \
-Dzookeeper.DigestAuthenticationProvider.enabled=false \
-Dzookeeper.X509AuthenticationProvider.superUser=admin \
-Dzookeeper.superUser=auth:X509:admin,SASL:admin/admin@EXAMPLE.COM,SASL:zookeeper-quorum-server/zookeeper-quorum-server1@EXAMPLE.COM,SASL:zookeeper-server/zookeeper-server1@EXAMPLE.COM \
-Dzookeeper.ssl.authProvider=org.apache.zookeeper.server.auth.X509AuthenticationProvider \
-Dzookeeper.authProvider.x509=org.apache.zookeeper.server.auth.X509AuthenticationProvider \
-Dzookeeper.sessionRequireClientSASLAuth=true \
-Dzookeeper.enforce.auth.enabled=true \
-Dzookeeper.enforce.auth.schemes=sasl,x509 \
-Dzookeeper.requireClientAuthScheme=sasl,x509 \
-Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider \
-Dzookeeper.authProvider.2=org.apache.zookeeper.server.auth.X509AuthenticationProvider \
-Dzookeeper.kerberos.removeHostFromPrincipal=false \
-Dzookeeper.kerberos.removeRealmFromPrincipal=false \
-Dzookeeper.quorum.auth.enableSasl=true \
-Dzookeeper.quorum.auth.kerberos.servicePrincipal=zookeeper-quorum-server/zookeeper-quorum-server3@EXAMPLE.COM \
-Dzookeeper.quorum.auth.kerberos.removeHostFromPrincipal=false \
-Dzookeeper.quorum.auth.kerberos.removeRealmFromPrincipal=false \
-Dzookeeper.quorum.auth.server.loginContext=QuorumServer \
-Dzookeeper.quorum.auth.learner.loginContext=QuorumLearner \
-Dzookeeper.quorum.auth.saslMechanism=GSSAPI \
-Dzookeeper.quorum.cnxn.threads.size=25 \
-Dzookeeper.sslQuorum=true \
-Dzookeeper.serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory \
-Dzookeeper.ssl.keyStore.location=/opt/bitnami/zookeeper/conf/ssl/zookeeper-quorum-server3.keystore.p12 \
-Dzookeeper.ssl.keyStore.password=NeloNELO123456789 \
-Dzookeeper.ssl.keyStore.type=PKCS12 \
-Dzookeeper.ssl.trustStore.location=/opt/bitnami/zookeeper/conf/ssl/zookeeper-quorum-server3.truststore.p12 \
-Dzookeeper.ssl.trustStore.password=NeloNELO123456789 \
-Dzookeeper.ssl.trustStore.type=PKCS12 \
-Dzookeeper.ssl.quorum.keyStore.location=/opt/bitnami/zookeeper/conf/ssl/zookeeper-quorum-server3.keystore.p12 \
-Dzookeeper.ssl.quorum.keyStore.password=NeloNELO123456789 \
-Dzookeeper.ssl.quorum.trustStore.location=/opt/bitnami/zookeeper/conf/ssl/zookeeper-quorum-server3.truststore.p12 \
-Dzookeeper.ssl.quorum.trustStore.password=NeloNELO123456789 \
-Dzookeeper.ssl.quorum.trustStore.type=PKCS12 \
-Dzookeeper.ssl.protocol=TLSv1.2,TLSv1.3 \
-Dzookeeper.ssl.enabledProtocols=TLSv1.2,TLSv1.3 \
-Dzookeeper.ssl.ciphersuites=TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_AES_256_GCM_SHA384 \
-Dzookeeper.ssl.hostnameVerification=true \
-Dzookeeper.ssl.clientAuth=need \
-Dzookeeper.ssl.sslProvider=OpenSSL \
-Dzookeeper.ssl.quorum.protocol=TLSv1.2,TLSv1.3 \
-Dzookeeper.ssl.quorum.enabledProtocols=TLSv1.2,TLSv1.3 \
-Dzookeeper.ssl.quorum.ciphersuites=TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_AES_256_GCM_SHA384 \
-Dzookeeper.ssl.quorum.hostnameVerification=true \
-Dzookeeper.ssl.quorum.clientAuth=need \
-Dzookeeper.sslQuorumReloadCertFiles=true \
-Dzookeeper.sasl.client=true \
-Dzookeeper.sasl.clientconfig=Client \
-Dzookeeper.server.principal=zookeeper-server/zookeeper-server3@EXAMPLE.COM \
-Dzookeeper.sasl.client.username=zookeeper-server \
-Dzookeeper.sasl.client.canonicalize.hostname=false \
-Dzookeeper.server.realm=EXAMPLE.COM \
-Dzookeeper.client.secure=true \
-Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty \
-Dzookeeper.client.certReload=true \
-Dzookeeper.standaloneEnabled=false \
-Dzookeeper.reconfigEnabled=true \
-Dzookeeper.commitLogCount=1000 \
-Dzookeeper.snapSizeLimitInKb=10240 \
-Dzookeeper.txnLogSizeLimitInKb=5120 \
-Dzookeeper.fsync.warningthresholdms=100 \
-Dzookeeper.maxResponseCacheSize=1048576 \
-Dzookeeper.maxGetChildrenResponseCacheSize=524288 \
-Dzookeeper.connection_throttle_tokens=100 \
-Dzookeeper.connection_throttle_fill_time=500 \
-Dzookeeper.connection_throttle_fill_count=10 \
-Dzookeeper.connection_throttle_freeze_time=60000 \
-Dzookeeper.connection_throttle_drop_increase=5 \
-Dzookeeper.connection_throttle_drop_decrease=2 \
-Dzookeeper.connection_throttle_decrease_ratio=0.5 \
-Dzookeeper.connection_throttle_weight_enabled=true \
-Dzookeeper.connection_throttle_global_session_weight=1.0 \
-Dzookeeper.connection_throttle_local_session_weight=0.8 \
-Dzookeeper.connection_throttle_renew_session_weight=1.2 \
-Dzookeeper.request_throttle_max_requests=10000 \
-Dzookeeper.request_throttle_stall_time=3000 \
-Dzookeeper.request_throttle_drop_stale=true \
-Dzookeeper.request_throttled_op_wait_time=500 \
-Dzookeeper.throttled_op_wait_time=1000 \
-Dzookeeper.request_stale_latency_check=true \
-Dzookeeper.request_stale_connection_check=true \
-Dzookeeper.maxSessionTimeout=40000 \
-Dzookeeper.minSessionTimeout=4000 \
-Dzookeeper.DigestAuthenticationProvider.superDigest=admin:adminpassword \
-Dzookeeper.DigestAuthenticationProvider.digestAlg=sha-256 \
-Dzookeeper.IPAuthenticationProvider.usexforwardedfor=false \
-Dzookeeper.netty.advancedFlowControl.enabled=true \
-Dzookeeper.client.portUnification=false \
-Dzookeeper.electionAlg=3 \
-Dzookeeper.leader.maxTimeToWaitForEpoch=5000 \
-Dzookeeper.leaderServes=true \
-Dzookeeper.leader.maxConcurrentSnapSyncs=2 \
-Dzookeeper.leader.maxConcurrentDiffSyncs=2 \
-Dzookeeper.messageTracker.BufferSize=1024 \
-Dzookeeper.messageTracker.Enabled=true \
-Dzookeeper.outstandingHandshake.limit=100 \
-Dzookeeper.maxBatchSize=1048576 \
-Dzookeeper.maxWriteQueuePollTime=1000 \
-Dzookeeper.request_throttler.shutdownTimeout=30000 \
-Dzookeeper.socket.timeout=30000 \
-Dsun.security.jgss.native=true \
-Dlog4j.configuration=file:/opt/bitnami/zookeeper/conf/log4j.properties \
-Djdk.tls.server.signatureSchemes=ecdsa_secp384r1_sha384,ecdsa_secp256r1_sha256 \
-Djavax.security.auth.message.MessagePolicy.debug=true \
-Djava.net.debug=all \
-Djava.security.auth.login.config=\${JAAS_CONF} \
-Dsun.security.krb5.rcache=none \
-Djava.security.krb5.conf=/etc/krb5.conf \
-Dsun.security.krb5.debug=true \
-Dsun.security.ssl.debug=all \
-Djavax.security.auth.debug=all \
-Djavax.security.auth.useSubjectCredsOnly=false \
-Dsun.security.jgss.debug=true \
-Dsun.security.spnego.debug=true \
-Dsun.security.krb5.encryption.types=aes256-cts-hmac-sha1-96,aes128-cts-hmac-sha1-96,aes256-cts-hmac-sha384-192,aes128-cts-hmac-sha256-128 \
-Djavax.net.ssl.hostnameVerification=true \
-Djava.security.egd=file:/dev/urandom"

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
