#!/bin/bash

# Determine JAAS configuration based on execution context
if [ "$ZK_IS_CLIENT" = "true" ]; then
    JAAS_CONF="/conf/jaas_client.conf"
    KEYTAB="/conf/zookeeper_keytabs/zookeeper-quorum-client1.keytab"
    PRINCIPAL="zookeeper-quorum-client1@NELO.COM"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Configuring ZooKeeper client JAAS settings."
else
    # Separate principals and keytabs for server and quorum
    JAAS_CONF="/conf/jaas_server.conf"
    KEYTAB_SERVER="/conf/zookeeper_keytabs/zookeeper-server1.keytab"
    PRINCIPAL_SERVER="zookeeper-server/zookeeper-server1@NELO.COM"
    
    KEYTAB_QUORUM="/conf/zookeeper_keytabs/zookeeper-quorum1.keytab"
    PRINCIPAL_QUORUM="zookeeper-quorum-server/zookeeper-quorum-server1@NELO.COM"
    
    PRINCIPAL_LEARNER="zookeeper-quorum-learner/zookeeper-quorum-learner1@NELO.COM"
    
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Configuring ZooKeeper server and quorum JAAS settings."
fi

# Set JAVA_HOME
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# JVM heap size for ZooKeeper
export ZOOMAXHEAP=4G

# Directory to store ZooKeeper logs
export ZOO_LOG_DIR="/logs"

# Add essential libraries for SASL/GSSAPI
export LD_LIBRARY_PATH="/lib/x86_64-linux-gnu:/usr/lib:${LD_LIBRARY_PATH}"

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
-Dzookeeper.trace=true \
-Djava.security.debug=ssl,handshake,sslkeymanager,ssltrustmanager,alpn,cipher,trustmanager,record,logincontext,configfile,gssloginconfig,accesscontrol,access,failure,configparser,configfileparser \
-Djava.security.properties=/usr/lib/jvm/java-17-openjdk-amd64/conf/security/java.security \
-Djava.security.policy=/usr/lib/jvm/java-17-openjdk-amd64/conf/security/java.policy \
-Djavax.net.debug=ssl:handshake:verbose:trustmanager:session:record:certpath:keymanager:sslkeymanager:ssltrustmanager:alpn:cipher \
-Djavax.security.debug=sasl,gssapi,login \
-Djava.net.preferIPv4Stack=true \
-Djava.net.preferIPv6Addresses=false \
-Djavax.net.ssl.keyStore=/conf/ssl/zookeeper-quorum-server1.keystore.p12 \
-Djavax.net.ssl.keyStorePassword=NeloNELO123456789 \
-Djavax.net.ssl.keyStoreType=PKCS12 \
-Djavax.net.ssl.trustStore=/conf/ssl/zookeeper-quorum-server1.truststore.p12 \
-Djavax.net.ssl.trustStorePassword=NeloNELO123456789 \
-Djavax.net.ssl.trustStoreType=PKCS12 \
-Djavax.net.ssl.keyStoreProvider=BC \
-Djavax.net.ssl.trustStoreProvider=BC \
-Dlog4j.configuration=file:/conf/log4j.properties \
-Djdk.tls.client.cipherSuites=TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 \
-Djdk.tls.server.cipherSuites=TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 \
-Djdk.tls.server.SignatureSchemes=ecdsa_secp384r1_sha384,ecdsa_secp256r1_sha256,rsa_pss_rsae_sha256,rsa_pss_rsae_sha384,rsa_pss_rsae_sha512,rsa_pkcs1_sha256,rsa_pkcs1_sha384,rsa_pkcs1_sha512 \
-Djdk.tls.client.SignatureSchemes=ecdsa_secp384r1_sha384,ecdsa_secp256r1_sha256,rsa_pss_rsae_sha256,rsa_pss_rsae_sha384,rsa_pss_rsae_sha512,rsa_pkcs1_sha256,rsa_pkcs1_sha384,rsa_pkcs1_sha512 \
-Djavax.security.auth.message.MessagePolicy.debug=true \
-Djavax.security.auth.debug=all \
-Dzookeeper.ssl.sslProvider=OPENSSL \
-Dzookeeper.sslQuorum=true \
-Dzookeeper.tcpKeepAlive=true \
-Dzookeeper.clientTcpKeepAlive=true \
-Dzookeeper.4lw.commands.whitelist=srvr,ruok,stat,mntr,cons \
-Dzookeeper.serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory \
-Dzookeeper.ssl.keyStore.location=/conf/ssl/zookeeper-quorum-server1.keystore.p12 \
-Dzookeeper.ssl.keyStore.password=NeloNELO123456789 \
-Dzookeeper.ssl.quorum.keyStore.location=/conf/ssl/zookeeper-quorum-server1.keystore.p12 \
-Dzookeeper.ssl.quorum.keyStore.password=NeloNELO123456789 \
-Dzookeeper.ssl.keyStore.type=PKCS12 \
-Dzookeeper.ssl.quorum.keyStore.type=PKCS12 \
-Dzookeeper.ssl.trustStore.location=/conf/ssl/zookeeper-quorum-server1.truststore.p12 \
-Dzookeeper.ssl.trustStore.password=NeloNELO123456789 \
-Dzookeeper.ssl.quorum.trustStore.location=/conf/ssl/zookeeper-quorum-server1.truststore.p12 \
-Dzookeeper.ssl.quorum.trustStore.password=NeloNELO123456789 \
-Dzookeeper.ssl.trustStore.type=PKCS12 \
-Dzookeeper.ssl.quorum.trustStore.type=PKCS12 \
-Dzookeeper.ssl.protocol=TLSv1.2,TLSv1.3 \
-Dzookeeper.ssl.quorum.protocol=TLSv1.2,TLSv1.3 \
-Dzookeeper.ssl.enabledProtocols=TLSv1.2,TLSv1.3 \
-Dzookeeper.ssl.quorum.enabledProtocols=TLSv1.2,TLSv1.3 \
-Dzookeeper.ssl.ciphersuites=TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 \
-Dzookeeper.ssl.quorum.ciphersuites=TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 \
-Dzookeeper.ssl.hostnameVerification=true \
-Dzookeeper.ssl.quorum.hostnameVerification=true \
-Dzookeeper.ssl.clientAuth=need \
-Dzookeeper.ssl.quorum.clientAuth=need \
-Dzookeeper.DigestAuthenticationProvider.enabled=false \
-Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider \
-Dzookeeper.superUser=SASL:admin/admin@NELO.COM,SASL:zookeeper-quorum-server/zookeeper-quorum-server1@NELO.COM,SASL:zookeeper-server/zookeeper-server1@NELO.COM \
-Dzookeeper.sessionRequireClientSASLAuth=true \
-Dzookeeper.enforce.auth.enabled=true \
-Dzookeeper.enforce.auth.schemes=sasl \
-Dzookeeper.kerberos.removeHostFromPrincipal=false \
-Dzookeeper.kerberos.removeRealmFromPrincipal=false \
-Dzookeeper.sasl.client=true \
-Dzookeeper.sasl.clientconfig=Client \
-Dzookeeper.server.principal=zookeeper-server/zookeeper-server1@NELO.COM \
-Dzookeeper.sasl.client.username=zookeeper-server \
-Dzookeeper.sasl.client.canonicalize.hostname=false \
-Dzookeeper.server.realm=NELO.COM \
-Dzookeeper.client.secure=true \
-Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty \
-Dzookeeper.client.certReload=true \
-Dsun.security.jgss.native=true \
-Dlog4j.configuration=file:/conf/log4j.properties \
-Djdk.tls.server.signatureSchemes=ecdsa_secp384r1_sha384,ecdsa_secp256r1_sha256 \
-Djavax.security.auth.message.MessagePolicy.debug=true \
-Djava.net.debug=all \
-Djava.security.krb5.realm=NELO.COM \
-Djava.security.krb5.kdc=krb5-kdc-server-nelo-com \
-Djava.security.auth.login.config=\${JAAS_CONF} \
-Djavax.security.sasl.server.authentication=krb5 \
-Djavax.security.sasl.qop=auth-conf,auth-int,auth \
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
