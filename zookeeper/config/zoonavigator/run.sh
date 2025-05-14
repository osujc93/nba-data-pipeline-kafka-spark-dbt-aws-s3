#!/usr/bin/env sh

# Exit immediately if a command exits with a non-zero status
set -e

# Remove existing PID file to prevent startup issues
rm -f ./RUNNING_PID

# Ensure SERVER_NAME is set; exit if not
if [ -z "$SERVER_NAME" ]; then
  echo "Error: SERVER_NAME is not set. Exiting."
  exit 1
fi

# Define ROOT_SERVER_PATH using SERVER_NAME
ROOT_SERVER_PATH="/app/${SERVER_NAME}-server"

# Export ROOT_SERVER_PATH so it's available to child processes
export ROOT_SERVER_PATH

# Optionally, you can log the ROOT_SERVER_PATH for debugging
echo "ROOT_SERVER_PATH is set to: $ROOT_SERVER_PATH"

# Generate a random secret key if not already set
export SECRET_KEY=${SECRET_KEY:-$(tr -dc 'a-zA-Z0-9~!@#$%^&*_-' < /dev/urandom | head -c 64)}

# Define Zookeeper JAR version for Bitnami Zookeeper 3.9.2
ZOOKEEPER_392_JAR="zookeeper-3.9.2.jar"
ZOOKEEPER_JAR="zookeeper.jar"

# Ensure the Zookeeper JAR exists in the 'extra' directory
if [ ! -f "/app/extra/${ZOOKEEPER_392_JAR}" ]; then
  echo "Zookeeper client JAR (${ZOOKEEPER_392_JAR}) not found in '/app/extra/' directory."
  exit 1
fi

# Set Java options directly without using environment variables
JAVA_OPTS="$JAVA_OPTS \
  -XX:+UseContainerSupport \
  -server \
  -Dzookeeper.kinit=/usr/bin/kinit \
  -Dplay.assets.path=/public \
  -Dplay.assets.urlPrefix=/ \
  -Djava.security.auth.login.config=/app/jaas_client.conf \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -Dsun.security.krb5.debug=true \
  -Djavax.security.auth.useSubjectCredsOnly=false \
  -Dzookeeper.sasl.clientconfig=Client \
  -Dzookeeper.client.secure=true \
  -Dzookeeper.sasl.client=true \
  -Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty \
  -Dzookeeper.ssl.keyStore.location=/opt/bitnami/zookeeper/conf/ssl/zoonavigator.keystore.p12 \
  -Dzookeeper.ssl.keyStore.password=NeloNELO123456789 \
  -Dzookeeper.ssl.trustStore.location=/opt/bitnami/zookeeper/conf/ssl/zoonavigator.truststore.p12 \
  -Dzookeeper.ssl.trustStore.password=NeloNELO123456789 \
  -Dzookeeper.ssl.enabledProtocols=TLSv1.3 \
  -Djavax.net.debug=ssl:handshake:verbose:trustmanager:session:record:certpath:keymanager \
  -Dzookeeper.debug=true \
  -Djava.net.preferIPv4Stack=true \
  -Djava.net.preferIPv6Addresses=false \
  -Dzookeeper.tcpKeepAlive=true \
  -Dzookeeper.serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory \
  -Dzookeeper.serverCnxn.netty.receiveBufferSize=3145728 \
  -Dzookeeper.serverCnxn.netty.sendBufferSize=3145728 \
  -Dzookeeper.clientCnxn.netty.receiveBufferSize=3145728 \
  -Dzookeeper.clientCnxn.netty.sendBufferSize=3145728 \
  -Dzookeeper.quorumCnxn.netty.receiveBufferSize=3145728 \
  -Dzookeeper.quorumCnxn.netty.sendBufferSize=3145728 \
  -Djavax.net.ssl.keyStoreType=PKCS12 \
  -Djavax.net.ssl.trustStoreType=PKCS12 \
  -Dsun.security.krb5.rcache=none \
  -Dsun.security.ssl.debug=all \
  -Djavax.security.auth.debug=all \
  -Djava.security.debug=logincontext,configfile,gssloginconfig,accesscontrol,access,failure,configparser,configfileparser \
  -Dsun.security.jgss.useSubjectCredsOnly=false \
  -Dsun.security.jgss.native=true \
  -Dio.netty.tryReflectionSetAccessible=true \
  -Dsun.security.jgss.debug=true \
  -Dsun.security.spnego.debug=true \
  -Dsun.security.jgss.krb5.debug=true \
  -Dsun.security.krb5.encryption.types=aes256-cts-hmac-sha1-96,aes128-cts-hmac-sha1-96,aes256-cts-hmac-sha384-192,aes128-cts-hmac-sha256-128 \
  -Djavax.net.ssl.hostnameVerification=true \
  -Djava.security.egd=file:/dev/urandom \
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
  --add-modules=java.security.jgss,java.security.sasl,jdk.security.auth,java.naming \
  -Djdk.tls.server.signatureSchemes=rsa_pss_rsae_sha256,rsa_pss_rsae_sha384,rsa_pss_rsae_sha512,rsa_pkcs1_sha256,rsa_pkcs1_sha384,rsa_pkcs1_sha512 \
  -Djavax.security.auth.message.MessagePolicy.debug=true \
  -Dzookeeper.kerberos.servicePrincipal=zoonavigator/zoonavigator@EXAMPLE.COM \
  -Dsun.security.krb5.ktname=/etc/krb5kdc/keytabs/zoonavigator.keytab"

# Update Zookeeper JAR to 3.9.2
cp /app/extra/${ZOOKEEPER_392_JAR} /app/lib/${ZOOKEEPER_JAR}

# Generate application configuration by appending zoonavigator.conf.sh
/app/conf/zoonavigator.conf.sh >> /app/conf/application.conf

# Start the Zoonavigator application with the defined Java options
exec ./bin/zoonavigator-play ${JAVA_OPTS}