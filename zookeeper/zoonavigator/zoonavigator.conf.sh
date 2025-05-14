#!/usr/bin/env bash

if [ -n "$ZOONAVIGATOR_ZK_HOSTS" ]; then
  cat <<EOF
# Default Zookeeper connection using ZOONAVIGATOR_ZK_HOSTS
zoonavigator.connections.default = {
  conn = "${ZOONAVIGATOR_ZK_HOSTS}"
  auth = [ { scheme = "sasl", id = "Client" } ]
}
EOF
fi

if [ -n "$REQUEST_TIMEOUT_MILLIS" ]; then
  cat <<EOF
# Client will cancel requests that take longer than this
zoonavigator.requestTimeout = ${REQUEST_TIMEOUT_MILLIS} milliseconds

EOF
fi

if [ -n "$REQUEST_MAX_SIZE_KB" ]; then
  cat <<EOF
play.http.parser.maxMemoryBuffer = ${REQUEST_MAX_SIZE_KB}k

EOF
fi

if [ -n "$AUTO_CONNECT_CONNECTION_ID" ]; then
  CONN="CONNECTION_${AUTO_CONNECT_CONNECTION_ID}_CONN"

  # Throw error if connection string is not defined
  if [ -z "${!CONN}" ]; then
    >&2 cat <<EOF
Invalid Auto Connect configuration: connection '${AUTO_CONNECT_CONNECTION_ID}' not properly defined.
Make sure the following environment variable is set:
 - CONNECTION_${AUTO_CONNECT_CONNECTION_ID}_CONN

EOF
    exit 1
  fi

  cat <<EOF
# Optional auto connect
zoonavigator.autoConnect = "${AUTO_CONNECT_CONNECTION_ID}"

EOF
fi

# Extract all connection IDs from environment variables
CONNECTION_IDS=$(env | cut -f1 -d= | grep -E "CONNECTION_.*?_CONN" | sed -E "s/CONNECTION_(.*)_CONN/\1/g")

for ID in $CONNECTION_IDS; do
  NAME="CONNECTION_${ID}_NAME" # Optional connection name
  CONN="CONNECTION_${ID}_CONN"

  cat <<EOF
# Predefined connection ${ID}
zoonavigator.connections.${ID} = {
EOF

  # Add connection name if defined
  if [ -n "${!NAME}" ]; then
    cat <<EOF
  name = "${!NAME}"
EOF
  fi

  cat <<EOF
  conn = "${!CONN}"
  auth = [ { scheme = "sasl", id = "Client" } ]
}

EOF

  # Extract authentication schemes for the connection
  CONNECTION_AUTH_IDS=$(env | cut -f1 -d= | grep -E "CONNECTION_${ID}_AUTH_(.*)_SCHEME" | sed -E "s/CONNECTION_${ID}_AUTH_(.*)_SCHEME/\1/g")

  for AUTH_ID in $CONNECTION_AUTH_IDS; do
    SCHEME_NAME="CONNECTION_${ID}_AUTH_${AUTH_ID}_SCHEME"
    SCHEME_ID="CONNECTION_${ID}_AUTH_${AUTH_ID}_ID"

    # Throw error if scheme name or ID is not defined
    if [ -z "${!SCHEME_NAME}" ] || [ -z "${!SCHEME_ID}" ]; then
      >&2 cat <<EOF
Invalid Auth '${AUTH_ID}' configuration in Connection '${ID}'.
Make sure the following environment variables are set:
 - ${SCHEME_NAME}
 - ${SCHEME_ID}

EOF
      exit 1
    fi

    cat <<EOF
zoonavigator.connections.${ID}.auths.${AUTH_ID} = {
  scheme = "${!SCHEME_NAME}"
  id = "${!SCHEME_ID}"
}

zoonavigator.connections.${ID}.auth = \${zoonavigator.connections.${ID}.auth} [\${zoonavigator.connections.${ID}.auths.${AUTH_ID}}]

EOF
  done
done
