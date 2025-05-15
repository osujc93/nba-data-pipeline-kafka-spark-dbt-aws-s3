#!/bin/bash

# Exit on any error
set -e

# Script configuration
readonly CLUSTER_ID='123e4567-e89b-12d3-a456-426614174001'
readonly CLEAN_SHUTDOWN_CONTENT='{
    "version": 1,
    "brokerEpoch": 0,
    "timestamp": '$(date +%s)'
}'

# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Error handling
error_exit() {
    log "ERROR: $1"
    exit 1
}

# Get NODE_ID and DATA_DIR from server.properties
NODE_ID=$(grep -E '^node\.id=' /opt/kafka/config/server.properties | cut -d'=' -f2) || error_exit "Failed to get node.id"
DATA_DIR=$(grep -E '^log\.dirs=' /opt/kafka/config/server.properties | cut -d'=' -f2) || error_exit "Failed to get log.dirs"

# Validate required variables
if [ -z "${NODE_ID}" ] || [ -z "${DATA_DIR}" ]; then
    error_exit "NODE_ID and DATA_DIR must be set in server.properties"
fi

# Clear the data directory
if [ -d "${DATA_DIR}" ]; then
    log "Clearing the data directory: ${DATA_DIR}"
    rm -rf "${DATA_DIR:?}"/* || error_exit "Failed to clear the data directory. Check permissions."
else
    log "Data directory ${DATA_DIR} does not exist. Creating it..."
    mkdir -p "${DATA_DIR}" || error_exit "Failed to create the data directory. Check permissions."
fi

# Set proper permissions
log "Setting directory permissions..."
chmod -R 700 "${DATA_DIR}" || error_exit "Failed to set permissions on data directory"

# Create meta.properties file
log "Creating meta.properties file..."
cat <<EOF > "${DATA_DIR}/meta.properties"
version=1
cluster.id=${CLUSTER_ID}
node.id=${NODE_ID}
EOF
chmod 600 "${DATA_DIR}/meta.properties" || error_exit "Failed to set permissions on meta.properties"

# Create .kafka_cleanshutdown file with proper JSON content
log "Creating .kafka_cleanshutdown file..."
echo "${CLEAN_SHUTDOWN_CONTENT}" > "${DATA_DIR}/.kafka_cleanshutdown" || error_exit "Failed to create .kafka_cleanshutdown file"
chmod 600 "${DATA_DIR}/.kafka_cleanshutdown" || error_exit "Failed to set permissions on .kafka_cleanshutdown file"

# Verify the file was created correctly
if [ -f "${DATA_DIR}/.kafka_cleanshutdown" ]; then
    log "Successfully created .kafka_cleanshutdown file with content:"
    cat "${DATA_DIR}/.kafka_cleanshutdown"
else
    error_exit ".kafka_cleanshutdown file was not created successfully"
fi

# Set up metadata directory
log "Setting up metadata directory..."
mkdir -p /var/lib/kafka/metadata || error_exit "Failed to create metadata directory"
chmod 750 /var/lib/kafka/metadata || error_exit "Failed to set permissions on metadata directory"

log "Setup completed successfully:"
log "- Cluster ID: ${CLUSTER_ID}"
log "- Broker ID: ${NODE_ID}"
log "- Data Directory: ${DATA_DIR}"
