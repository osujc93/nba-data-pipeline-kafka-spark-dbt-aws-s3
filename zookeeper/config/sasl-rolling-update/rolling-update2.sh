#!/bin/bash

# rolling_upgrade.sh

sudo ln -s /root/.ssh/zookeeper_keytabs/zookeeper-quorum2.keytab /etc/krb5.keytab

# Path to zoo.cfg on the local node
ZOO_CFG_PATH="/opt/bitnami/zookeeper/conf/zoo.cfg"

NODE_NUM=2

# Settings for each step
STEP_SETTINGS=(
    "quorum.auth.enableSasl=true"
    "quorum.auth.learnerRequireSasl=true"
    "quorum.auth.serverRequireSasl=true"
)

# Logging (optional)
LOG_FILE="/var/log/zookeeper_rolling_upgrade.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=========================================="
echo "Starting Rolling Upgrade on Node ${NODE_NUM}"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

# Function to ensure the file ends with a newline
ensure_newline() {
    # Check if the last character is a newline
    if [ -n "$(tail -c1 "$ZOO_CFG_PATH")" ]; then
        echo "" >> "$ZOO_CFG_PATH"
        echo "Added missing newline to $ZOO_CFG_PATH"
    fi
}

# Function to set a setting in zoo.cfg
set_setting() {
    local setting="$1"
    local key
    key=$(echo "$setting" | cut -d'=' -f1)

    echo "Updating setting '${key}' on local node"

    if grep -q "^${key}=" "$ZOO_CFG_PATH"; then
        # Replace the existing setting with the new value
        sed -i "s|^${key}=.*|${setting}|" "$ZOO_CFG_PATH"
        echo "Updated existing setting '${key}'"
    else
        # Ensure the file ends with a newline before appending
        ensure_newline

        # Append the setting as a new line
        echo "${setting}" >> "$ZOO_CFG_PATH"
        echo "Appended new setting '${key}'"
    fi
}

client_port=2181
secure_client_port=2281
admin_port=7071
quorum_port=3888
metrics_port=7002

# Function to stop processes using a specific port
stop_process_using_port() {
    port=$1
    process_pid=$(lsof -ti tcp:$port)
    if [ -n "$process_pid" ]; then
        echo "Port $port is in use by process $process_pid. Stopping process..."
        kill -9 $process_pid
        echo "Process $process_pid stopped."
    else
        echo "Port $port is not in use."
    fi
}

# Function to restart ZooKeeper on the local node
restart_zookeeper() {
    echo "Restarting ZooKeeper on local node"

    /opt/bitnami/zookeeper/bin/zkServer.sh stop
    stop_process_using_port $client_port
    stop_process_using_port $secure_client_port
    stop_process_using_port $admin_port
    stop_process_using_port $metrics_port
    stop_process_using_port $quorum_port
    sleep 5
    /opt/bitnami/zookeeper/bin/zkServer.sh start
    sleep 45
}

# Function to verify ZooKeeper is up
verify_zookeeper() {
    echo "Verifying ZooKeeper on local node"

    local response=""
    local attempt=0
    local max_attempts=10
    local sleep_time=5

    while (( attempt < max_attempts )); do
        response=$(/opt/bitnami/zookeeper/bin/zkServer.sh status 2>/dev/null)

        if [[ "$response" == *"Mode:"* ]]; then
            echo "ZooKeeper on local node is up: $response"
            return 0
        else
            echo "Waiting for ZooKeeper on local node to be up... (Attempt $((attempt+1))/$max_attempts)"
            sleep "${sleep_time}"
            ((attempt++))
        fi
    done

    echo "ZooKeeper on local node did not start within expected time"
    return 1
}

# Function to update the local node's configuration and restart ZooKeeper
update_node() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Updating local node..."

    # Perform the update locally
    echo "Updating configuration files on local node..."

    # Check if the configuration files exist
    if [ ! -f "/opt/bitnami/zookeeper/conf/zoo-sasl-${NODE_NUM}.cfg" ] || [ ! -f "/usr/lib/jvm/java-17-openjdk-amd64/conf/security/java${NODE_NUM}.security" ]; then
        echo "Configuration files for local node are missing."
        exit 1
    fi

    # Replace the configuration files with node-specific versions
    cp "/opt/bitnami/zookeeper/conf/zoo-sasl-${NODE_NUM}.cfg" "$ZOO_CFG_PATH"
    cp "/usr/lib/jvm/java-17-openjdk-amd64/conf/security/java${NODE_NUM}.security" "/usr/lib/jvm/java-17-openjdk-amd64/conf/security/java.security"
    cp "/opt/bitnami/zookeeper/bin/zkEnv-sasl-${NODE_NUM}.sh" "/opt/bitnami/zookeeper/bin/zkEnv.sh"

    # Initialize Kerberos credential cache
    kinit -k -t /root/.ssh/zookeeper_keytabs/zookeeper-quorum2.keytab zookeeper-quorum-server/zookeeper-quorum-server2@EXAMPLE.COM

    # Verify the ticket
    klist -c /var/lib/zookeeper/krb5cc/zookeeper-cc_2

    sudo chown root:root /var/lib/zookeeper/krb5cc/zookeeper-cc_2

    # Restart ZooKeeper to apply SASL/TLS settings
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Restarting ZooKeeper with SASL/TLS enabled on local node..."
    restart_zookeeper

    # Check ZooKeeper status
    /opt/bitnami/zookeeper/bin/zkServer.sh status

    if [ $? -ne 0 ]; then
        echo "Error updating local node"
        exit 1
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S') - Local node updated successfully."
}

# Main script logic

# Update the local node's configuration and restart ZooKeeper
update_node

echo "$(date '+%Y-%m-%d %H:%M:%S') - Rolling upgrade completed successfully on the local node."
