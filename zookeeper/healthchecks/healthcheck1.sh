#!/bin/bash

# Maximum number of attempts
MAX_ATTEMPTS=6

DELAY=10

LOG_FILE="/logs/health_check.log"
exec > >(tee -a "$LOG_FILE") 2>&1

# Set ZK_IS_CLIENT to true to configure client JAAS settings
export ZK_IS_CLIENT=true

# Source the zkenv.sh to set environment variables for client
source /apache-zookeeper-3.8.5-bin/bin/zkEnv.sh

# Function to log messages
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Check if Zookeeper is in a quorum
check_quorum() {
    log_message "Checking if Zookeeper is in a quorum..."

    output=$(timeout 10 /apache-zookeeper-3.8.5-bin/bin/zkServer.sh status)
    if [ -z "$output" ]; then
        log_message "No response from Zookeeper."
        return 1
    fi
    log_message "Zookeeper quorum status: $output"
}

# Check if quorum port is listening locally
check_quorum_port() {
    log_message "Checking if quorum port 3888 is open locally..."

    # Get the IP address for this node
    local_ip=$(hostname -I | awk '{print $1}')

    # Use ss to check if port 3888 is listening on the specific IP address
    if ss -ltn | grep "$local_ip:3888" > /dev/null 2>&1; then
        log_message "Port 3888 is listening on IP $local_ip."
    else
        log_message "Port 3888 is not listening on IP $local_ip!"
        return 1
    fi
}

# Check if secure client port is listening locally
check_secure_client_port() {
    log_message "Checking if secure client port 2181 is open locally..."

    # Get the IP address for this node
    local_ip=$(hostname -I | awk '{print $1}')

    # Use ss to check if port 2181 is listening on the specific IP address
    if ss -ltn | grep "$local_ip:2181" > /dev/null 2>&1; then
        log_message "Port 2181 is listening on IP $local_ip."
    else
        log_message "Port 2181 is not listening on IP $local_ip!"
        return 1
    fi
}

# Check connectivity to other nodes' quorum ports
check_connectivity() {
    log_message "Checking connectivity to other Zookeeper nodes on quorum ports..."

    # List of other nodes
    nodes=(
        "zookeeper-quorum-server2"
        "zookeeper-quorum-server3"
        "zookeeper-quorum-learner4"
        "zookeeper-quorum-learner5"
    )

    # Ports to check
    ports=(3888)

    for node in "${nodes[@]}"; do
        for port in "${ports[@]}"; do
            if nc -z -w5 "$node" "$port"; then
                log_message "Successfully connected to $node on port $port."
            else
                log_message "Failed to connect to $node on port $port!"
                return 1
            fi
        done
    done
}

# Check mntr command using zkCli.sh with SASL/Kerberos authentication
check_mntr_command() {
    log_message "Executing mntr command on $(hostname) using zkCli.sh with SASL authentication..."

    # Execute the mntr command by piping it into zkCli.sh
    output=$(echo "stat" | /apache-zookeeper-3.8.5-bin/bin/zkCli_client.sh -server zookeeper-quorum-server1:2181,zookeeper-quorum-server2:2181,zookeeper-quorum-server3:2181,zookeeper-quorum-learner4:2181,zookeeper-quorum-learner5:2181 2>&1)

    if echo "$output" | grep -q "zk_version"; then
        log_message "Successfully executed mntr command on $(hostname) using zkCli.sh."
    else
        log_message "Failed to execute mntr command on $(hostname) using zkCli.sh!"
        log_message "zkCli.sh output: $output"
        return 1
    fi
}

# Run the checks
main() {
    check_quorum && \
    check_quorum_port && \
    check_secure_client_port && \
    check_connectivity && \
    check_mntr_command
}

# Retry Mechanism
attempt=1
while [ $attempt -le $MAX_ATTEMPTS ]; do
    log_message "Attempt $attempt of $MAX_ATTEMPTS: Starting Zookeeper health check."

    if main; then
        log_message "Zookeeper health check passed on attempt $attempt."
        exit 0
    else
        log_message "Zookeeper health check failed on attempt $attempt."
        if [ $attempt -lt $MAX_ATTEMPTS ]; then
            log_message "Retrying in $DELAY seconds..."
            sleep $DELAY
        fi
    fi

    attempt=$((attempt + 1))
done

# After all attempts have failed
log_message "Zookeeper health check failed after $MAX_ATTEMPTS attempts."
exit 1
