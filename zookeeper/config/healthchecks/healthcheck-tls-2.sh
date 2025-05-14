#!/bin/bash

# Maximum number of attempts
MAX_ATTEMPTS=6

# Logging (optional)
LOG_FILE="/logs/health_check.log"
exec > >(tee -a "$LOG_FILE") 2>&1

# Function to log messages
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Check if Zookeeper is in a quorum
check_quorum() {
    log_message "Checking if Zookeeper is in a quorum..."

    output=$(/apache-zookeeper-3.8.4-bin/bin/zkServer.sh status)
    if [ -z "$output" ]; then
        log_message "No response from Zookeeper."
        return 1
    else
        log_message "Zookeeper quorum status: $output"
    fi
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

# Check connectivity to other nodes' quorum ports
check_connectivity() {
    log_message "Checking connectivity to other Zookeeper nodes on quorum ports..."

    # List of other nodes
    nodes=(
        "zookeeper1"
        "zookeeper2"
        "zookeeper3"
        "zookeeper4"
        "zookeeper5"
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

# Check mntr command on a specified node
check_mntr_command() {
    log_message "Executing mntr command on $(hostname)..."

    # Use netcat to send mntr command to the specified node on port 2181
    if echo "mntr" | sudo nc -zv "$(hostname)" 2181 > /dev/null 2>&1; then
        log_message "Successfully executed mntr command on $(hostname)."
    else
        log_message "Failed to execute mntr command on $(hostname)."
        return 1
    fi
}

# Run the checks
main_checks() {
    check_quorum && check_quorum_port && check_connectivity && check_mntr_command
}

# Retry mechanism
attempt=1
while [ $attempt -le $MAX_ATTEMPTS ]; do
    log_message "Attempt $attempt of $MAX_ATTEMPTS: Starting Zookeeper health checks..."
    
    if main_checks; then
        log_message "Zookeeper health check passed on attempt $attempt."
        exit 0
    else
        log_message "Zookeeper health check failed on attempt $attempt."
        if [ $attempt -lt $MAX_ATTEMPTS ]; then
            log_message "Retrying in 30 seconds..."
            sleep 30
        fi
    fi
    
    attempt=$((attempt + 1))
done

# If reached here, all attempts failed
log_message "Zookeeper health check failed after $MAX_ATTEMPTS attempts."
exit 1
