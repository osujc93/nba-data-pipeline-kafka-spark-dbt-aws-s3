#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define the new /etc/hosts content
sudo cat <<EOF | sudo tee /etc/hosts >/dev/null
127.0.0.1       localhost
::1             localhost ip6-localhost ip6-loopback
fe00::0         ip6-localnet
ff00::0         ip6-mcastprefix
ff02::1         ip6-allnodes
ff02::2         ip6-allrouters

# Redis
172.16.10.8   redis

# Postgres
172.16.10.9   postgres

# Kafka Brokers
172.16.10.1   kafka1
172.16.10.2   kafka2
172.16.10.3   kafka3
172.16.10.4   kafka4

# Kafka-UI
172.16.10.5   kafka-ui

# Prometheus
172.16.10.6   prometheus

# Grafana
172.16.10.7   grafana

# Airflow Components
172.16.10.10  airflow-webserver
172.16.10.11  airflow-scheduler
172.16.10.12  airflow-worker
172.16.10.13  airflow-triggerer
172.16.10.14  airflow-init
172.16.10.15  airflow-cli
172.16.10.25  airflow-exporter
172.16.10.16  flower

# Spark
172.16.10.40  spark-client
172.16.10.23  spark-history

# Trino
172.16.10.24  trino

# dbt
172.16.10.26  dbt-service

# MLflow
172.16.10.29  mlflow

# Superset
172.16.10.30  superset

# PGAdmin
172.16.10.31  pgadmin

# Hive
172.16.10.33  hive-metastore
172.16.10.34  hiveserver2

# Qdrant
172.16.10.45  qdrant

# Zookeeper Quorum Cluster
172.16.10.71  zookeeper1
172.16.10.72  zookeeper2
172.16.10.73  zookeeper3
172.16.10.74  zookeeper4
172.16.10.75  zookeeper5

# Zoonavigator
172.16.10.78  zoonavigator

# Hadoop & HDFS-HA
172.16.10.79  hadoop

# Journalnodes
172.16.10.50  journalnode1
172.16.10.51  journalnode2
172.16.10.52  journalnode3

# Namenodes
172.16.10.53  namenode1
172.16.10.54  namenode2

# Datanode
172.16.10.55  datanode1

# ResourceManager
172.16.10.56  resourcemanager
EOF

# Ensure /etc/iptables directory exists
if [ ! -d "/etc/iptables" ]; then
  sudo mkdir -p /etc/iptables
  echo "Created /etc/iptables directory."
fi

# Set default iptables policies
sudo iptables -P INPUT DROP
sudo iptables -P FORWARD DROP
sudo iptables -P OUTPUT ACCEPT
echo "Set default iptables policies to DROP for INPUT and FORWARD, ACCEPT for OUTPUT."

# Allow established and related incoming traffic (PRIORITIZED)
sudo iptables -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
echo "Allowed established and related incoming traffic."

# Allow inter-container traffic within the Docker subnet, including ephemeral ports
sudo iptables -A INPUT -s 172.16.10.0/24 -d 172.16.10.0/24 -j ACCEPT
sudo iptables -A INPUT -p tcp -s 172.16.10.0/24 --sport 32768:60999 -j ACCEPT
sudo iptables -A INPUT -p tcp -d 172.16.10.0/24 --dport 32768:60999 -j ACCEPT
sudo iptables -A FORWARD -s 172.16.10.0/24 -d 172.16.10.0/24 -j ACCEPT
echo "Allowed inter-container traffic and ephemeral port traffic within the Docker subnet."

# Allow specific ports for all traffic
# Gather all ports exposed in dockercompose.yaml (plus any standard ones you still need)
PORTS=(
  22
  88
  750
  6379
  5432
  9091 7001 9001 9002 9003
  9092 7002 9004 9005 9006
  9093 7003 9007 9008 9009
  9094 7004 9010 9011 9012
  8084
  9090
  3000
  8080
  8799
  5555
  18080
  8085
  5000
  8098
  80
  9083
  10002
  6333 6334
  2222 7011 2181 2888 3888
  2223 7012 2182 2889 3889
  2224 7013 2183 2890 3890
  2225 7014 2184 2891 3891
  2226 7015 2185 2892 3892
  9999
  8485 8480
  8481
  8486 8487 8482
  47070 8020
  47071 8021
  9864 50075 8042
  8032 8088
)

# Maximum number of ports per multiport rule
MAX_PORTS=15

# Split the ports into chunks of MAX_PORTS
for ((i=0; i<${#PORTS[@]}; i+=MAX_PORTS)); do
  CHUNK=$(printf "%s," "${PORTS[@]:i:MAX_PORTS}" | sed 's/,$//')
  sudo iptables -A INPUT -p tcp -m multiport --dports $CHUNK -j ACCEPT
  echo "Allowed IPv4 TCP traffic on ports: $CHUNK"
done

# Add logging rules for dropped packets AFTER ACCEPT rules
sudo iptables -N LOGGING
sudo iptables -A INPUT -j LOGGING
sudo iptables -A LOGGING -m limit --limit 5/min -j LOG --log-prefix "IPTables-Dropped: " --log-level 4
sudo iptables -A LOGGING -j DROP
echo "Added logging rules for dropped packets."

# Save the iptables rules for persistence
sudo iptables-save | sudo tee /etc/iptables/rules.v4 >/dev/null
echo "Saved iptables rules."

# Execute entrypoint command if passed
exec "$@"