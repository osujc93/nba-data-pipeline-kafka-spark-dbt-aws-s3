#!/bin/bash

#sudo /conf/update_hosts.sh

sleep 10

sleep 5

sudo /conf/start-zookeeper3.sh

sleep 5

sudo /opt/healthcheck-tls.sh

#/conf/rolling-update.sh

sleep 5

sudo /apache-zookeeper-3.8.4-bin/bin/zkServer.sh status

sudo cat /logs/zookeeper.out

sudo grep -i -C 3 "sasl" /logs/zookeeper.out

sudo grep -i -C 3 "ERROR" /logs/zookeeper.out

sudo grep -i -C 3 "sasl" /logs/zookeeper.log

#cat /logs/zookeeper.log

#echo "ls /" | /opt/bitnami/zookeeper/bin/zkCli_client.sh -server 172.16.10.3:2281

# Keep the container running with an infinite loop
while :; do
    sleep 2073600
done