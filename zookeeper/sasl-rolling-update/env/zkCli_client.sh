#!/usr/bin/env bash

# Set environment variable to indicate client context
export ZK_IS_CLIENT=true

# Source the zkenv.sh to set environment variables
source /apache-zookeeper-3.9.3-bin/bin/zkEnv.sh

# Invoke the standard zkCli.sh with all passed arguments
/apache-zookeeper-3.9.3-bin/bin/zkCli.sh "$@"