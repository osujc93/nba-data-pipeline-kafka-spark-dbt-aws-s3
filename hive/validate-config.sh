#!/bin/bash

HIVE_CONFIG_FILE="${HIVE_HOME}/conf/hive-site.xml"

check_property() {
    local property_name=$1
    local config_file=$2
    local property_value

    property_value=$(xmlstarlet sel -t -v "//property[name='$property_name']/value" "$config_file")

    if [ -z "$property_value" ]; then
        echo "Error: Property '${property_name}' in ${config_file} is not set or is empty."
        exit 1
    else
        echo "Property ${property_name} in ${config_file} is set to ${property_value}."
    fi
}

validate_jdbc_url() {
    local config_file=$1
    local jdbc_url

    jdbc_url=$(xmlstarlet sel -t -v "//property[name='javax.jdo.option.ConnectionURL']/value" "$config_file")
    if [[ ! "$jdbc_url" =~ ^jdbc:postgresql:// ]]; then
        echo "Error: Invalid JDBC URL format in ${config_file}. Must start with 'jdbc:postgresql://'."
        exit 1
    else
        echo "JDBC URL format in ${config_file} is valid."
    fi
}

validate_jdbc_driver() {
    local config_file=$1
    local driver_name

    driver_name=$(xmlstarlet sel -t -v "//property[name='javax.jdo.option.ConnectionDriverName']/value" "$config_file")
    if [[ "$driver_name" != "org.postgresql.Driver" ]]; then
        echo "Error: Invalid JDBC driver in ${config_file}. Must be 'org.postgresql.Driver'."
        exit 1
    else
        echo "JDBC driver in ${config_file} is valid."
    fi
}

echo "Validating hive-site.xml ..."

# 1) Check Metastore properties
check_property "javax.jdo.option.ConnectionURL" "$HIVE_CONFIG_FILE"
check_property "javax.jdo.option.ConnectionDriverName" "$HIVE_CONFIG_FILE"
check_property "javax.jdo.option.ConnectionUserName" "$HIVE_CONFIG_FILE"
check_property "javax.jdo.option.ConnectionPassword" "$HIVE_CONFIG_FILE"

validate_jdbc_url "$HIVE_CONFIG_FILE"
validate_jdbc_driver "$HIVE_CONFIG_FILE"

# 2) Check HiveServer2 properties
check_property "hive.server2.transport.mode" "$HIVE_CONFIG_FILE"
check_property "hive.server2.thrift.port" "$HIVE_CONFIG_FILE"
check_property "hive.server2.thrift.bind.host" "$HIVE_CONFIG_FILE"

echo "All necessary properties (Metastore + HiveServer2) appear to be set correctly."
