#!/bin/bash

DB_TYPE=postgres

echo "Running schematool -info to check current schema status..."
METASTORE_INFO=$(schematool -dbType $DB_TYPE -info 2>&1)
SCHEMA_TOOL_EXIT=$?

echo "=== schematool -info output ==="
echo "$METASTORE_INFO"
echo "=== End of schematool -info output ==="

if [ $SCHEMA_TOOL_EXIT -ne 0 ] || \
   echo "$METASTORE_INFO" | grep -Eq "No current version available|Schema version not stored in the metastore|relation \"VERSION\" does not exist|Failed to get schema version"; then

  echo "Hive schema not initialized. Running initSchema..."
  if ! schematool -dbType $DB_TYPE -initSchema --verbose; then
    echo "ERROR: initSchema failed!"
    exit 1
  fi

else
  echo "Hive schema already initialized. Skipping initSchema."
fi

echo "Upgrading the Hive Metastore schema if needed..."
if ! schematool -dbType $DB_TYPE -upgradeSchema --verbose; then
  echo "Automatic schema upgrade failed, trying manual version specification..."
  if ! schematool -dbType $DB_TYPE -upgradeSchema; then
    echo "ERROR: manual upgrade failed too."
    exit 1
  fi
fi

echo "Hive Metastore schema initialization/upgrade script completed successfully."
