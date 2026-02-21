#!/bin/bash
# superset_init.sh

set -e

# [ADDED for Debugging]
# Echo each command so we see exactly where it fails
set -x

# Provide default values for admin user if they aren't set externally
: "${ADMIN_USERNAME:=admin}"
: "${ADMIN_PASSWORD:=admin}"
: "${ADMIN_FIRSTNAME:=Admin}"
: "${ADMIN_LASTNAME:=User}"
: "${ADMIN_EMAIL:=admin@example.com}"

if [ "$1" = "superset" ]; then
  echo "Initializing the Superset environment..."

  echo "Upgrading the Superset DB..."
  superset db upgrade

  echo "Applying role and permission sync..."
  superset init

  echo "Creating admin user..."
  superset fab create-admin \
    --username "${ADMIN_USERNAME}" \
    --firstname "${ADMIN_FIRSTNAME}" \
    --lastname "${ADMIN_LASTNAME}" \
    --email "${ADMIN_EMAIL}" \
    --password "${ADMIN_PASSWORD}"

  datasource_config_path="/app/config/datasource_config.yaml"
  if [ -f "${datasource_config_path}" ]; then
    echo "Importing datasources from ${datasource_config_path}..."
    superset import-datasources --path "${datasource_config_path}"
  else
    echo "No datasource_config.yaml found, skipping import-datasources."
  fi

  echo "Starting the Superset server..."
  # Gunicorn logging in DEBUG mode
  exec gunicorn \
    -b 0.0.0.0:8098 \
    --log-level debug \
    "superset.app:create_app()"

else
  exec "$@"
fi
