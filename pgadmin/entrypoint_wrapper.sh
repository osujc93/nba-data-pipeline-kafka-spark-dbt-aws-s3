#!/usr/bin/env bash
set -e

# Substitute environment variables in /tmp/servers.json into /pgadmin4/servers.json
envsubst < /tmp/servers.json > /pgadmin4/servers.json

/venv/bin/python /pgadmin4/setup.py load-servers /pgadmin4/servers.json --user "${PGADMIN_DEFAULT_EMAIL}"

exec /entrypoint.sh "$@"
