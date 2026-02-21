#!/bin/bash

#
# https://airflow.apache.org/docs/apache-airflow/stable/_images/airflow_erd.svg


set -e

# Define the path to the .env file
ENV_FILE="/docker-entrypoint-initdb.d/.env"

# Load environment variables from .env file
if [ -f "$ENV_FILE" ]; then
  export $(grep -v '^#' "$ENV_FILE" | xargs)
else
  echo "Error: .env file not found at $ENV_FILE."
  exit 1
fi

# Set environment variables with defaults if not set
export POSTGRES_USER="${POSTGRES_USER:-nelonba}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-Password123456789}"
export POSTGRES_DB="${POSTGRES_DB:-nelonba}"
export POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_ROLE="${POSTGRES_ROLE:-nelonba}"
export PGPASSWORD="${POSTGRES_PASSWORD}"

# Ensure required environment variables are set
if [ -z "$POSTGRES_USER" ]; then
  echo "Error: POSTGRES_USER is not set."
  exit 1
fi

if [ -z "$POSTGRES_PASSWORD" ]; then
  echo "Error: POSTGRES_PASSWORD is not set."
  exit 1
fi

if [ -z "$POSTGRES_DB" ]; then
  POSTGRES_DB=$POSTGRES_USER
fi

# Define the path to PostgreSQL binaries
export PATH=$PATH:/usr/lib/postgresql/16/bin

# Set the PGDATA environment variable if not already set
: "${PGDATA:=/var/lib/postgresql/data}"

# Ensure the script has execute permissions
if [ ! -x "$0" ]; then
  chmod +x "$0"
fi

# Create the data directory if it doesn't exist
if [ ! -d "$PGDATA" ]; then
  mkdir -p "$PGDATA"
  chown postgres:postgres "$PGDATA"
fi

# Ensure proper ownership of the data directory
chown -R postgres:postgres "$PGDATA"

# Perform PostgreSQL configuration changes
# Update postgresql.conf to listen on all addresses
sed -i "s/^#listen_addresses = .*/listen_addresses = '*'/" "$PGDATA/postgresql.conf"

# Update pg_hba.conf to allow connections from all addresses
echo "host all all 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"
echo "host all all ::/0 md5" >> "$PGDATA/pg_hba.conf"

# Function to initialize a database with its SQL script
initialize_database() {
  local db_name=$1
  local sql_script=$2

  echo "Initializing database: $db_name"

  # Create the database if it doesn't exist
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tc "SELECT 1 FROM pg_database WHERE datname = '$db_name';" | grep -q 1 || psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE \"$db_name\" OWNER \"$POSTGRES_USER\";"

  # Execute the SQL script
  psql -U "$POSTGRES_USER" -d "$db_name" -f "$sql_script"
}

# Function to grant privileges on a database
grant_privileges() {
  local db_name=$1
  psql -U "$POSTGRES_USER" -d "$db_name" -c "GRANT ALL PRIVILEGES ON DATABASE \"$db_name\" TO ${POSTGRES_ROLE};"
  psql -U "$POSTGRES_USER" -d "$db_name" -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_ROLE};"
  psql -U "$POSTGRES_USER" -d "$db_name" -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${POSTGRES_ROLE};"
}

# Create the specified role if it doesn't exist and set the password
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tc "SELECT 1 FROM pg_roles WHERE rolname= '${POSTGRES_ROLE}';" | grep -q 1 || psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE ROLE ${POSTGRES_ROLE} WITH LOGIN PASSWORD '${POSTGRES_PASSWORD}';"

# Grant the role the necessary permissions
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "ALTER ROLE ${POSTGRES_ROLE} CREATEDB;"
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_DB TO ${POSTGRES_ROLE};"

# Grant privileges on the primary database
grant_privileges "$POSTGRES_DB"

# Initialize nelonba Database
cat <<EOF > /tmp/nelonba.sql
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL ON SCHEMA public TO public;

-- Create other required tables for nelonba
CREATE TABLE IF NOT EXISTS registered_model (
    name VARCHAR(256) PRIMARY KEY,
    creation_time TIMESTAMP,
    last_updated_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS registered_model_tag (
    name VARCHAR(256) NOT NULL,
    key VARCHAR(250) NOT NULL,
    value VARCHAR(250),
    PRIMARY KEY (name, key)
);

CREATE TABLE IF NOT EXISTS model_version (
    name VARCHAR(256) NOT NULL,
    version BIGINT NOT NULL,
    creation_time TIMESTAMP,
    last_updated_time TIMESTAMP,
    description TEXT,
    user_id VARCHAR(256),
    current_stage VARCHAR(20),
    source VARCHAR(512),
    run_id VARCHAR(32),
    status VARCHAR(20),
    status_message VARCHAR(500),
    PRIMARY KEY (name, version)
);

CREATE TABLE IF NOT EXISTS model_version_tag (
    name VARCHAR(256) NOT NULL,
    version BIGINT NOT NULL,
    key VARCHAR(250) NOT NULL,
    value VARCHAR(250),
    PRIMARY KEY (name, version, key)
);

CREATE TABLE IF NOT EXISTS model_version_alias (
    name VARCHAR(256) NOT NULL,
    version BIGINT NOT NULL,
    alias VARCHAR(64) NOT NULL,
    PRIMARY KEY (name, alias)
);

CREATE TABLE IF NOT EXISTS experiments_tag (
    experiment_id BIGINT NOT NULL,
    key VARCHAR(250) NOT NULL,
    value VARCHAR(500),
    PRIMARY KEY (experiment_id, key)
);

CREATE TABLE IF NOT EXISTS run (
    run_uuid VARCHAR(32) PRIMARY KEY,
    name VARCHAR(250),
    source_type VARCHAR(20),
    source_name VARCHAR(500),
    entry_point_name VARCHAR(50),
    user_id VARCHAR(256),
    status VARCHAR(20),
    start_time BIGINT,
    end_time BIGINT,
    source_version VARCHAR(50),
    lifecycle_stage VARCHAR(20),
    artifact_uri VARCHAR(200),
    experiment_id BIGINT
);

CREATE TABLE IF NOT EXISTS run_tag (
    run_uuid VARCHAR(32) NOT NULL,
    key VARCHAR(250) NOT NULL,
    value VARCHAR(250),
    PRIMARY KEY (run_uuid, key)
);

CREATE TABLE IF NOT EXISTS params (
    run_uuid VARCHAR(32) NOT NULL,
    key VARCHAR(250) NOT NULL,
    value VARCHAR(250),
    PRIMARY KEY (run_uuid, key)
);

CREATE TABLE IF NOT EXISTS metrics (
    key VARCHAR(250) NOT NULL,
    value DOUBLE PRECISION,
    timestamp BIGINT,
    step BIGINT,
    is_nan BOOLEAN,
    run_uuid VARCHAR(32) NOT NULL,
    PRIMARY KEY (run_uuid, key, timestamp, step)
);

CREATE TABLE IF NOT EXISTS tags (
    run_uuid VARCHAR(32) NOT NULL,
    key VARCHAR(250) NOT NULL,
    value VARCHAR(500),
    PRIMARY KEY (run_uuid, key)
);

CREATE TABLE IF NOT EXISTS latest_metrics (
    run_uuid VARCHAR(32) NOT NULL,
    key VARCHAR(250) NOT NULL,
    value DOUBLE PRECISION,
    timestamp BIGINT,
    step BIGINT,
    is_nan BOOLEAN,
    PRIMARY KEY (run_uuid, key)
);

-- Grant privileges on all tables and sequences
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${POSTGRES_ROLE};

-- Grant usage and select/update on all sequences
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public') LOOP
        EXECUTE 'GRANT USAGE, SELECT, UPDATE ON SEQUENCE ' || quote_ident(r.sequence_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;

-- Grant execute on all functions
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'public') LOOP
        EXECUTE 'GRANT EXECUTE ON FUNCTION ' || quote_ident(r.routine_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;
EOF

initialize_database "nelonba" "/tmp/nelonba.sql"

# Initialize Hive Metastore Database
cat <<EOF > /tmp/hive.sql
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL ON SCHEMA public TO public;

-- Grant privileges on all tables and sequences
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${POSTGRES_ROLE};

-- Grant usage and select/update on all sequences
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public') LOOP
        EXECUTE 'GRANT USAGE, SELECT, UPDATE ON SEQUENCE ' || quote_ident(r.sequence_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;

-- Grant execute on all functions
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'public') LOOP
        EXECUTE 'GRANT EXECUTE ON FUNCTION ' || quote_ident(r.routine_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;
EOF

initialize_database "hive" "/tmp/hive.sql"

psql -U "${POSTGRES_USER}" -d "hive" -f "/docker-entrypoint-initdb.d/hives-schema-4.0.0.postgres.sql"

# Initialize Trino Database
cat <<EOF > /tmp/trino.sql
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL ON SCHEMA public TO public;

CREATE TABLE IF NOT EXISTS trino_query (
    query_id VARCHAR(50) PRIMARY KEY,
    state VARCHAR(50),
    "user" VARCHAR(50),
    started TIMESTAMP,
    finished TIMESTAMP,
    query TEXT,
    output VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS trino_resource_group (
    resource_group_id VARCHAR(50) PRIMARY KEY,
    resource_group_name VARCHAR(100),
    soft_memory_limit VARCHAR(20),
    soft_concurrency_limit INT,
    hard_concurrency_limit INT
);

CREATE TABLE IF NOT EXISTS trino_session (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    start_time TIMESTAMP,
    last_access_time TIMESTAMP
);

-- Grant privileges on all tables and sequences
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${POSTGRES_ROLE};

-- Grant usage and select/update on all sequences
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public') LOOP
        EXECUTE 'GRANT USAGE, SELECT, UPDATE ON SEQUENCE ' || quote_ident(r.sequence_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;

-- Grant execute on all functions
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'public') LOOP
        EXECUTE 'GRANT EXECUTE ON FUNCTION ' || quote_ident(r.routine_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;
EOF

initialize_database "trino" "/tmp/trino.sql"

# Initialize Superset Database
cat <<EOF > /tmp/superset.sql
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL ON SCHEMA public TO public;

CREATE TABLE IF NOT EXISTS my_slices (
    id SERIAL PRIMARY KEY,
    slice_name VARCHAR(250) NOT NULL,
    datasource_type VARCHAR(200),
    viz_type VARCHAR(200),
    params TEXT,
    description TEXT,
    cache_timeout INT,
    created_on TIMESTAMPTZ,
    changed_on TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS my_dashboards (
    id SERIAL PRIMARY KEY,
    dashboard_title VARCHAR(250) NOT NULL,
    slug VARCHAR(200),
    position_json TEXT,
    json_metadata TEXT,
    published BOOLEAN,
    created_on TIMESTAMPTZ,
    changed_on TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS my_logs (
    id SERIAL PRIMARY KEY,
    action VARCHAR(200),
    user_id INT,
    dashboard_id INT,
    slice_id INT,
    dttm TIMESTAMPTZ,
    json TEXT
);

-- Grant privileges on all tables and sequences
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${POSTGRES_ROLE};

-- Grant usage and select/update on all sequences
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public') LOOP
        EXECUTE 'GRANT USAGE, SELECT, UPDATE ON SEQUENCE ' || quote_ident(r.sequence_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;

-- Grant execute on all functions
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'public') LOOP
        EXECUTE 'GRANT EXECUTE ON FUNCTION ' || quote_ident(r.routine_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;
EOF

initialize_database "superset" "/tmp/superset.sql"

# Initialize MLflow Database
cat <<EOF > /tmp/mlflow.sql
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL ON SCHEMA public TO public;

-- Grant privileges on all tables and sequences
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${POSTGRES_ROLE};

-- Grant usage and select/update on all sequences
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public') LOOP
        EXECUTE 'GRANT USAGE, SELECT, UPDATE ON SEQUENCE ' || quote_ident(r.sequence_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;

-- Grant execute on all functions
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'public') LOOP
        EXECUTE 'GRANT EXECUTE ON FUNCTION ' || quote_ident(r.routine_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;
EOF

initialize_database "mlflow" "/tmp/mlflow.sql"

# Initialize Spark History Server Database
cat <<EOF > /tmp/spark.sql
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL ON SCHEMA public TO public;

CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255),
    submission_time TIMESTAMPTZ,
    completion_time TIMESTAMPTZ,
    state VARCHAR(255),
    num_tasks INT,
    num_active_tasks INT,
    num_completed_tasks INT,
    num_skipped_tasks INT,
    num_failed_tasks INT,
    num_active_stages INT,
    num_completed_stages INT,
    num_skipped_stages INT,
    num_failed_stages INT
);

CREATE TABLE IF NOT EXISTS stages (
    stage_id VARCHAR(36) PRIMARY KEY,
    job_id VARCHAR(36) REFERENCES jobs (job_id),
    submission_time TIMESTAMPTZ,
    completion_time TIMESTAMPTZ,
    state VARCHAR(255),
    num_tasks INT,
    num_active_tasks INT,
    num_completed_tasks INT,
    num_skipped_tasks INT,
    num_failed_tasks INT
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id VARCHAR(36) PRIMARY KEY,
    stage_id VARCHAR(36) REFERENCES stages (stage_id),
    index INT,
    attempt INT,
    launch_time TIMESTAMPTZ,
    finish_time TIMESTAMPTZ,
    duration BIGINT,
    executor_id VARCHAR(255),
    host VARCHAR(255),
    status VARCHAR(255),
    locality VARCHAR(255),
    speculative BOOLEAN,
    failed BOOLEAN
);

CREATE TABLE IF NOT EXISTS task_metrics (
    task_id VARCHAR(36) PRIMARY KEY REFERENCES tasks (task_id),
    executor_deserialize_time BIGINT,
    executor_run_time BIGINT,
    result_size BIGINT,
    jvm_gc_time BIGINT,
    result_serialization_time BIGINT,
    memory_bytes_spilled BIGINT,
    disk_bytes_spilled BIGINT,
    peak_execution_memory BIGINT
);

CREATE TABLE IF NOT EXISTS executors (
    executor_id VARCHAR(255) PRIMARY KEY,
    host VARCHAR(255),
    total_cores INT,
    max_memory BIGINT,
    start_time TIMESTAMPTZ,
    finish_time TIMESTAMPTZ,
    total_tasks INT,
    total_duration BIGINT,
    total_input_bytes BIGINT,
    total_shuffle_read BIGINT,
    total_shuffle_write BIGINT,
    total_failed_tasks INT
);

-- Grant privileges on all tables and sequences
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${POSTGRES_ROLE};

-- Grant usage and select/update on all sequences
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public') LOOP
        EXECUTE 'GRANT USAGE, SELECT, UPDATE ON SEQUENCE ' || quote_ident(r.sequence_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;

-- Grant execute on all functions
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'public') LOOP
        EXECUTE 'GRANT EXECUTE ON FUNCTION ' || quote_ident(r.routine_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;
EOF

initialize_database "spark" "/tmp/spark.sql"

# Initialize dbt Database
cat <<EOF > /tmp/dbt.sql
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL ON SCHEMA public TO public;

CREATE TABLE IF NOT EXISTS dbt_models (
    model_id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    schema_name VARCHAR(100),
    description TEXT,
    status VARCHAR(50),
    created_on TIMESTAMPTZ,
    updated_on TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS dbt_runs (
    run_id SERIAL PRIMARY KEY,
    model_id INT REFERENCES dbt_models (model_id),
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    status VARCHAR(50),
    created_on TIMESTAMPTZ,
    updated_on TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS dbt_run_artifacts (
    artifact_id SERIAL PRIMARY KEY,
    run_id INT REFERENCES dbt_runs (run_id),
    artifact_type VARCHAR(50),
    artifact_path TEXT,
    created_on TIMESTAMPTZ,
    updated_on TIMESTAMPTZ
);

-- Grant privileges on all tables and sequences
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_ROLE};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${POSTGRES_ROLE};

-- Grant usage and select/update on all sequences
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public') LOOP
        EXECUTE 'GRANT USAGE, SELECT, UPDATE ON SEQUENCE ' || quote_ident(r.sequence_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;

-- Grant execute on all functions
DO \$\$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'public') LOOP
        EXECUTE 'GRANT EXECUTE ON FUNCTION ' || quote_ident(r.routine_name) || ' TO ${POSTGRES_ROLE};';
    END LOOP;
END \$\$;
EOF

initialize_database "dbt" "/tmp/dbt.sql"

cat <<EOF > /tmp/incremental.sql
CREATE TABLE IF NOT EXISTS nba_ingestion_metadata (
    id SERIAL PRIMARY KEY,
    last_ingestion_date DATE NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
EOF

initialize_database "nelonba" "/tmp/incremental.sql"

echo "All databases have been initialized successfully."
