#!/bin/bash

set -e

function create_user_and_database() {
    local database=$1
    local username=$2
    local password=$3
    echo "Creating user '$username' and database '$database'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d "postgres" <<-EOSQL
        DO \$\$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_user WHERE usename = '$username') THEN
                CREATE USER $username WITH PASSWORD '$password';
            END IF;
        END
        \$\$;
        
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $username;
EOSQL
    echo "User '$username' and database '$database' created successfully"
}

echo "Starting database initialization..."
echo "METADATA_DATABASE_NAME: ${METADATA_DATABASE_NAME:-NOT SET}"
echo "CELERY_BACKEND_NAME: ${CELERY_BACKEND_NAME:-NOT SET}"
echo "ELT_DATABASE_NAME: ${ELT_DATABASE_NAME:-NOT SET}"

# Metadata database
if [ -n "${METADATA_DATABASE_NAME:-}" ]; then
    create_user_and_database "$METADATA_DATABASE_NAME" "$METADATA_DATABASE_USERNAME" "$METADATA_DATABASE_PASSWORD"
else
    echo "ERROR: METADATA_DATABASE_NAME not set"
fi

# Celery result backend database
if [ -n "${CELERY_BACKEND_NAME:-}" ]; then
    create_user_and_database "$CELERY_BACKEND_NAME" "$CELERY_BACKEND_USERNAME" "$CELERY_BACKEND_PASSWORD"
else
    echo "ERROR: CELERY_BACKEND_NAME not set"
fi

# ELT database
if [ -n "${ELT_DATABASE_NAME:-}" ]; then
    create_user_and_database "$ELT_DATABASE_NAME" "$ELT_DATABASE_USERNAME" "$ELT_DATABASE_PASSWORD"
else
    echo "ERROR: ELT_DATABASE_NAME not set"
fi

echo "All databases and users created successfully"