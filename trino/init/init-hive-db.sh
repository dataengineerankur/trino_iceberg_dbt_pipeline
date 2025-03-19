#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS hive;
    ALTER SCHEMA hive OWNER TO hive;
    GRANT ALL ON SCHEMA hive TO hive;
    GRANT ALL ON ALL TABLES IN SCHEMA hive TO hive;
EOSQL