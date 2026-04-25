-- Create the pipeline database and schema during Postgres container initialization.
-- This script runs once when the Postgres data directory is initialized.

SELECT 'CREATE DATABASE stock_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'stock_db')\gexec
\connect stock_db
\i /docker-entrypoint-initdb.d/create_tables.sql
