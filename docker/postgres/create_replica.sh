#!/bin/bash
set -e

POSTGRES="psql --username postgres"

echo "Creating database: db_replica"

$POSTGRES <<EOSQL
CREATE DATABASE db_replica OWNER postgres;
EOSQL