#!/bin/sh
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER order_user WITH PASSWORD 'order_pass';
  CREATE DATABASE order_db OWNER order_user;
  CREATE USER inventory_user WITH PASSWORD 'inventory_pass';
  CREATE DATABASE inventory_db OWNER inventory_user;
  CREATE USER payment_user WITH PASSWORD 'payment_pass';
  CREATE DATABASE payment_db OWNER payment_user;
  CREATE USER shipping_user WITH PASSWORD 'shipping_pass';
  CREATE DATABASE shipping_db OWNER shipping_user;
  CREATE USER messageria_user WITH PASSWORD 'messageria_pass';
  CREATE DATABASE messageria_db OWNER messageria_user;
EOSQL
