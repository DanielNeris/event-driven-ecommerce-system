#!/bin/sh
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  -- Order service
  CREATE USER order_user WITH PASSWORD 'order_pass' LOGIN;
  CREATE DATABASE order_db OWNER order_user;
  GRANT CONNECT ON DATABASE order_db TO order_user;
  GRANT ALL PRIVILEGES ON DATABASE order_db TO order_user;
  -- Inventory service
  CREATE USER inventory_user WITH PASSWORD 'inventory_pass' LOGIN;
  CREATE DATABASE inventory_db OWNER inventory_user;
  GRANT CONNECT ON DATABASE inventory_db TO inventory_user;
  GRANT ALL PRIVILEGES ON DATABASE inventory_db TO inventory_user;
  -- Payment service
  CREATE USER payment_user WITH PASSWORD 'payment_pass' LOGIN;
  CREATE DATABASE payment_db OWNER payment_user;
  GRANT CONNECT ON DATABASE payment_db TO payment_user;
  GRANT ALL PRIVILEGES ON DATABASE payment_db TO payment_user;
  -- Shipping service
  CREATE USER shipping_user WITH PASSWORD 'shipping_pass' LOGIN;
  CREATE DATABASE shipping_db OWNER shipping_user;
  GRANT CONNECT ON DATABASE shipping_db TO shipping_user;
  GRANT ALL PRIVILEGES ON DATABASE shipping_db TO shipping_user;
  -- Messageria service
  CREATE USER messageria_user WITH PASSWORD 'messageria_pass' LOGIN;
  CREATE DATABASE messageria_db OWNER messageria_user;
  GRANT CONNECT ON DATABASE messageria_db TO messageria_user;
  GRANT ALL PRIVILEGES ON DATABASE messageria_db TO messageria_user;
EOSQL
