CREATE SCHEMA IF NOT EXISTS analytics;

-- Enable FDW for cross-DB reads (CRM, Telemetry)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Foreign server: CRM
CREATE SERVER IF NOT EXISTS crm_fdw
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'crm_db', dbname 'crm_db', port '5432');

CREATE USER MAPPING IF NOT EXISTS FOR olap_user
  SERVER crm_fdw
  OPTIONS (user 'crm_user', password 'crm_password');

CREATE SCHEMA IF NOT EXISTS ext_crm;
IMPORT FOREIGN SCHEMA public
  LIMIT TO (customers)
  FROM SERVER crm_fdw INTO ext_crm;

-- Foreign server: Telemetry
CREATE SERVER IF NOT EXISTS telemetry_fdw
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'telemetry_db', dbname 'telemetry_db', port '5432');

CREATE USER MAPPING IF NOT EXISTS FOR olap_user
  SERVER telemetry_fdw
  OPTIONS (user 'telemetry_user', password 'telemetry_password');

CREATE SCHEMA IF NOT EXISTS ext_telemetry;
IMPORT FOREIGN SCHEMA public
  LIMIT TO (sensor_events)
  FROM SERVER telemetry_fdw INTO ext_telemetry;

-- Dimension: customers
CREATE TABLE IF NOT EXISTS analytics.dim_customers (
  customer_id INT PRIMARY KEY,
  device_id VARCHAR(64) UNIQUE NOT NULL,
  email VARCHAR(255) NOT NULL,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL
);

-- Fact: daily telemetry aggregated by customer
CREATE TABLE IF NOT EXISTS analytics.fact_daily_telemetry (
  customer_id INT NOT NULL,
  usage_date DATE NOT NULL,
  steps_sum BIGINT,
  avg_battery NUMERIC(5,2),
  load_avg NUMERIC(6,2),
  PRIMARY KEY (customer_id, usage_date)
);

-- View/reporting table combining dim and fact for fast API reads
CREATE OR REPLACE VIEW analytics.v_customer_daily_report AS
SELECT
  d.customer_id,
  d.device_id,
  d.email,
  d.first_name,
  d.last_name,
  f.usage_date,
  f.steps_sum,
  f.avg_battery,
  f.load_avg
FROM analytics.fact_daily_telemetry f
JOIN analytics.dim_customers d ON d.customer_id = f.customer_id;


