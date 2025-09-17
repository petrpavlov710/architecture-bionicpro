from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


COMMON_SCRIPT = r"""
    set -euo pipefail

    CH_URL="${CH_URL:-http://clickhouse:8123}"
    CH_USER="${CH_USER:-}"
    CH_PASS="${CH_PASS:-}"
    AUTH_ARGS=()
    if [ -n "${CH_USER}" ] || [ -n "${CH_PASS}" ]; then
      AUTH_ARGS=(-u "${CH_USER}:${CH_PASS}")
    fi

    run_ch_get() {
      local query="$1"
      echo "→ GET: ${query}" 1>&2
      local tmp code
      tmp="$(mktemp)"
      code=$(curl -sS -w "%{http_code}" --get "${CH_URL}" \
              "${AUTH_ARGS[@]}" \
              --data-urlencode "query=${query}" \
              -o "$tmp" || echo "000")
      echo "---- ClickHouse response (HTTP ${code}) ----"
      cat "$tmp" || true; echo; echo "-------------------------------------------"
      if [ "${code}" -ge 400 ] || [ "${code}" = "000" ]; then
        echo "ERROR (GET): HTTP ${code} for query: ${query}" 1>&2
        exit 1
      fi
      rm -f "$tmp"
    }

    run_ch_post() {
      local query="$1"
      echo "→ POST: ${query}" 1>&2
      local tmp code
      tmp="$(mktemp)"
      code=$(curl -sS -w "%{http_code}" -X POST "${CH_URL}" \
              -H 'Content-Type: text/plain; charset=UTF-8' \
              "${AUTH_ARGS[@]}" \
              --data-binary "${query}" \
              -o "$tmp" || echo "000")
      echo "---- ClickHouse response (HTTP ${code}) ----"
      cat "$tmp" || true; echo; echo "-------------------------------------------"
      if [ "${code}" -ge 400 ] || [ "${code}" = "000" ]; then
        echo "ERROR (POST): HTTP ${code} for query: ${query}" 1>&2
        exit 1
      fi
      rm -f "$tmp"
    }
"""


with DAG(
    dag_id='reports_etl_dag',
    default_args=default_args,
    description='ETL: Load CRM + Telemetry into OLAP and build reporting mart',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ch_init = BashOperator(
        task_id='ch_init',
        bash_command=COMMON_SCRIPT + r"""
            run_ch_get "SELECT version()"

            run_ch_post "CREATE DATABASE IF NOT EXISTS analytics"

            run_ch_post $'CREATE TABLE IF NOT EXISTS analytics.dim_customers (
              customer_id Int32,
              device_id String,
              email String,
              first_name String,
              last_name String
            ) ENGINE = MergeTree
            ORDER BY customer_id'

            run_ch_post $'CREATE TABLE IF NOT EXISTS analytics.fact_daily_telemetry (
              customer_id Int32,
              usage_date Date,
              steps_sum Int64,
              avg_battery Decimal(5,2),
              load_avg Decimal(6,2)
            ) ENGINE = MergeTree
            ORDER BY (customer_id, usage_date)'

            run_ch_post $'CREATE TABLE IF NOT EXISTS analytics.processed_watermark (
              id UInt8,
              last_processed_date Date
            ) ENGINE = MergeTree
            ORDER BY id'

            run_ch_post $'CREATE OR REPLACE VIEW analytics.v_customer_daily_report AS
            SELECT
              d.customer_id AS customer_id,
              d.device_id   AS device_id,
              d.email       AS email,
              d.first_name  AS first_name,
              d.last_name   AS last_name,
              f.usage_date  AS usage_date,
              f.steps_sum   AS steps_sum,
              f.avg_battery AS avg_battery,
              f.load_avg    AS load_avg
            FROM analytics.fact_daily_telemetry f
            ANY LEFT JOIN analytics.dim_customers d USING customer_id;'
        """,
        env={},
    )

    ch_load_dim = BashOperator(
        task_id='ch_load_dim',
        bash_command=COMMON_SCRIPT + r"""
            PG_CRM_HOST="${PG_CRM_HOST:-crm_db}"
            PG_CRM_PORT="${PG_CRM_PORT:-5432}"
            PG_CRM_DB="${PG_CRM_DB:-crm_db}"
            PG_CRM_SCHEMA="${PG_CRM_SCHEMA:-public}"
            PG_CRM_TABLE="${PG_CRM_TABLE:-customers}"
            PG_CRM_USER="${PG_CRM_USER:-crm_user}"
            PG_CRM_PASS="${PG_CRM_PASS:-crm_password}"

            run_ch_get "SELECT version()"

            run_ch_get "SELECT 1
            FROM postgresql('${PG_CRM_HOST}:${PG_CRM_PORT}','${PG_CRM_DB}','tables','${PG_CRM_USER}','${PG_CRM_PASS}','information_schema')
            LIMIT 1"

            run_ch_get "SELECT
                if(count()=0, throwIf(1, 'PG table not found: ${PG_CRM_DB}.${PG_CRM_SCHEMA}.${PG_CRM_TABLE}'), 1)
            FROM postgresql('${PG_CRM_HOST}:${PG_CRM_PORT}','${PG_CRM_DB}','tables','${PG_CRM_USER}','${PG_CRM_PASS}','information_schema')
            WHERE table_schema = '${PG_CRM_SCHEMA}' AND table_name = '${PG_CRM_TABLE}'"

            echo "→ Listing known tables in ${PG_CRM_DB}.${PG_CRM_SCHEMA} (first 50):" 1>&2
            run_ch_get "SELECT table_schema, table_name
            FROM postgresql('${PG_CRM_HOST}:${PG_CRM_PORT}','${PG_CRM_DB}','tables','${PG_CRM_USER}','${PG_CRM_PASS}','information_schema')
            WHERE table_schema = '${PG_CRM_SCHEMA}'
            ORDER BY table_name
            LIMIT 50"

            run_ch_post "TRUNCATE TABLE IF EXISTS analytics.dim_customers"

            run_ch_post "INSERT INTO analytics.dim_customers (customer_id, device_id, email, first_name, last_name)
            SELECT toInt32(customer_id), device_id, email, first_name, last_name
            FROM postgresql('${PG_CRM_HOST}:${PG_CRM_PORT}','${PG_CRM_DB}','${PG_CRM_TABLE}','${PG_CRM_USER}','${PG_CRM_PASS}','${PG_CRM_SCHEMA}')"
        """,
        env={},
    )

    ch_load_fact = BashOperator(
        task_id='ch_load_fact',
        bash_command=COMMON_SCRIPT + r"""
            PG_TEL_HOST="${PG_TEL_HOST:-telemetry_db}"
            PG_TEL_PORT="${PG_TEL_PORT:-5432}"
            PG_TEL_DB="${PG_TEL_DB:-telemetry_db}"
            PG_TEL_SCHEMA="${PG_TEL_SCHEMA:-public}"
            PG_TEL_TABLE="${PG_TEL_TABLE:-sensor_events}"
            PG_TEL_USER="${PG_TEL_USER:-telemetry_user}"
            PG_TEL_PASS="${PG_TEL_PASS:-telemetry_password}"

            run_ch_get "SELECT version()"

            run_ch_get "SELECT 1
            FROM postgresql('${PG_TEL_HOST}:${PG_TEL_PORT}','${PG_TEL_DB}','tables','${PG_TEL_USER}','${PG_TEL_PASS}','information_schema')
            LIMIT 1"

            run_ch_get "SELECT count()
            FROM postgresql('${PG_TEL_HOST}:${PG_TEL_PORT}','${PG_TEL_DB}','tables','${PG_TEL_USER}','${PG_TEL_PASS}','information_schema')
            WHERE table_schema='${PG_TEL_SCHEMA}' AND table_name='${PG_TEL_TABLE}'"

            run_ch_post "$(cat <<SQL
            INSERT INTO analytics.fact_daily_telemetry (customer_id, usage_date, steps_sum, avg_battery, load_avg)
            WITH
              src AS (
                SELECT
                  device_id,
                  toDate(event_ts) AS usage_date,
                  sum(steps)         AS steps_sum,
                  avg(battery_level) AS avg_battery,
                  avg(load_kg)       AS load_avg
                FROM postgresql('${PG_TEL_HOST}:${PG_TEL_PORT}','${PG_TEL_DB}','${PG_TEL_TABLE}','${PG_TEL_USER}','${PG_TEL_PASS}','${PG_TEL_SCHEMA}')
                GROUP BY device_id, usage_date
              ),
              dim AS (
                SELECT device_id, anyHeavy(toInt32(customer_id)) AS customer_id
                FROM analytics.dim_customers
                GROUP BY device_id
              )
            SELECT
              d.customer_id,
              s.usage_date,
              s.steps_sum,
              toDecimal32(s.avg_battery, 2),
              toDecimal32(s.load_avg, 2)
            FROM src s
            ANY LEFT JOIN dim d USING device_id
            SQL
            )"
        """,
        env={},
    )

    ch_update_watermark = BashOperator(
        task_id="ch_update_watermark",
        bash_command=COMMON_SCRIPT + r"""
            run_ch_post "INSERT INTO analytics.processed_watermark
            SELECT 1, toDate('1970-01-01')
            WHERE NOT EXISTS(SELECT 1 FROM analytics.processed_watermark WHERE id = 1)"

            run_ch_post "ALTER TABLE analytics.processed_watermark
            UPDATE last_processed_date = coalesce((SELECT max(usage_date) FROM analytics.fact_daily_telemetry),
                                                  toDate('1970-01-01'))
            WHERE id = 1"
        """,
        env={},
    )

    ch_init >> ch_load_dim >> ch_load_fact >> ch_update_watermark
