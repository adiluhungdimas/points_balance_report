import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'dimas',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'load_csv_to_postgres',
    default_args=default_args,
    description='Load CSV',
    schedule_interval='@daily',
    start_date=days_ago(3),
)

csv_path = "/Users/dimas.adiluhung/points_balance_report/seeds/"

load_infos = [
    {"csv_file": csv_path + "crm__customers_tenant_a.csv", "table_name": "raw_data.crm__customers_tenant_a"},
    {"csv_file": csv_path + "crm__customers_tenant_b.csv", "table_name": "raw_data.crm__customers_tenant_b"},
    {"csv_file": csv_path + "crm__customers_tenant_c.csv", "table_name": "raw_data.crm__customers_tenant_c"},
    {"csv_file": csv_path + "ledger__points_tranche_snapshots_tenant_a.csv", "table_name": "raw_data.ledger__points_tranche_snapshots_tenant_a"},
    {"csv_file": csv_path + "ledger__points_tranche_snapshots_tenant_b.csv", "table_name": "raw_data.ledger__points_tranche_snapshots_tenant_b"},
    {"csv_file": csv_path + "ledger__points_tranche_snapshots_tenant_c.csv", "table_name": "raw_data.ledger__points_tranche_snapshots_tenant_c"},
]

for load_info in load_infos:
    csv_file = load_info["csv_file"]
    table_name = load_info["table_name"]
    task_id = f'load_{table_name}_to_postgres'
    load_csv_task = PostgresOperator(
        task_id=task_id,
        sql = f"COPY {table_name} FROM '{csv_file}' WITH (FORMAT CSV, HEADER);",
        dag=dag,
    )