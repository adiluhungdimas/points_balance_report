import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'dimas',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'dbt_on_airflow',
    default_args=default_args,
    description='A dbt wrapper for airflow',
    schedule_interval='@daily',
    start_date=days_ago(3),    
)

def load_manifest():
    local_filepath = "/Users/dimas.adiluhung/points_balance_report/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)

    return data

def make_dbt_task(node, dbt_verb):
    """Returns an Airflow operator either run and test an individual model"""
    model = node.split(".")[-1]

    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=node,
            bash_command=f"""
            cd /Users/dimas.adiluhung/points_balance_report &&
            source /Users/dimas.adiluhung/dbt-postgre-env/bin/activate &&
            dbt {dbt_verb} --model {model} --profiles-dir /Users/dimas.adiluhung/points_balance_report/profiles
            """,
            dag=dag,
        )

    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = BashOperator(
            task_id=node_test,
            bash_command=f"""
            cd /Users/dimas.adiluhung/points_balance_report &&
            source /Users/dimas.adiluhung/dbt-postgre-env/bin/activate &&
            dbt {dbt_verb} --model {model} --profiles-dir /Users/dimas.adiluhung/points_balance_report/profiles
            """,
            dag=dag,
        )

    return dbt_task

data = load_manifest()

dbt_tasks = {}
for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")

        dbt_tasks[node] = make_dbt_task(node, "run")
        dbt_tasks[node_test] = make_dbt_task(node, "test")

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":

        # Set dependency to run tests on a model after model runs finishes
        node_test = node.replace("model", "test")
        dbt_tasks[node] >> dbt_tasks[node_test]

        # Set all model -> model dependencies
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:

            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type == "model":
                dbt_tasks[upstream_node] >> dbt_tasks[node]