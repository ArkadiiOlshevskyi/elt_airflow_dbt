import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup

from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# TODO - try sensor
# from airflow.utils.sensor_helper import Sensor



# TOD0 - import task modules here
from task_modules.any_task import *

from task_modules.slack_notifier import *




DOC = """
This is main DAG that:
- unzips data by spark
- slises data into small chunks - 10 mil rows
- process them (mapping the folder with files) - how better? queue?
- store cleaned data into jdbc postgres
- store cleaned parquet data + version contro detlalake
- uses dbt project to update views and windows in postgres
- vizualize updated data by superset (by trigger it?)
"""


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(hour=1),
    "start_date": datetime(2025, 2, 25),
    "on_success_callback": dag_success_slack_alert,
    "on_failure_callback": dag_failure_slack_alert,
}


@dag(
    start_date=datetime(2025, 2, 25),
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    tags=["ELT", "dbt", "PySpark", "Main Dag"],
    doc_md=DOC,
    default_view="graph",
)


def main_etl_dag():

    start_dag = EmptyOperator(
        task_id="start_dag",
        on_success_callback=dag_slack_call_start
    )

    with TaskGroup("elt", tooltip="elt") as elt:

        with TaskGroup("check_input")


main_etl_dag = main_etl_dag()