import pendulum
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine


# [START instantiate_dag]
dag=DAG(
    dag_id='schedule_test',
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None
)

date_comparison = BashOperator(
    task_id = "date_comparison",
    bash_command=(
    "echo execution_date : {{execution_date}}"
    "echo next_execution_date : {{next_execution_date}}"
    "echo ds(start_date) : {{ds}}"
    "echo next_ds(end_date) : {{next_ds}}"
    "echo logical_date :{{logical_date}}"
    "echo data_interval_end :{{data_interval_end}}" 
    "echo data_interval_start : {{data_interval_start}}"
    ),
    dag=dag
)
date_comparison