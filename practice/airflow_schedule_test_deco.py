import pendulum
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine


# [START instantiate_dag]
@dag(
    dag_id='schedule_test_deco',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@daily'
)
def schedule_test_deco():

 # [END instantiate_dag]
    @task
    def print_context(**kwargs):
        print(f'''\n        execution_date: {kwargs['execution_date']}
        next_execution_date: {kwargs['next_execution_date']}\n
        ds(start_date) : {kwargs['ds']}\n
        next_ds(end_date) :{kwargs['next_ds']}\n
        logical_date :{kwargs['logical_date']}\n
        data_interval_start : {kwargs['data_interval_start']}\n
        data_interval_end : {kwargs['data_interval_end']}
''')

    
    print_context()
schedule_test_deco()
