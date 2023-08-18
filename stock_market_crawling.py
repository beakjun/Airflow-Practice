import pendulum
from airflow.decorators import dag, task
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging
import time
# [END import_module]
regions = ['US', 'EU', 'APAC']

@dag(schedule_interval='@daily', start_date=datetime(2022, 1, 1))
def dynamic_tasks_within_single_dag():

    @task
    def process_region(region):
        print(f"Processing data for region: {region}")

    for region in regions:
        process_region(region)

dag_instance = dynamic_tasks_within_single_dag()