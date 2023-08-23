from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
import airflow

dag=DAG(
    dag_id='create_table',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None
)
create_employees_table = PostgresOperator(
    task_id="create_employees_table",
    postgres_conn_id="sr-postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS employees (
            "Serial Number" NUMERIC PRIMARY KEY,
            "Company Name" TEXT,
            "Employee Markme" TEXT,
            "Description" TEXT,
            "Leave" INTEGER
        );""",
    dag=dag
)

create_employees_temp_table = PostgresOperator(
    task_id="create_employees_temp_table",
    postgres_conn_id="sr-postgres",
    sql="""
        DROP TABLE IF EXISTS employees_temp;
        CREATE TABLE employees_temp (
            "Serial Number" NUMERIC PRIMARY KEY,
            "Company Name" TEXT,
            "Employee Markme" TEXT,
            "Description" TEXT,
            "Leave" INTEGER
        );""",
    dag=dag
)