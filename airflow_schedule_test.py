import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator


# [START instantiate_dag]
dag=DAG(
    dag_id='schedule_test',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@daily'
)
#### 각 날짜관련 테스크 콘테스트 비교 출력
date_comparison = BashOperator(
    task_id = "date_comparison",
    bash_command=(
    'echo -e "execution_date: {{execution_date}}\n'
    'next_execution_date: {{next_execution_date}}\n'
    'ds(start_date): {{ds}}\n'
    'next_ds(end_date): {{next_ds}}\n'
    'logical_date:{{logical_date}}\n'
    'data_interval_end:{{data_interval_end}}\n'
    'data_interval_start: {{data_interval_start}}"'
    ),
    dag=dag
)
