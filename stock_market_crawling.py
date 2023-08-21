from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
def process_task_fn(item, **kwargs):
    print(f"Processing: {item}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
}

dag = DAG(
    'dynamic_tasks_to_end_task',
    default_args=default_args,
    description='DAG with dynamically generated tasks all pointing to an end task',
    schedule_interval='@daily',
)
start_task = DummyOperator(task_id='start_task', dag=dag)
items = ["item1", "item2", "item3"]

# 동적으로 태스크를 생성합니다
tasks = []
for item in items:
    task = PythonOperator(
        task_id=f'process_task_{item}',
        python_callable=process_task_fn,
        op_args=[item],
        dag=dag,
    )
    tasks.append(task)

# end 태스크를 생성합니다
end_task = DummyOperator(task_id='end_task', dag=dag)

# 생성된 모든 태스크를 end 태스크에 연결합니다
for task in tasks:
    start_task >> task >> end_task