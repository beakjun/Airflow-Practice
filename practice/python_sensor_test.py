from pathlib import Path
import airflow
from airflow.sensors.python import PythonSensor
from airflow import DAG



def _wait_for_supermarket(supermarket_id):
    supermarket_path = Path('/opt/airflow/data1/' + supermarket_id)# Path 객체 초기화
    data_files=supermarket_path.glob("data-*.csv") # data-*.csv 파일 수집
    print(supermarket_path)
    #success_file=supermarket_path / "_SUCCESS" # _SUCCESS 파일 수집
    return data_files #and success_file.exists() # 데이터 파일과 성공 파일이 있는지 확인 후 반환
dag = DAG(
    dag_id='python_sensor',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None
)
wait_for_supermarket_1 = PythonSensor(
    task_id = "wait_for_supermarket_1",
    python_callable = _wait_for_supermarket,
    op_kwargs= {"supermarket_id": "supermarket1"},
    dag=dag,
)