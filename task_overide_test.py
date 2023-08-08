from airflow.decorators import task, dag
from datetime import datetime


@task # 여기서 정의한 task를 아래의 dag에서 여러번 사용할 수 있게끔 task의 유연한 활용 가능
def add_task(x, y):
    print(f"Task args: x={x}, y={y}")
    return x + y # x+y 값을 구하고 


@dag(start_date=datetime(2022, 1, 1))
def mydag():
    start = add_task.override(task_id="start")(1, 2) #초기 x=1 y=2
    for i in range(3):
        start >> add_task.override(task_id=f"add_start_{i}")(start, i) #루프를 돌면서 start= 위의함수의 return 값 i는 루프에서 가져와서 계산 총 루프수만큼 task_1,2,3이생김


@dag(start_date=datetime(2022, 1, 1))
def mydag2():
    start = add_task(1, 2)
    for i in range(3):
        start >> add_task.override(task_id=f"new_add_task_{i}")(start, i)


first_dag = mydag()
second_dag = mydag2()