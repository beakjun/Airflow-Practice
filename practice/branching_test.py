"""Example DAG demonstrating the usage of the `@task.branch`
TaskFlow API decorator."""

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label ## 브랜치에 네이밍을 정의해주는 역할을 하는듯

import random
from pendulum import datetime


@dag(
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule="@daily"
)
def branch_python_operator_decorator_example():

    run_this_first = EmptyOperator(task_id="run_this_first") # 첫번째 엠티 오퍼레이터

    options = ["branch_a", "branch_b", "branch_c", "branch_d"] ## 옵션 정의

    @task.branch(task_id="branching")  ##브런치 태스크 정의
    def random_choice(choices): # 랜던하게 선택하는 함수
        return random.choice(choices)

    random_choice_instance = random_choice(choices=options) ## 옵션중 하나 서택

    run_this_first >> random_choice_instance ### 첫번째 엠티 오퍼레이터이후 branching이라는 오퍼레이터 실행 이러면 이후 분기가 생김

    join = EmptyOperator(    ## 조인 오퍼레이터 브랜칭을 할때 왠만하면 Trigger rule을 사용하는 것이 좋음
        task_id="join",
        trigger_rule="none_failed_min_one_success"
    )

    for option in options:  #옵션들을 루프문으로 정의해준다.

        t = EmptyOperator(
            task_id=option
        )

        empty_follow = EmptyOperator(
            task_id="follow_" + option
        )

        # Label is optional here, but it can help identify more complex branches
        random_choice_instance >> Label(option) >>t >> empty_follow >> join


branch_python_operator_decorator_example()