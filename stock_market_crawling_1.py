import pendulum
from airflow.decorators import dag, task
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging
import html5lib
# [END import_module]



def get_postgres(autocommit=True):
    hook = PostgresHook(postgres_conn_id = 'sr-postgres')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

jmk_list = ['005930','011200']



@task
def url_gen(url,code):
    new_url = f'{str(url)}code={code}'
    print(new_url)
    return new_url

@task
def html_request(url,edate):
    date=edate.strftime('%Y.%m.%d')
    page=0
    while True:
        if page>2000:
            break
        page+=1
        new_url=f'{url}&page={str(page)}'
        df=pd.read_html(new_url)[0]
        if date in list(df['날짜']):
            return df[df['날짜']==date]
    

# [START instantiate_dag]
@dag(
    dag_id = 'stock_market_crawling_1',
    schedule_interval='30 16 * * 1-5',
    start_date=pendulum.datetime(2023, 7, 25, 16 ,30 , tz='Asia/Seoul'),
    #end_date = pendulum.now(),
    catchup=True,  # backfill과 비슷한 기능 
    tags=['crawling'],
)
def stock_market_crawling():

    # [END instantiate_dag]

    # [START extract]

    @task
    def date_execution(**kwargs):
        execution_date=kwargs['data_interval_end']
        #execution_date=local_tz.convert(execution_date)
        logging.info(f'collection date : {execution_date}')
        return execution_date
    
    url = 'https://finance.naver.com/item/sise_day.naver?'
    edate=date_execution()
    for item in jmk_list:
        html_request.override(task_id=f'html_request_{item}')(url_gen.override(task_id=f'url_gen_{item}')(url,item),edate)

# [START dag_invocation]
stock_market_crawling()
# [END dag_invocation]

# [END tutorial]
