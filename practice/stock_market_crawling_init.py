import pendulum
from random import randint
from airflow.decorators import dag, task
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup
import requests
import re
import time
from sqlalchemy import create_engine
local_tz = pendulum.timezone("Asia/Seoul") # UTC로 설정되어있기때문에 한국시간으로 

def postgres_engine(conn_id):

    
    engine = postgres_hook.get_sqlalchemy_engine()
    return engine

url = 'https://finance.naver.com/sise/sise_index_day.naver?code=KOSPI'
def html_request(url):
    df=pd.read_html(url)[0]
    return df

def get_last_page(url:str, page) -> int:
    params = {'page': page}
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
    res = requests.get(url, params=params, headers = headers)
    html = res.text
    soup = BeautifulSoup(html,"html.parser")
    last_page = soup.select_one('.on').text
    last_page=re.sub(r'[^0-9]', '', last_page)
    if int(last_page) == page:
        return get_last_page(url, page+50)
    else:
        return int(last_page)

# [START instantiate_dag]
@dag(
    dag_id = 'stock_market_crawling_init',
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 7, 25, 16 ,30 , tz=local_tz),
    #end_date = pendulum.now(),
    catchup=False,  # backfill과 비슷한 기능 
    tags=['crawling']
)
def stock_market_crawling_init():

    @task
    def first_load(url,page,edate):
        last_page = get_last_page(url,page)
        for i in range(1,100):
            tmpurl = url+f'&page={i}'
            print(tmpurl)
            raw=html_request(tmpurl)
            raw=raw.dropna()
            raw=raw.drop(columns='전일비')
            raw['등락률'] = raw['등락률'].apply(lambda x:x[:-1]).astype(float)
            raw.rename(columns={'등락률': '등락률(%)'},inplace=True)
            raw = raw[raw['날짜']<=edate]
            time.sleep(randint(1, 5))
            table_name='stock_chart_test'
            postgres_hook = PostgresHook('sr-postgres')
            conn=postgres_hook.get_conn()
            engine=create_engine(postgres_hook.get_uri(), echo=False)
            raw.to_sql(table_name,engine,schema='test',if_exists='append',index=False)
            
    first_load(url,10,'2023.07.25')
stock_market_crawling_init()