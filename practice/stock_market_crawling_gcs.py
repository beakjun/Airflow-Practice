import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
import pandas as pd
import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# [END import_module]




#param={'Servicekey': key ,'resultType':'json','numOfRows':3000,'basDt':'20230821'}
url='https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo?'
# response=requests.get(url,param)

# [START instantiate_dag]
@dag(
    dag_id = 'stock_market_crawling',
    schedule_interval='0 12 * * 1-5',
    start_date=pendulum.datetime(2020,1, 1 , 12 ,00 , tz='Asia/Seoul'),
    #end_date = pendulum.now(),
    catchup=True,  # backfill과 비슷한 기능 
    max_active_runs =10,
    concurrency = 10,
    tags=['crawling'],
)
def stock_market_crawling():

    # [END instantiate_dag]

    # [START extract]

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def date_execution(**kwargs):
        execution_date=kwargs['logical_date']
        return execution_date
    

    @task(retries=2,retry_delay=timedelta(minutes=1))
    def html_request(url,bsdt):
        key='VtJk4y5W9b0T3ZOFuwD8v+nyzxuOEfRsSeIU8pnok9bPZpxQ40a9qzwoMB38tnJgog/lvxMAxNBJNMpt4f482A=='
        bsdt=bsdt.strftime('%Y%m%d')
        param = {'Servicekey': key ,'resultType':'json','numOfRows':3000,'basDt':bsdt}
        response=requests.get(url,param)
        if response.status_code != 200:
            raise ValueError(f"Request failed with status code {response.status_code}")
        data=response.json()
        df = pd.json_normalize(data,record_path=['response','body','items','item'])
        if len(df)==0:
            raise ValueError("Empty Data")
        return df
    
    @task(retries=2,retry_delay=timedelta(minutes=1))
    def upload_dataframe(df,bsdt):
        bsdt=bsdt.strftime('%Y%m%d')
        hook = GCSHook(
            gcp_conn_id = 'bj-gcs'
        )
        hook.upload(bucket_name = 'asia-northeast2-airflow-4ac21cad-bucket',
                    object_name = f"StockPriceInfo/{bsdt}/data.csv",
                    data= df.to_csv(index=False),
                    )
        

    bsdt=date_execution()
    df=html_request(url,bsdt)
    upload_dataframe(df,bsdt)

stock_market_crawling()