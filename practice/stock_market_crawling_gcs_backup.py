import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
import pandas as pd
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine


# [END import_module]




#param={'Servicekey': key ,'resultType':'json','numOfRows':3000,'basDt':'20230821'}
url='https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo?'
# response=requests.get(url,param)

# [START instantiate_dag]
@dag(
    dag_id = 'stock_market_crawling_test',
    #schedule_interval='0 9 * * 1-5',
    start_date=pendulum.datetime(2020,1, 1 , 9 ,00 , tz='Asia/Seoul'),
    catchup=True,  # backfill과 비슷한 기능 
    tags=['backup'],
)
def stock_market_crawling_test():

    # [END instantiate_dag]

    # [START extract]

    @task()
    def html_request(url):
        key='VtJk4y5W9b0T3ZOFuwD8v+nyzxuOEfRsSeIU8pnok9bPZpxQ40a9qzwoMB38tnJgog/lvxMAxNBJNMpt4f482A=='
        stdate=pendulum.datetime(2021,3,23)
        eddate=pendulum.datetime(2023,1,1)

        while stdate<eddate:
            stdate=stdate+timedelta(1)
            bsdt=stdate.strftime('%Y%m%d')
            param = {'Servicekey': key ,'resultType':'json','numOfRows':3000,'basDt':bsdt}
            response=requests.get(url,param)
            if response.status_code != 200:
                raise ValueError(f"Request failed with status code {response.status_code}")
            data=response.json()
            df = pd.json_normalize(data,record_path=['response','body','items','item'])
            if len(df)==0:
                continue
            df['basDt'] = pd.to_datetime(df['basDt'], format='%Y%m%d')
            df['basDt'] = df['basDt'].apply(lambda x:x.date())
            df['fltRt']=df['fltRt'].astype(float)
            df = df.astype({'clpr':int,'vs':int,'mkp':int,'hipr':int,'lopr':int,'trqu':int,'trPrc':int,'clpr':int,'lstgStCnt':int,'mrktTotAmt':int,})
            df = df.sort_values(by=['basDt'])
            postgres_hook = PostgresHook('bj-postgres')
            engine=create_engine(postgres_hook.get_uri(), echo=False)
            with engine.connect() as connection:
                kospi_df = df[df['mrktCtg']=='KOSPI']
                kospi_df.to_sql('kospi_stockprice_info_1',connection,schema='stockprice_info',if_exists='append',index=False)
                kosdaq_df = df[df['mrktCtg']=='KOSDAQ']
                kosdaq_df.to_sql('kosdaq_stockprice_info_1',connection,schema='stockprice_info',if_exists='append',index=False)
                
    html_request(url)
stock_market_crawling_test()