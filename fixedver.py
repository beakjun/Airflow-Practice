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
    dag_id = 'collecting Stock price info',
    schedule_interval='0 9 * * 1-5',
    start_date=pendulum.datetime(2023,1, 1 , 9 ,00 , tz='Asia/Seoul'),
    #end_date = pendulum.now(),
    catchup=True,  # backfill과 비슷한 기능 
    max_active_runs =10,
    concurrency = 10,
    tags=['stock price info'],
)
def collecting_info():

    # [END instantiate_dag]

    # [START extract]

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def date_execution(**kwargs):
        execution_date=kwargs['logical_date']
        return execution_date
    

    @task(retries=3,retry_delay=timedelta(minutes=60))
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
    def extract_kospi(table_nm,df):
        print(df['basDt'])
        df['basDt'] = pd.to_datetime(df['basDt'], format='%Y%m%d')
        df['basDt'] = df['basDt'].apply(lambda x:x.date())
        df['fltRt']=df['fltRt'].astype(float)
        df = df.astype({'clpr':int,'vs':int,'mkp':int,'hipr':int,'lopr':int,'trqu':int,'trPrc':int,'clpr':int,'lstgStCnt':int,'mrktTotAmt':int,})
        df = df.sort_values(by=['basDt'])
        postgres_hook = PostgresHook('bj-postgres')  # 포스트그레스 컨넥션 재정의 필요
        engine=create_engine(postgres_hook.get_uri(), echo=False)
        kospi_df = df[df['mrktCtg']=='KOSPI']
        kospi_df.to_sql(table_nm,engine,schema='stockprice_info',if_exists='append',index=False)
    
    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract_kosdaq(table_nm,df):
        df['basDt'] = pd.to_datetime(df['basDt'], format='%Y%m%d')
        df['basDt'] = df['basDt'].apply(lambda x:x.date())
        df['fltRt']=df['fltRt'].astype(float)
        df = df.astype({'clpr':int,'vs':int,'mkp':int,'hipr':int,'lopr':int,'trqu':int,'trPrc':int,'clpr':int,'lstgStCnt':int,'mrktTotAmt':int,})
        df = df.sort_values(by=['basDt'])
        postgres_hook = PostgresHook('bj-postgres') # 포스트그레스 컨넥션 재정의 필요
        engine=create_engine(postgres_hook.get_uri(), echo=False)
        kospi_df = df[df['mrktCtg']=='KOSDAQ']
        kospi_df.to_sql(table_nm,engine,schema='stockprice_info',if_exists='append',index=False)




    def table_exists(engine, table_name, schema):
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{schema}')"
        result = engine.execute(query).scalar()
        return result

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def ranking_table_gen(df):
        df['basDt'] = pd.to_datetime(df['basDt'], format='%Y%m%d')
        df['basDt'] = df['basDt'].apply(lambda x:x.date())
        df['fltRt']=df['fltRt'].astype(float)
        df = df.astype({'clpr':int,'vs':int,'mkp':int,'hipr':int,'lopr':int,'trqu':int,'trPrc':int,'clpr':int,'lstgStCnt':int,'mrktTotAmt':int,})
        stat_df=df[['itmsNm','srtnCd','mrktCtg','basDt','clpr','vs','fltRt','trqu','mrktTotAmt']]
        postgres_hook = PostgresHook('bj-postgres') # 재정의 필요
        engine=create_engine(postgres_hook.get_uri(), echo=False)
        if table_exists(engine, 'stockprice_stat', 'stockprice_info'):
            # 테이블이 이미 존재하면 TRUNCATE
            with engine.begin() as connection:
                connection.execute('TRUNCATE TABLE stockprice_info.stockprice_stat')
            stat_df.to_sql('stockprice_stat', engine, schema='stockprice_info', if_exists='append', index=False)
        else:
            stat_df.to_sql('stockprice_stat', engine, schema='stockprice_info', index=False)
            # 테이블이 없으면 새로 생성
            
    bsdt=date_execution()
    df=html_request(url,bsdt)
    extract_kospi("kospi_stockprice_info",df)>>extract_kosdaq("kosdaq_stockprice_info",df)>>ranking_table_gen(df)
stock_market_crawling()