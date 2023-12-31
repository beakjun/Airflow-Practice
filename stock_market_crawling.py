import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from sqlalchemy import create_engine
# [END import_module]




#param={'Servicekey': key ,'resultType':'json','numOfRows':3000,'basDt':'20230821'}
url='https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo?'
# response=requests.get(url,param)



# df_contacts = pd.json_normalize(datajs, record_path=['response','body','items','item'])

def get_postgres(autocommit=True):
    hook = PostgresHook(postgres_conn_id = 'bj-postgres')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

        self.df['basDt'] = self.df['basDt'].apply(lambda x:x.date())
        self.df['fltRt']=self.df['fltRt'].astype(float)
        self.df = self.df.astype({'clpr':int,'vs':int,'mkp':int,'hipr':int,'lopr':int,'trqu':int,'trPrc':int,'clpr':int,'lstgStCnt':int,'mrktTotAmt':int,})

dtype = { 'basDt' : 'date',
         'srtnCd' : 'char(6)',
         'isinCd' : 'char(12)',
         'itmsNm' : 'varchar(20)',
         'mrkCtg' : 'varchar(10)',
         'clpr' : 'int',
         'vs' : 'int',
         'fltRt' : 'numeric(4,2)',
         'mkp' : 'int',
         'hipr' : 'int',
         'lopr' : 'int',
         'trqu' : 'int',
         'trPrc' : 'bigint',
         'lstgStCnt' : 'bigint',
         'mrktTotAmt' : 'bigint'
}  


# [START instantiate_dag]
@dag(
    dag_id = 'stock_market_crawling',
    schedule_interval='0 12 * * 1-5',
    start_date=pendulum.datetime(2022,1, 1 , 12 ,00 , tz='Asia/Seoul'),
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
    
    @task(retries=2, retry_delay=timedelta(minutes=1))
    def load(table_nm,df):
        postgres_hook = PostgresHook('bj-postgres')
        engine=create_engine(postgres_hook.get_uri(), echo=False)
        df.to_sql(table_nm,engine,schema='airflow',if_exists='append',index=False)

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract_kospi(table_nm,df):
        postgres_hook = PostgresHook('bj-postgres')
        engine=create_engine(postgres_hook.get_uri(), echo=False)
        kospi_df = df[df['mrktCtg']=='KOSPI']
        kospi_df.to_sql(table_nm,engine,schema='airflow',if_exists='append',index=False,type=dtype)

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract_kosdaq(table_nm,df):
        postgres_hook = PostgresHook('bj-postgres')
        engine=create_engine(postgres_hook.get_uri(), echo=False)
        kospi_df = df[df['mrktCtg']=='KOSDAQ']
        kospi_df.to_sql(table_nm,engine,schema='airflow',if_exists='append',index=False,type=dtype)
# [START dag_invocation]
    bsdt=date_execution()
    df=html_request(url,bsdt)
    load('stock_market_tbl',df)
    extract_kospi('kospi_market_tbl',df)
    extract_kosdaq('kosdaq_market_tbl',df)
stock_market_crawling()
# [END dag_invocation]

# [END tutorial]
