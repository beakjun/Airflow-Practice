import pendulum
from airflow.decorators import dag, task
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging
# [END import_module]



def get_postgres(autocommit=True):
    hook = PostgresHook(postgres_conn_id = 'sr-postgres')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

url = 'https://finance.naver.com/sise/sise_index_day.naver?code=KOSPI&page='



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
    

    @task
    def html_request(url,date):
        date=date.strftime('%Y.%m.%d')
        page=0
        while True:
            if page>2000:
                break
            page+=1
            url1=url+str(page)
            df=pd.read_html(url1)[0]
            print(url,df)
            print(date, df['날짜'])
            if date in list(df['날짜']):
                return df[df['날짜']==date]
    @task
    def extract_date(df,date):
        new_df = df.copy()
        new_df.drop(columns = '전일비', inplace =True)
        new_df['등락률'] = new_df['등락률'].apply(lambda x:x[:-1]).astype(float)
        new_df.rename(columns={'등락률': '등락률(%)'},inplace=True)
        return new_df 
    
    @task
    def load(df, tablename, date):
        logging.info('load started')
        cur = get_postgres()
        value1=tuple(df.iloc[0].values)
        sql = f"INSERT INTO {tablename} VALUES (%s,%s,%s,%s,%s)" 
        cur.execute(sql,value1)
        logging.info(f'{df.iloc[0,0]} 적재 완료')

        

    edate=date_execution()
    df = html_request(url,edate)
    new_df = extract_date(df,edate)
    load(new_df,'test.stock_chart_test',edate)
# [START dag_invocation]
stock_market_crawling()
# [END dag_invocation]

# [END tutorial]
