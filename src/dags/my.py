import requests
import json
import datetime
import time
import psycopg2

import numpy as np
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom

url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
headers={
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f", # ключ API
    "X-Nickname": "andreas-lucky", # авторизационные данные
    "X-Cohort": "8" # авторизационные данные   
    }
def load_stg_table_couriers (ti, url, headers):
    method_url = '/couriers'
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()
    offset = 0
    limit = 50
    sort_field='id'
    sort_direction = 'asc'
    while True:
      params = {'limit': limit, 'offset': offset,'sort_field':sort_field,'sort_direction':sort_direction}
      couriers_rep = requests.get(url + method_url, headers=headers, params=params).json()
      if len(couriers_rep) == 0:
        conn.commit()
        cur.close()
        conn.close()
        break
      values = [[value for value in couriers_rep[i].values()] for i in range(len(couriers_rep))]
      sql = f"INSERT INTO stg.couriers (courier_id, name) VALUES %s"
      execute_values(cur, sql, values)
      offset += len(couriers_rep)

def load_stg_table_deliveries (ti, url, headers):
    method_url = '/deliveries'
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()
    offset = 0
    limit = 50
    sort_field='id'
    sort_direction = 'asc'
    while True:
      params = {'limit': limit, 'offset': offset,'sort_field':sort_field,'sort_direction':sort_direction}
      deliveries_rep = requests.get(url + method_url, headers=headers, params=params).json()
      if len(deliveries_rep) == 0:
        conn.commit()
        cur.close()
        conn.close()
        break
      values = [[value for value in deliveries_rep[i].values()] for i in range(len(deliveries_rep))]
      sql = f"INSERT INTO stg.deliveries (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum) VALUES %s"
      execute_values(cur, sql, values)
      offset += len(deliveries_rep)

def update_mart_d_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()
 
    cur.execute("""
 
                    DELETE FROM dds.dm_couriers;
                    DELETE FROM dds.dm_deliveries;
                    DELETE FROM dds.dm_orders;
                    DELETE FROM dds.fct_deliveries;
                    INSERT INTO dds.dm_couriers (courier_id_source, name) select distinct courier_id, name from stg.couriers;
                    INSERT INTO dds.dm_deliveries (delivery_id_source) select distinct delivery_id from stg.deliveries;
                    INSERT INTO dds.dm_orders (order_id_source) select distinct order_id from stg.deliveries;
                    INSERT INTO dds.fct_deliveries
                    (order_id_dwh, delivery_id_dwh, courier_id_dwh, order_ts, delivery_ts, address, rate, tip_sum, total_sum)
                    select distinct
                    o.order_id_dwh,
                    dd.delivery_id_dwh, 
                    c.courier_id_dwh,
                    d.order_ts,
                    d.delivery_ts,
                    d.address,
                    d.rate,
                    d.tip_sum,
                    d.sum as total_sum
                    from 
                    stg.deliveries d
                    inner join dds.dm_couriers c on c.courier_id_source = d.courier_id
                    inner join dds.dm_deliveries dd on dd.delivery_id_source = d.delivery_id
                    inner join dds.dm_orders o on o.order_id_source = d.order_id;
                """)
    conn.commit()
 
    cur.close()
    conn.close()
 
    return 200

############# D A G ####################

dag = DAG(
    dag_id='load_data',
    schedule_interval='0/15 * * * *',
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

t_stg_table1 = PythonOperator(task_id='load_stg_table_couriers',
                                        python_callable=load_stg_table_couriers,
                                        op_kwargs={'url':url,
                                                    'headers': headers
                                                    },
                                        dag=dag)
t_stg_table2 = PythonOperator(task_id='load_stg_table_deliveries',
                                        python_callable=load_stg_table_deliveries,
                                        op_kwargs={'url':url,
                                                    'headers': headers
                                                    },
                                        dag=dag)
t_update_mart_d_tables = PythonOperator(task_id='update_mart_d_tables',
                                        python_callable=update_mart_d_tables,
                                        dag=dag)
t_stg_table1 >> t_stg_table2 >> t_update_mart_d_tables