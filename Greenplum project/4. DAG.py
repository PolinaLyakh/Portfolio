from airflow import DAG
from datetime import datetime, timedelta, date 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import requests

# Интервал дат для функций
st_date = '2021-01-01'
en_date = '2021-03-01'

db_connection = "gp_std18_4"
ch_connection = 'ch_std18_4'
db_scheme = 'std18_4_fp'
CLUSTER_NAME = 'default_cluster'

# facts. Переменные с названиями таблиц
db_function_fact = 'f_delta_partition'
db_function_coupons = 'f_delta_partition_coupons'
db_fact_tables = ['bills_head', 'bills_item']
db_fact_traffic = 'traffic'
db_fact_coupons = 'coupons'

# dictionaries. Переменные с названиями таблиц
db_function_dict = 'f_full_load'
db_dict_tables = ['plants', 'promos', 'promo_types']

# sql facts. Переменные со скриптом для загрузки данных в таблицы фактов
db_fact_load_query = f"select {db_scheme}.{db_function_fact}(%(p_table)s, %(p_partition_key)s, %(p_start_date)s, %(p_end_date)s, %(p_pxf_table)s, %(p_user_id)s, %(p_pass)s);"   
db_load_coupons_query = f"select {db_scheme}.{db_function_coupons}(%(p_start_date)s, %(p_end_date)s, %(p_file_name)s);"

# sql dictionaries. Переменные со скриптом для загрузки данных в таблицы справочников
db_dict_load_query = f"select {db_scheme}.{db_function_dict}(%(p_table)s, %(p_file_name)s);" 

# mart. Переменные с названием таблицы и с скриптом загрузки данных в витрину
db_function_mart = 'f_create_and_load_mart'
query_for_mart = f"select {db_scheme}.{db_function_mart}(%(st_date)s, %(en_date)s);"

# sql ClickHouse
# Скрипт для очистки таблиц
ch_drop_partition_query = (
    "ALTER TABLE std18_4_fp.dm_plant_efficiency_local ON CLUSTER {cluster} "
    "DROP PARTITION ID '{pid}';"
)

# Данные загружаются в распределенную (Distributed) таблицу dm_plant_efficiency_distr. 
ch_load_query = f"""
                INSERT INTO std18_4_fp.dm_plant_efficiency_distr
                SELECT 
                    report_date,
                    plant,
                    plant_txt,
                    oborot,
                    coupon_discount,
                    oborot_net,
                    sold_qty,
                    bills_cnt,
                    traffic_cnt,
                    promo_qty
                FROM std18_4_fp.ch_dm_plant_efficiency_ext
                WHERE report_date >= '{st_date}' AND report_date < '{en_date}';
            """

def month_ids(start, end):
    '''
    Она принимает две даты-строки (например, '2026-01-15' и '2026-04-10'), 
    Возвращает список партиций (идетификаторы) из данного интервала
    Последняя дата не включена
    '''
    cur = datetime.strptime(start, '%Y-%m-%d').date().replace(day=1)
    stop = datetime.strptime(end, '%Y-%m-%d').date().replace(day=1)
    ids = []
    while cur < stop:
        ids.append(cur.strftime('%Y%m'))
        cur = date(cur.year + cur.month // 12, cur.month % 12 + 1, 1)
    return ids

def execute_ch_query(sql_query, connection_id):
    '''
    Подключение к CH для передачи туда скриптов для очистки таблиц и загрузки данных
    '''
    conn = BaseHook.get_connection(connection_id) # Подключение
    
    port = conn.port or 8123
    url = f"http://{conn.host}:{port}/"
    
    clean_query = sql_query.strip()
    
    print(f"Отправка запроса в ClickHouse. Длина запроса: {len(clean_query)} символов.")
    
    response = requests.post(
        url,
        data=clean_query.encode('utf-8'),
        auth=(conn.login, conn.password),
        timeout=1200  
    )
    
    if response.status_code == 200:
        print("Запрос успешно выполнен!")
    else:
        raise Exception(f"Ошибка ClickHouse: {response.status_code} - {response.text}")

# 
def reload_ch_period(connection_id, start, end):
    '''
    В цикле происходит удаление данных по месяцам.
    После того как все старые партиции удалены, выполняется финальный запрос загрузки данных. 
    '''
    for pid in month_ids(start, end):
        execute_ch_query(ch_drop_partition_query.format(cluster=CLUSTER_NAME, pid=pid), connection_id)
    execute_ch_query(ch_load_query, connection_id)

default_args = {
    'depends_on_past': False,
    'owner': 'std18_4',
    'start_date': datetime(2022, 5, 17),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email': False,
    'email_on_failure': False,
    'email_on_entery': False 
    }

with DAG(
    "std18_4_fp_DAG",
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    ) as dag:
        
        task_start = DummyOperator(task_id="start")    

        with TaskGroup("load_fact_tables") as load_fact_tables:
            for table in db_fact_tables:
                task_fact_load = PostgresOperator(task_id=f"load_fact_table-{table}",
                                                        postgres_conn_id=db_connection,
                                                        sql=db_fact_load_query,
                                                        parameters={'p_table':f'{db_scheme}.{table}',
                                                                    'p_partition_key':'calday',
                                                                    'p_start_date':st_date,
                                                                    'p_end_date':en_date,
                                                                    'p_pxf_table':f'gp.{table}',
                                                                    'p_user_id':'intern',
                                                                    'p_pass':'intern'
                                                                    }
                                                        )
        
        # Отдельно, потому что поле партицирование называется по другому
        load_fact_traffic = PostgresOperator(task_id=f"load_fact_table_{db_fact_traffic}",
                                                postgres_conn_id=db_connection,
                                                sql=db_fact_load_query,
                                                parameters={'p_table':f'{db_scheme}.{db_fact_traffic}',
                                                            'p_partition_key':'date',
                                                            'p_start_date':st_date,
                                                            'p_end_date':en_date,
                                                            'p_pxf_table':f'gp.{db_fact_traffic}', 
                                                            'p_user_id':'intern',
                                                            'p_pass':'intern'
                                                             }
                                                )
        
        # Отдельно, потому что используется протокл dpfdist ( а не PXF)
        load_fact_coupons = PostgresOperator(task_id=f"load_fact_table_{db_fact_coupons}",
                                                postgres_conn_id=db_connection,
                                                sql=db_load_coupons_query,
                                                parameters={'p_table':f'{db_scheme}.{db_fact_coupons}', 
                                                            'p_start_date':st_date,
                                                            'p_end_date':en_date,
                                                            'p_file_name':f'{db_fact_coupons}',
                                                             }
                                                )
        
        # Загрузка таблиц справочников
        with TaskGroup("load_dict_tables") as load_dict_tables:    
            for table in db_dict_tables:
                task = PostgresOperator(task_id=f"load_dict_table_{table}",
                                        postgres_conn_id=db_connection,
                                        sql=db_dict_load_query,
                                        parameters={'p_table':f'{db_scheme}.{table}',
                                                    'p_file_name':f'{table}'
                                                    }
                                        )

        # Загрузка витрины
        load_mart = PostgresOperator(task_id="load_mart",
                                                postgres_conn_id=db_connection,
                                                sql=query_for_mart,
                                                parameters={'st_date':st_date,
                                                            'en_date':en_date
                                                            }
                                                )
        
        # ClickHouse 
        # Удаление заданного периода из таблицы и загрузка нового
        reload_ch = PythonOperator(
            task_id='reload_clickhouse_period',
            python_callable=reload_ch_period,
            op_kwargs={
                'connection_id': ch_connection,
                'start': st_date,
                'end': en_date
            }
        )

        task_end = DummyOperator(task_id="end")    
        
        task_start >> load_dict_tables >> load_fact_tables >> load_fact_traffic >> load_fact_coupons >> load_mart >> reload_ch >> task_end
