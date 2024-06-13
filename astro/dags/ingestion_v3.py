from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.dates import days_ago
import pandas as pd
import io

def get_table_names():
    sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    tables = mssql_hook.get_pandas_df(sql)
    return tables['TABLE_NAME'].tolist()

def get_all_data_from_sql(**kwargs):
    table_names = get_table_names()
    data_dict = {}
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    
    for table_name in table_names:
        sql = f"SELECT * FROM {table_name}"
        df = mssql_hook.get_pandas_df(sql)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        data_dict[table_name] = csv_buffer.getvalue()
    
    kwargs['ti'].xcom_push(key='data_dict', value=data_dict)

def upload_all_to_adls(**kwargs):
    data_dict = kwargs['ti'].xcom_pull(key='data_dict')
    wasb_hook = WasbHook(wasb_conn_id='azure_data_lake_default')
    
    for table_name, data_csv in data_dict.items():
        file_name = f'dados/{table_name}.csv'
        wasb_hook.load_string(data_csv, container_name='landing-zone', blob_name=file_name, overwrite=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='mssql_to_adls_v3',
    default_args=default_args,
    description='Copia dados do SQL Server para o ADLS',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    get_all_data_task = PythonOperator(
        task_id='get_all_data_from_sql',
        python_callable=get_all_data_from_sql,
        provide_context=True,
    )

    upload_all_data_task = PythonOperator(
        task_id='upload_all_to_adls',
        python_callable=upload_all_to_adls,
        provide_context=True,
    )

    get_all_data_task >> upload_all_data_task
