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

def get_data_from_table(table_name, **kwargs):
    sql = f"SELECT * FROM {table_name}"
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    df = mssql_hook.get_pandas_df(sql)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    kwargs['ti'].xcom_push(key=f'data_csv_{table_name}', value=csv_buffer.getvalue())

def upload_to_adls(table_name, **kwargs):
    data_csv = kwargs['ti'].xcom_pull(key=f'data_csv_{table_name}')
    file_name = f'dados/{table_name}.csv'
    wasb_hook = WasbHook(wasb_conn_id='azure_data_lake_default')
    wasb_hook.load_string(data_csv, container_name='landing-zone', blob_name=file_name, overwrite=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='mssql_to_adls_v2',
    default_args=default_args,
    description='Copia dados do SQL Server para o ADLS',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    table_names = get_table_names()
    
    for table_name in table_names:
        get_data_task = PythonOperator(
            task_id=f'get_data_from_{table_name}',
            python_callable=get_data_from_table,
            op_kwargs={'table_name': table_name},
            provide_context=True,
        )

        upload_data_task = PythonOperator(
            task_id=f'upload_{table_name}_to_adls',
            python_callable=upload_to_adls,
            op_kwargs={'table_name': table_name},
            provide_context=True,
        )

        get_data_task >> upload_data_task
