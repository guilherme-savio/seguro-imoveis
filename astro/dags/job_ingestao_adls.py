from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.dates import days_ago
import pandas as pd
import io

def get_data_from_sql(**kwargs):
    sql = "SELECT * FROM carro"  # Substitua pelo nome da sua tabela
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    df = mssql_hook.get_pandas_df(sql)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    kwargs['ti'].xcom_push(key='data_csv', value=csv_buffer.getvalue())

def upload_to_adls(**kwargs):
    data_csv = kwargs['ti'].xcom_pull(key='data_csv')
    file_name = 'dados\carro.csv'  # Substitua pelo nome do arquivo desejado
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
    dag_id='mssql_to_adls',
    default_args=default_args,
    description='Copia dados do SQL Server para o ADLS',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    get_data_task = PythonOperator(
        task_id='get_data_from_sql',
        python_callable=get_data_from_sql,
        provide_context=True,
    )

    upload_data_task = PythonOperator(
        task_id='upload_to_adls',
        python_callable=upload_to_adls,
        provide_context=True,
    )

    get_data_task >> upload_data_task
