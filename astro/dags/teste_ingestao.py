import pandas as pd
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError

import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

import pyodbc
import os
from urllib.parse import quote_plus
    
def teste_ingestao():
    # Configurações do SQL Server
    server = '192.168.0.197'
    database = 'dados'
    schema = database
    table_name = 'sinistro'
    username = 'sa'
    password = 'satc@2024'

    # Configurações do Azure Data Lake Storage
    account_name = 'datalake6a11231d1ee887ec'
    file_system_name = 'landing-zone'
    directory_name = database
    sas_token = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-06-12T11:16:26Z&st=2024-06-12T03:16:26Z&spr=https&sig=zAIQ7OcHWGkdX5TKLTXy1W%2B0ZY7hfwcPO%2Bf3nmmUTHM%3D'

    from urllib.parse import quote_plus

    if any(var is None for var in [server, database, schema, table_name, username, password, account_name, file_system_name, sas_token]):
        raise ValueError("Uma ou mais variáveis de ambiente não estão definidas")

    # Supondo que você tenha a senha armazenada em uma variável chamada 'password'
    password = quote_plus(str(password))  # Garantir que a senha seja uma string

    # Consulta SQL
    query = f"SELECT * FROM {schema}.{table_name}"

    # Conectar ao SQL Server e ler os dados
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    df = pd.read_sql(query, conn_str)

    # Escrever os dados no Azure Data Lake Storage
    file_system_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", 
                                            credential=sas_token,
                                            api_version="2020-02-10")  # Definir a versão da API explicitamente

    # Tentar criar o diretório, se não existir
    try:
        directory_client = file_system_client.get_file_system_client(file_system_name).get_directory_client(directory_name)
        directory_client.create_directory()
    except ResourceExistsError:
        print(f"O diretório '{directory_name}' já existe.")
    
    # Carregar o arquivo para o Azure Data Lake Storage
    file_client = directory_client.get_file_client(f"{table_name}.csv")

    # Criar o arquivo
    file_client.create_file()

    # Converter DataFrame para CSV e obter os dados como bytes
    data = df.to_csv(index=False).encode()

    # Carregar os dados
    file_client.upload_data(data, overwrite=True)
    
default_args = {
    'owner': 'jorge',
    'start_date': datetime(2020, 11, 18),
    'retries': 10,
	  'retry_delay': timedelta(hours=1)
}

with airflow.DAG(
    'teste_ingestao',
    default_args=default_args,
    description='Ingestão de dados do SQL Server para o ADLS',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    ingestao_task = PythonOperator(
        task_id='teste_ingestao',
        python_callable=teste_ingestao,
    )

    ingestao_task
    
