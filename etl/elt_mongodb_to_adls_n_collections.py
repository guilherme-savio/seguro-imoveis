import pandas as pd
from pymongo import MongoClient
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError

import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do MongoDB
mongo_uri = os.getenv("MONGODB_URI")  # URI de conexão do MongoDB, por exemplo: "mongodb://usuario:senha@host:porta/"
database_name = os.getenv("MONGODB_DATABASE")  # Nome do banco de dados MongoDB

# Configurações do Azure Data Lake Storage
account_name = os.getenv("ADLS_ACCOUNT_NAME")
file_system_name = os.getenv("ADLS_FILE_SYSTEM_NAME")
directory_name = database_name
sas_token = os.getenv("ADLS_SAS_TOKEN")

# Conectar ao MongoDB
client = MongoClient(mongo_uri)
db = client[database_name]

# Listar todas as coleções do banco de dados
collections = db.list_collection_names()

# Criar cliente do Azure Data Lake Storage
file_system_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", 
                                           credential=sas_token,
                                           api_version="2020-02-10")

# Tentar criar o diretório, se não existir
try:
    directory_client = file_system_client.get_file_system_client(file_system_name).get_directory_client(directory_name)
    directory_client.create_directory()
except ResourceExistsError:
    print(f"O diretório '{directory_name}' já existe.")

# Para cada coleção encontrada, ler os dados e carregar para o Azure Data Lake Storage
for collection_name in collections:
    collection = db[collection_name]
    df = pd.DataFrame(list(collection.find()))  # Ler todos os documentos da coleção em um DataFrame
    
    # Carregar os dados para o Azure Data Lake Storage
    file_client = directory_client.get_file_client(f"{collection_name}.csv")
    data = df.to_csv(index=False).encode()
    file_client.upload_data(data, overwrite=True)
    print(f"Dados da coleção '{collection_name}' carregados com sucesso.")
