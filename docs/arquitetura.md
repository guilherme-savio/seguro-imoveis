# Arquitetura de Solução

A pipeline é construída em cima do core de ferramentas de análise de dados e engenharia de dados da Azure. Por estar centralizada em apenas um ambiente cloud, todo o processo de deploy, armazenamento, orquestramento do fluxo de processamento dos dados e custos se torna simples de ser feito, sendo esse um ponto chave para a utilização.

![image](assets\pipeline_arch.png)

O processo conta com os seguintes componentes:

- `Origem`: Representa os dados do ambiente relacional que estão armazenados em um banco de dados ```Azure SQL Server```;
- `Ingestão`: Etapa em que os dados do ambiente relacional são salvos no formato .parquet no container landing-zone por meio do ```Azure Databricks``` e ```Apache Spark```;
- `Processamento e Orquestração`: Expõe as ferramentas (anteriormente citadas) responsáveis por essas tarefas;
- `Armazenamento`: Representa a estrutura de armazenamento do ```Azure Datalake Storage Gen2``` da pipeline. Indica o fluxo de processamento, os containeres existentes na nuvem e as tecnologias usadas;
- `Análise`: Simboliza o ambiente final onde ocorre o consumo e análise dos dados tratados em um dashboard criado com ```Power BI```;

## Origem

O ambiente relacional indicado, vem de um banco de dados Azure SQL Server criado previamente com Terraform.

```terraform
Exemplo do SQL Server e SQL Database que são criados:

resource "azurerm_mssql_server" "sql" {
  name                         = "satcseguroimoveissqlserver"
  resource_group_name          = var.resource_group_name
  location                     = var.resource_group_location
  version                      = "12.0"
  administrator_login          = var.mssql_admin
  administrator_login_password = var.password
}

resource "azurerm_mssql_database" "sql" {
  name                        = "satcseguroimoveisdatabase"
  server_id                   = azurerm_mssql_server.sql.id
  collation                   = "SQL_Latin1_General_CP1_CI_AS"
  auto_pause_delay_in_minutes = -1
  max_size_gb                 = 2
  read_replica_count          = 0
  read_scale                  = false
  sku_name                    = "Basic"
  zone_redundant              = false
  geo_backup_enabled          = false
}
```

Para mais informações a respeito do formato de tabelas ou da origem dos dados analisados, você pode conferir na documentação [aqui!](relational.md)

## Ingestão

O processo de ingestão ocorre internamente no Azure Databricks, por meio de um Jupyter Notebook que transforma os dados da origem para o formato .parquet, armazenando no container landing-zone. Para mais informações a respeito do script de ingestão você pode conferir [aqui!](etl.md)

## Processamento e Orquestração

Também gerenciado pelo Azure Databricks, as tarefas de processamento são orquestradas para acontecerem em cadeia, dependendo do sucesso da execução da tarefa anterior. Partindo do container landing-zone, os restantes utilizam o formato de armazenamento de tabelas ```Delta```, caracterizando o ADLS como um ```Deltalake```, garantindo a atomicidade, consistência, isolamento e durabilidade das operações.

## Armazenamento

O armazenamento de dados ocorre num blob storage centralizado para todos os dados que foram coletados, que estão em processamento e que já estão disponíveis para análise. Orientado à arquitetura medalhão, é dividido em 4 containers, sendo eles:

- `landing-zone`: Camada responsável pelo armazenamento dos dados brutos oriundos do ambiente relacional no formato parquet;
- `bronze`: Os dados da camada landing-zone no formato de Delta-table com histórico de extração e registro do arquivo de origem.
- `silver`: Os dados da camada bronze no formato de Delta-table com desnormalizações, histórico de extração e registro do tabela delta de origem;
- `gold`: Os dados da camada silver no formato de Delta-table completamete desnormalizados e organizados numa OBT (One Big Table) baseado nas métricas e KPI's requisitados para análise;

## Análise

O componente de análise é responsável por exibir num dashboard os dados tratados que estão na camada gold. Pelo PowerBI, os consumidores visualizam as métricas e KPI's necessitados.
