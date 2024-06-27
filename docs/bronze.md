# Documentação da Camada Bronze

## Visão Geral
A camada Bronze serve como o armazenamento de dados brutos em nosso pipeline de dados. Esta camada é projetada para capturar dados em sua forma bruta, enriquecida com metadados para fins de rastreamento e auditoria. Os dados da zona de aterrissagem (landing-zone) são ingeridos na camada Bronze no formato Delta.

## Conta de Armazenamento
- **Nome da Conta de Armazenamento:** `satcseguroimoveis`

## Tabelas Ingeridas
As seguintes tabelas são ingeridas da landing-zone e armazenadas na camada Bronze:
- `apolice_cobertura`
- `cobertura`
- `sinistro`
- `pagamento`
- `apolice`
- `avaliacao`
- `imovel`
- `cliente`

## Processo de Ingestão de Dados
O processo de ingestão de dados envolve os seguintes passos:

1. **Leitura dos Dados:** Os dados são lidos da landing-zone no formato Parquet.
2. **Adição de Metadados:**
   - `dt_insert_bronze`: Timestamp de quando os dados foram inseridos na camada Bronze.
   - `filename`: Nome da tabela de origem.
3. **Gravação dos Dados:** Os dados são gravados na camada Bronze no formato Delta.

### Código de Exemplo
Abaixo está o código PySpark utilizado para ler os dados da landing-zone, adicionar metadados e gravá-los na camada Bronze.

```python
from pyspark.sql.functions import current_timestamp, lit
storageAccountName = "satcseguroimoveis"
tables = ["apolice_cobertura", "cobertura", "sinistro", "pagamento", "apolice", "avaliacao", "imovel", "cliente"]
for table in tables:
    remote_table = spark.read.option("inferschema", "true").option("header", "true").parquet(f"/mnt/{storageAccountName}/landing-zone/{table}")
    remote_table = remote_table.withColumn("dt_insert_bronze", current_timestamp()).withColumn("filename", lit(table))
    remote_table.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/{table}")
```

