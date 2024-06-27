# Documentação da Camada Silver

Bem-vindo à documentação da camada Silver. Aqui você encontrará informações sobre as transformações realizadas e a referência da API para as funções de transformação.

## Transformações da Camada Silver

A camada Silver realiza transformações nos dados brutos da camada Bronze para prepará-los para a camada Gold. As transformações específicas realizadas são detalhadas abaixo.

### Transformação de Cliente

A transformação de cliente inclui:
- Manter apenas os 10 últimos números da coluna `telefone`.
- Transformar `dt_nasc` em `data_nascimento`.
- Renomear colunas para letras maiúsculas e padronizar prefixos `ID_` para `CODIGO_` e `DT_` para `DATA_`.
- Adicionar colunas `FILENAME_BRONZE` e `DATA_INSERT_SILVER`.

#### Código

```python
def transformar_cliente():
    df_cliente = spark.read.format("delta").load(f"/mnt/{storageAccountName}/bronze/cliente")
    df_cliente = df_cliente.withColumn("telefone", regexp_replace("telefone", "[^0-9]", ""))
    df_cliente = df_cliente.withColumn("telefone", col("telefone").substr(-10, 10))
    df_cliente = df_cliente.withColumnRenamed("dt_nasc", "data_nascimento")
    df = renomear_colunas(df_cliente, "cliente")

    salvar_silver(df, "cliente")
```

### Tabela de conversão silver
| CODIGO_APOLICE | CODIGO_COBERTURA | FILENAME_BRONZE  | DATA_INSERT_SILVER      |
|----------------|------------------|------------------|-------------------------|
| 1              | 5                | apolice_cobertura| 2024-06-27 03:11:...    |
| 1              | 10               | apolice_cobertura| 2024-06-27 03:11:...    |
| 1              | 6                | apolice_cobertura| 2024-06-27 03:11:...    |
| 2              | 1                | apolice_cobertura| 2024-06-27 03:11:...    |
| 2              | 9                | apolice_cobertura| 2024-06-27 03:11:...    |
| 2              | 3                | apolice_cobertura| 2024-06-27 03:11:...    |
| 2              | 8                | apolice_cobertura| 2024-06-27 03:11:...    |
| 3              | 5                | apolice_cobertura| 2024-06-27 03:11:...    |
| 3              | 10               | apolice_cobertura| 2024-06-27 03:11:...    |
| 3              | 1                | apolice_cobertura| 2024-06-27 03:11:...    |
| 3              | 7                | apolice_cobertura| 2024-06-27 03:11:...    |
| 4              | 6                | apolice_cobertura| 2024-06-27 03:11:...    |
| 5              | 2                | apolice_cobertura| 2024-06-27 03:11:...    |
| 5              | 9                | apolice_cobertura| 2024-06-27 03:11:...    |
| 5              | 1                | apolice_cobertura| 2024-06-27 03:11:...    |
