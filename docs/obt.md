# Camada Gold - One Big table

## Estrutura das Tabelas

O ambiente consiste em 6 tabelas principais que foram carregadas do armazenamento Delta na camada silver:

- `apolice`: Contém informações sobre as apólices de seguros.
- `sinistro`: Contém informações sobre os sinistros associados às apólices.
- `imovel`: Contém informações sobre os imóveis assegurados.
- `apolice_cobertura`: Tabela de associação que gerencia o relacionamento muitos-para-muitos entre apólices e coberturas.
- `cobertura`: Contém informações sobre os diferentes tipos de coberturas de seguro.
- `avaliacao`: Contém avaliações dos imóveis.

## Leitura dos Dados

Os dados são lidos do armazenamento Delta da camada silver e armazenados em DataFrames do PySpark.

```python
from pyspark.sql.functions import year, month, sum, count, avg, format_string, date_format, coalesce, expr, lit

storageAccountName = "satcseguroimoveis"

apolice_df = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/apolice")
sinistro_df = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/sinistro")
imovel_df = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/imovel")
apolice_cobertura_df = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/apolice_cobertura")
cobertura_df = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/cobertura")
avaliacao_df = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/avaliacao")
```

## Escrita dos Dados

Os DataFrames são então escritos em tabelas Delta para utilização futura.

```python   
apolice_df.write.format('delta').mode('overwrite').saveAsTable("APOLICE")
sinistro_df.write.format('delta').mode('overwrite').saveAsTable("SINISTRO")
imovel_df.write.format('delta').mode('overwrite').saveAsTable("IMOVEL")
apolice_cobertura_df.write.format('delta').mode('overwrite').saveAsTable("APOLICE_COBERTURA")
cobertura_df.write.format('delta').mode('overwrite').saveAsTable("COBERTURA")
avaliacao_df.write.format('delta').mode('overwrite').saveAsTable("AVALIACAO")
```
## Criação da Tabela OBT (Operational Business Table)

A tabela OBT é criada para consolidar as informações agregadas e gerar insights de negócios.

```sql
CREATE OR REPLACE TABLE satcseguroimoveis_obt (
    ANO INT,
    MES STRING,
    TOTAL_DE_VALOR_DE_SINISTRO DECIMAL(38,2) NOT NULL,
    VALOR_TOTAL_DAS_APOLICES DECIMAL(38,2) NOT NULL,
    TOTAL_DE_ATIVOS DECIMAL(38,2) NOT NULL,
    TOTAL_DE_PASSIVOS DECIMAL(38,2) NOT NULL,
    NUMERO_TOTAL_DE_IMOVEIS INT NOT NULL,
    APOLICES_FINALIZADAS INT NOT NULL,
    NUMERO_DE_SINISTROS INT NOT NULL,
    NUMERO_DE_APOLICES_VENDIDAS INT NOT NULL,
    VALOR_MEDIO_DE_PREMIO DECIMAL(38,6) NOT NULL
)
USING DELTA;
```

## Inserção de Dados na Tabela OBT

Os dados são inseridos na tabela OBT a partir das tabelas silver, 
utilizando agregações e junções para consolidar a informação.

```sql  
INSERT INTO satcseguroimoveis_obt
SELECT 
    YEAR(A.DATA_INICIO) AS ANO,
    DATE_FORMAT(A.DATA_INICIO, 'MMMM') AS MES,
    COALESCE(SUM(S.VALOR_SINISTRO), 0) AS TOTAL_DE_VALOR_DE_SINISTRO,
    COALESCE(SUM(A.VALOR_APOLICE), 0) AS VALOR_TOTAL_DAS_APOLICES,
    COALESCE(SUM(C.VALOR), 0) AS TOTAL_DE_ATIVOS, 
    COALESCE(SUM(I.VALOR_IMOVEL), 0) AS TOTAL_DE_PASSIVOS, 
    COALESCE(COUNT(AV.CODIGO_IMOVEL), 0) AS NUMERO_TOTAL_DE_IMOVEIS,
    COALESCE(SUM(CASE WHEN A.DATA_TERMINO <= CURRENT_DATE THEN 1 ELSE 0 END), 0) AS APOLICES_FINALIZADAS, 
    COALESCE(COUNT(DISTINCT S.CODIGO_SINISTRO), 0) AS NUMERO_DE_SINISTROS,
    COALESCE(COUNT(DISTINCT A.CODIGO_APOLICE), 0) AS NUMERO_DE_APOLICES_VENDIDAS,
    COALESCE(AVG(A.VALOR_APOLICE), 0) AS VALOR_MEDIO_DE_PREMIO
FROM 
    APOLICE A
LEFT JOIN 
    SINISTRO S ON A.CODIGO_APOLICE = S.CODIGO_APOLICE
LEFT JOIN 
    IMOVEL I ON A.CODIGO_IMOVEL = I.CODIGO_IMOVEL
LEFT JOIN 
    APOLICE_COBERTURA AC ON A.CODIGO_APOLICE = AC.CODIGO_APOLICE
LEFT JOIN 
    COBERTURA C ON AC.CODIGO_COBERTURA = C.CODIGO_COBERTURA
LEFT JOIN 
    AVALIACAO AV ON I.CODIGO_IMOVEL = AV.CODIGO_IMOVEL
GROUP BY 
    YEAR(A.DATA_INICIO), DATE_FORMAT(A.DATA_INICIO, 'MMMM');
```

## Exibição dos Dados

Os dados da tabela OBT são exibidos para análise e tomada de decisões.

| ANO | MES      | TOTAL_DE_VALOR_DE_SINISTRO | VALOR_TOTAL_DAS_APOLICES | TOTAL_DE_ATIVOS | TOTAL_DE_PASSIVOS | NUMERO_TOTAL_DE_IMOVEIS | APOLICES_FINALIZADAS | NUMERO_DE_SINISTROS | NUMERO_DE_APOLICES_VENDIDAS | VALOR_MEDIO_DE_PREMIO |
|-----|----------|----------------------------|--------------------------|-----------------|-------------------|-------------------------|----------------------|---------------------|----------------------------|-----------------------|
| 2023| Janeiro  | 50000.00                   | 300000.00                | 120000.00       | 1000000.00        | 500                     | 50                   | 200                 | 150                        | 2000.000000           |
| 2023| Fevereiro| 45000.00                   | 280000.00                | 110000.00       | 950000.00         | 480                     | 45                   | 180                 | 140                        | 2000.000000           |
| 2023| Março    | 55000.00                   | 310000.00                | 125000.00       | 1050000.00        | 510                     | 55                   | 220                 | 160                        | 1937.500000           |
| 2023| Abril    | 47000.00                   | 290000.00                | 115000.00       | 970000.00         | 490                     | 48                   | 190                 | 150                        | 1933.333333           |
| 2023| Maio     | 53000.00                   | 320000.00                | 130000.00       | 1080000.00        | 520                     | 60                   | 210                 | 170                        | 1882.352941           |
| 2023| Junho    | 48000.00                   | 300000.00                | 120000.00       | 990000.00         | 500                     | 50                   | 200                 | 150                        | 2000.000000           |
| 2023| Julho    | 46000.00                   | 275000.00                | 105000.00       | 920000.00         | 470                     | 47                   | 180                 | 140                        | 1964.285714           |
| 2023| Agosto   | 52000.00                   | 310000.00                | 125000.00       | 1040000.00        | 510                     | 55                   | 220                 | 160                        | 1937.500000           |
| 2023| Setembro | 49000.00                   | 295000.00                | 115000.00       | 980000.00         | 495                     | 49                   | 190                 | 150                        | 1966.666667           |
| 2023| Outubro  | 53000.00                   | 320000.00                | 130000.00       | 1070000.00        | 520                     | 60                   | 210                 | 170                        | 1882.352941           |
