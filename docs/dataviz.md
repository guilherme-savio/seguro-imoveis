# Dashboard no Power BI sobre Seguro de Imóveis

## Consumo de Dados

O dashboard consome os dados de uma OBT (Operational Data Store) através de um arquivo parquet adicionado na camada gold do ETL (Extract, Transform, Load) do projeto. Esse arquivo parquet contém os dados consolidados e otimizados para análise no Power BI. A camada gold do ETL é responsável por armazenar os dados de forma estruturada e pronta para serem consumidos pelo dashboard.

Essa abordagem permite que o dashboard tenha acesso aos dados atualizados e de alta qualidade, garantindo uma visualização precisa e confiável das informações relacionadas a seguros de imóveis.

## KPIs Desenvolvidas

Aqui estão as KPIs que foram desenvolvidas para análise dos dados:

- Prêmio Total Coletado (PremioTotalColetado)
- Total de Sinistros Pagos (TotalSinistrosPagos)
- Custos Operacionais (CustosOperacionais)
- Lucro (Lucro)
- Total de Valor de Sinistro (TotalValueOfClaims)
- Valor Total das Apólices (TotalValueOfPolicies)
- Número de Apólices Vendidas (NumberOfPoliciesSold)
- Valor Médio de Prêmio (AveragePremiumValue)
- Número de Sinistros (NumberOfClaims)
- Total de Ativos (TotalAssetsValue)
- Total de Passivos (TotalLiabilitiesValue)
- Número Total de Imóveis (NumberOfProperties)

Essas KPIs fornecem insights importantes sobre o desempenho e a saúde do negócio de seguros de imóveis.

![image](assets\dashboard.png)
