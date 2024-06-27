# Arquitetura de Solução

A arquitetura é composta pelos seguintes componentes principais:

- `Origem`: Os dados do ambiente relacional;
- `Ingestão`: Etapa em que os dados do ambiente relacional são salvos no formato .parquet no container landing-zone;
- `Armazenamento`: Representa a estrutura de armazenamento da pipeline. Indica o fluxo de processamento, os containeres existentes na nuvem e as tecnologias usadas;
- `Processamento e Orquestração`: Expõe as ferramentas responsáveis por essas tarefas;
- `Análise`: Simboliza o ambiente final onde ocorre o consumo e análise dos dados tratados;

![image](assets\arch.png)

## Origem

A partir de um script em Python, são criadas as tabelas e populada toda a cadeia de dados para N clientes.

```python 
total_clients = 100

clientes = insert_clientes(session, total_clients)
log(f"{len(clientes)} clientes inseridos.")

imoveis = insert_imoveis(session, clientes, tipos_imovel)
log(f"{len(imoveis)} imóveis inseridos.")

apolices = insert_avaliacoes_apolices(session, imoveis)
log(f"{len(apolices)} apólices e avaliações inseridas.")

sinistros, pagamentos, apolice_coberturas = await insert_sinistros_pagamentos_apolices_coberturas(session, apolices, imoveis, coberturas)
log(f"{len(sinistros)} sinistros, {len(pagamentos)} pagamentos, {len(apolice_coberturas)} apolice_coberturas inseridos.")

session.commit()
log("Todos registros salvos.")
```

Para cada cliente, podem existir de 1 a 4 imóveis. A partir desses imóveis, obtém-se a mesma quantidade em apólices e avaliações. Após isso, por cada apólice, tem-se a criação de 0 a 4 sinistros, 1 a 10 tipos de coberturas e 1 pagamento por mês desde a data de início da apólice até o dia atual.

Ao final do processo, a base de dados obteve um total de 337.010 linhas a partir da soma de todas as tabelas.

| linhas | tabela |
| :--- | :--- |
| 58332 | apolice\_cobertura |
| 10 | cobertura |
| 18289 | sinistro |
| 207253 | pagamento |
| 15042 | apolice |
| 15042 | avaliacao |
| 15042 | imovel |
| 8000 | cliente |
