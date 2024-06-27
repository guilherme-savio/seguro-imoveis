# Modelo Relacional e Inserção de Dados

A estrutura incial do banco de dados do projeto contém 8 tabelas, sendo elas as seguintes:

- `cliente`: armazena informações dos clientes;
- `imovel`: armazena informações dos imóveis, relacionando-os com proprietários e inquilinos que são clientes;
- `apolice`: armazena informações das apólices de seguros associadas a imóveis;
- `cobertura`: armazena informações das coberturas de seguro;
- `apolice_cobertura`: associação para o relacionamento muitos-para-muitos entre apólices e coberturas;
- `sinistro`: armazena informações dos sinistros associados a apólices;
- `pagamento`: armazena informações dos pagamentos associados a apólices;
- `avaliacao`: armazena informações das avaliações dos imóveis;

![image](assets\modelo_fisico_relacional.png)

## Inserção de Dados

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
