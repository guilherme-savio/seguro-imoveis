# Seguro Imóveis

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Projeto desenvolvido para matéria de Engenharia de Dados, que consta no desenvolvimento completo de uma pipeline de dados para um sistema de seguro de imóveis, partindo da criação de ambientes em cloud utilizando IaC (Terraform), seguindo para os processos de ingestão, transformação e carregamento dos dados utilizando Azure Databricks e Azure Datalake Storage Gen2. Ao final da pipeline, os dados manipulados são exibidos em um dashboard feito com Power BI.  

## Começando

Essas instruções permitirão que você obtenha uma cópia do projeto em operação na sua máquina local para fins de desenvolvimento e teste.

Consulte **[Implantação](#-implanta%C3%A7%C3%A3o)** para saber como implantar o projeto.

## Arquitetura Utilizada

![image](https://github.com/guilherme-savio/seguro-imoveis/blob/main/assets/pipeline_arch.png)


## Pré-requisitos

* <a href="https://developer.hashicorp.com/terraform/install">Terraform</a>
* <a href="https://azure.microsoft.com/pt-br/free/databricks">Conta Microsoft/Azure com assinatura paga</a>**
* <a href="https://www.python.org/downloads/">Python</a>

<sup><sub>** Existe a possibilidade de adquirir 14 dias gratuitos dos serviços premium ofertados pela Microsoft/Azure, verifique a disponibilidade no mesmo site informado</sub></sup>


## Instalação

1. Clone o repositório
   ```bash
   git clone https://github.com/guilherme-savio/seguro-imoveis.git
   ```
2. Com sua conta Microsoft/Azure criada e apta para uso dos recursos pagos, no <a href="https://portal.azure.com/">```Portal Azure```</a> crie um workspace Azure Databricks seguindo a <a href="https://learn.microsoft.com/en-us/azure/databricks/getting-started/">```documentação```</a> fornecida pela Microsoft. Durante a execução deste processo, você irá criar um ```resource group```. Salve o nome informado no ```resource group``` pois ele será utilizado logo em seguida.
3. Com o ```Terraform``` instalado e o ```resource group``` em mãos, no arquivo <a href="https://github.com/guilherme-savio/seguro-imoveis/blob/main/iac/variables.tf">```/iac/variables.tf```</a> modifique a seguinte váriavel adicionando o ```resource group``` que você criou previamente.  

![image](https://github.com/guilherme-savio/seguro-imoveis/blob/main/assets/readme/terraform_var.png)

4. Nesta etapa, iremos iniciar o deploy do nosso ambiente cloud. Após alterar a variável no último passo, acesse a pasta ```/iac``` e execute os seguintes comandos:
   ```bash
   terraform init
   ```

   ```bash
   terraform apply
   ```
5. Com a execução dos comandos finalizada, verifique no <a href="https://portal.azure.com/">```Portal Azure```</a> o ```MS SQL Server```, ```MS SQL Database``` e o ```ADLS Gen2``` contendo os containers ```landing-zone```, ```bronze```, ```silver``` e ```gold``` que foram criados no passo anterior. 

6. No <a href="https://portal.azure.com/">```Portal Azure```</a>, gere um ```SAS TOKEN``` para o contâiner ```landing-zone``` seguindo esta <a href="https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers#create-sas-tokens-in-the-azure-portal">```documentação```</a>. Guarde este token em um local seguro pois ele será utilizado no próximo passo. 

7. Na pasta <a href="https://github.com/guilherme-savio/seguro-imoveis/tree/main/data">```/data```</a>, crie um arquivo chamado ```.env``` com o mesmo conteúdo disponibilizado no arquivo de exemplo <a href="https://github.com/guilherme-savio/seguro-imoveis/tree/main/data">```.env.example```</a> e preencha as informações necessárias.

8. No mesmo diretório, vamos iniciar o processo de população do nosso banco de dados. Verifique corretamente o preenchimento das váriaveis no arquivo ```.env``` e prossiga com os seguintes comandos:
   1. Criar ```venv``` (ambiente virtual) do Python:
        ```bash
        python3 -m venv env
        ```
   2. Ativar a ```venv``` criada:
      - Linux/MacOS:
        ```bash
        source env/bin/activate
        ```
      - Windows:
        ```pwsh
        .env\Scripts\activate
        ```
   3. Instalar os pacotes necessários:
      ```bash
      pip install -r requirements.txt
      ```
   4. Executar o script de população:
      ```bash
      python -B main.py
      ``` 
9. Acesse o <a href="https://portal.azure.com/">```Portal Azure```</a> e acesse o seu workspace Azure Databricks. Realize o upload dos notebooks encontrados em <a href="https://github.com/guilherme-savio/seguro-imoveis/tree/main/etl">```/etl```</a> para o workspace.
10. Por fim, você pode executá-los separadamente ou elaborar um Job para orquestrar às execuções.

## Implantação

Para realizar a implantação em um sistema ativo, inicialmente é necessário um estudo de custo sobre os modelos de armazenamento utilizados e clusters alocados. Após o estudo, é preciso modificar os arquivos IaC para se adequarem as novas configurações de ambiente propostas.

## Ferramentas utilizadas

* <a href="https://www.terraform.io/">Terraform<a/> - Automação de infraestrutura para provisionar e gerenciar recursos em qualquer nuvem ou data center.
* <a href="https://azure.microsoft.com/pt-br/products/databricks">Azure Databricks<a/> - Análise e processamento de Big Data
* <a href="https://azure.microsoft.com/pt-br/products/azure-sql/database">Azure SQL Server<a/> - Sistema de gerenciamento de banco de dados relacional
* <a href="https://learn.microsoft.com/pt-br/azure/storage/blobs/data-lake-storage-introduction">Azure Datalake Storage Gen2<a/> - Plataforma para armazenar, gerenciar e analisar dados na nuvem

## Autores

Mencione todos aqueles que ajudaram a levantar o projeto desde o seu início

* **Bruno Venturini** - *Ingestão, ETL, DDL* - [https://github.com/Bruno-Venturini](https://github.com/Bruno-Venturini)
* **Eduardo Freitas** - *Infraestrutura, ETL* - [https://github.com/dufrtss](https://github.com/dufrtss)
* **Gabriel Della** - *DDL, Diagramas* - [https://github.com/GabrielSouzaDG](https://github.com/GabrielSouzaDG)
* **Gabriel Ferreira** - *Dashboard, ETL* - [(https://github.com/GabrielGuinzani)](https://github.com/GabrielGuinzani)
* **Guilherme Savio** - *Arquitetura, ETL, Infraestrutura* - [https://github.com/guilherme-savio](https://github.com/guilherme-savio)
* **Higor Goulart** - *População, ETL* - [https://github.com/higorgoulart](https://github.com/higorgoulart)
* **Sofia Martins** - *Documentação, ETL, Dashboard* - [https://github.com/SofiaMartinslv](https://github.com/SofiaMartinslv)

Você também pode ver a lista de todos os [colaboradores](https://github.com/guilherme-savio/seguro-imoveis/graphs/contributors) que participaram deste projeto.

### Modelo Físico:
Utilizando [dbdiagram](https://dbdiagram.io/), confira o nosso [Modelo Físico Dimensional](https://github.com/guilherme-savio/seguro-imoveis/blob/main/assets/modelo_fisico_relacional.png).
<div>
    <img src="https://github.com/guilherme-savio/seguro-imoveis/blob/main/assets/modelo_fisico_dimensional.png" style="heigh: 400px; width: 600px;">
</div>

Confira também nosso [Modelo Físico Relacional](https://github.com/guilherme-savio/seguro-imoveis/blob/main/assets/modelo_fisico_relacional.png).
<div>
    <img src="https://github.com/guilherme-savio/seguro-imoveis/blob/main/assets/modelo_fisico_relacional.png" style="heigh: 400px; width: 600px;">
</div>


## Referências

* *Connect to Azure Data Lake Storage Gen2 and Blob Storage* - [Databricks](https://docs.databricks.com/en/connect/storage/azure-storage.html)
* *Creating Fake Data In Python Using Faker* - [Faker](https://www.udacity.com/blog/2023/03/creating-fake-data-in-python-using-faker.html)
* *Getting Started with Delta Lake* - [Delta.io](https://delta.io/learn/getting-started/)
