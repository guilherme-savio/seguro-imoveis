# Introdução

Esse projeto foi desenvolvido para a disciplina de Engenharia de Dados, que consta no desenvolvimento completo de uma pipeline de dados para um sistema de seguro de imóveis, partindo da criação de ambientes em cloud utilizando **IaC (Terraform)**, seguindo para os processos de ingestão, transformação e carregamento dos dados utilizando **Azure Databricks** e **Azure Datalake Storage Gen2**. Ao final da pipeline, os dados manipulados são exibidos em um dashboard feito com Power BI.  

## Começando
Essas instruções permitirão que você obtenha uma cópia do projeto em operação na sua máquina local para fins de desenvolvimento e teste.

### Pré-requisitos
---
* [Terraform](https://developer.hashicorp.com/terraform/install)
* [Conta Microsoft/Azure com assinatura paga](https://azure.microsoft.com/pt-br/free/databricks)
* [Python](https://www.python.org/downloads/)

### Instalação
---

1. Clone o repositório

    ``` bash
    git clone https://github.com/guilherme-savio/seguro-imoveis.git
    ```

2. Com sua conta Microsoft/Azure criada e apta para uso dos recursos pagos, no [Portal Azure](https://portal.azure.com/) crie um workspace Azure Databricks seguindo a [documentação](https://learn.microsoft.com/en-us/azure/databricks/getting-started/) fornecida pela Microsoft. Durante a execução deste processo, você irá criar um ```resource group```. Salve o nome informado no ```resource group``` pois ele será utilizado logo em seguida.

3. Com o Terraform instalado e o resource group em mãos, no arquivo ```/iac/variables.tf``` troque a váriavel ```"resource_group_name"``` para resource group que você criou previamente.

4. Nesta etapa, iremos iniciar o deploy do nosso ambiente cloud. Após alterar a variável no último passo, acesse a pasta ```/iac``` e execute os seguintes comandos:
    ```bash
    terraform init
    ```
   ```bash
   terraform apply
   ```
5. Com a execução dos comandos finalizada, verifique no [Portal Azure](https://portal.azure.com/) o ```MS SQL Server```, ```MS SQL Database``` e o ```ADLS Gen2``` contendo os containers ```landing-zone```, ```bronze```, ```silver``` e ```gold``` que foram criados no passo anterior. 

6. No [Portal Azure](https://portal.azure.com/), gere um ```SAS TOKEN``` para o contêiner ```landing-zone``` seguindo esta [documentação](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers#create-sas-tokens-in-the-azure-portal). Guarde este token em um local seguro pois ele será utilizado no próximo passo. 

7. Na pasta ```/data```, crie um arquivo chamado ```.env``` com o mesmo conteúdo disponibilizado no arquivo de exemplo ```.env.example``` e preencha as informações necessárias.

8. No mesmo diretório, vamos iniciar o processo de população do nosso banco de dados. Verifique corretamente o preenchimento das váriaveis no arquivo ```.env``` e prossiga com os seguintes comandos:
    * Criar ```venv``` (ambiente virtual) do Python:
            ```bash
            python3 -m venv env
            ```
    * Ativar a ```venv``` criada:
        - Linux/MacOS:
            ```bash
            source env/bin/activate
            ```
        - Windows:
            ```pwsh
            .env\Scripts\activate
            ```
    * Instalar os pacotes necessários:
        ```bash
        pip install -r requirements.txt
        ```
    * Executar o script de população:
        ```bash
        python -B main.py
        ``` 
9. Acesse o <a href="https://portal.azure.com/">```Portal Azure```</a> e acesse o seu workspace Azure Databricks. Realize o upload dos notebooks encontrados em <a href="https://github.com/guilherme-savio/seguro-imoveis/tree/main/etl">```/etl```</a> para o workspace.
10. Por fim, você pode executá-los separadamente ou elaborar um Job para orquestrar às execuções.

## Ferramentas utilizadas
---
* [Terraform](https://www.terraform.io/) - Automação de infraestrutura para provisionar e gerenciar recursos em qualquer nuvem ou data center.
* [Azure Databricks](https://azure.microsoft.com/pt-br/products/databricks) - Análise e processamento de Big Data
* [Azure SQL Server](https://azure.microsoft.com/pt-br/products/azure-sql/database) - Sistema de gerenciamento de banco de dados relacional
* [Azure Datalake Storage Gen2](https://learn.microsoft.com/pt-br/azure/storage/blobs/data-lake-storage-introduction) - Plataforma para armazenar, gerenciar e analisar dados na nuvem

