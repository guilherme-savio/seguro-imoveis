#!/bin/bash

# Passo 1: Crie uma venv no python
python3 -m venv env

# Passo 2: Ative essa venv
source env/bin/activate

# Passo 3: Execute pip install -r requirements.txt
pip install -r requirements.txt

cd env/lib/python3.10/site-packages/pyspark/jars

# Passo 4: Baixe os jars necessários para o pyspark
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.3/hadoop-azure-3.3.3.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.3.3/hadoop-azure-datalake-3.3.3.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.3/hadoop-common-3.3.3.jar

# Passo 5: Mensagem final
echo "Necessário instalar o JDK 17. Caso já possua em sua máquina, execute o comando: export JAVA_HOME=\"<local de instalação do JDK>\" dentro do ambiente virtual python."
