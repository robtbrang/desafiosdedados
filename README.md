# **Desafio de DaTa Engineering**

### **Descrição do Projeto**
O projeto consiste em uma extração de dados da API Pública Random User que gera dados aleatoriamente a cada consulta.
Para implementação utilizei de ferramentas da Google Cloud Platform.

Foram utilizados os serviços:
* Cloud Composer;
* Cloud Storage;
* Cloud BigQuery

Etapas do Projeto:
* Etapa 01: Extração de Dados e Disponibilização em Bronze
* Etapa 01: Disponibilização dos dados em Silver

### **Desenvolvimento**

#### **Etapa 01: Extração de Dados e Disponibilização em Bronze**
O desenvolvimento foi feito totalmente em Python. Foi criada uma DAGs dentro do Google Cloud Composer, chamada: etl_extract_random_user.
Essa DAG utiliza-se de PythonOperators para extração dos dados da API com a biblioteca Requests, salvar o resultado da extração em um arquivo JSON que foi disponibilizado no Storage e também dispónibilização dos dados em ambiente Bronze no BigQuery.

#### **Etapa 02: Disponibilização dos dados em Silver**
O desenvolvimento foi feito totalmente em Python. Foram criadas DAGs dentro do Google Cloud Composer utilizando-se de BigQueryOperators para validação, criação e carregamento das tabelas existentes no BigQuery em ambiente Silver, 

### **Features**
Este projeto extrai dados da API Random User, disponibiliza a extração como um arquivo Json dentro do Storage, converte para um arquivo CSV e escreve os dados dentro do BigQuery. 

### **Configuração**
Para execução do Projeto é necessário criar um ambiente no Google Cloud Composer v1. 
Uma vez criado o ambiente, é necessário criar uma Conta de Serviço (Service Account) com roles específicas para Storage e BigQuery.
Com o usuário criado, deve ser gerado uma chave JSON para conectar o script aos serviços Storage e BigQuery.
Disponibilizar a chave para o Cloud Composer.
Carregar as DAGs dentro da pasta dags do ambiente Cloud Composer (pode ser acessada via Console), ativá-las e esperar a execução.
