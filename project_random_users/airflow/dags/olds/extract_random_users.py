import datetime

import logging
import os

import airflow
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow import AirflowException

from google.cloud import storage
from google.oauth2 import service_account

# Variables
START_DATE = datetime.datetime(2022, 5, 30)
URL_API = Variable.get("etl_extract_users_url")

default_args = {
    'owner': 'Robton R Brangaitis',
    'start_date': START_DATE,
    'schedule_interval': '@daily'
}

def upload_to_bucket(bucket_name: str, file_path: str, file_key=None) -> None:
    """
    Funcao criada para fazer upload de arquivos (objetos) para um bucket no Cloud Storage.

    :param str bucket_name: Nome do bucket dentro do Cloud Storage.
    :param str file_path: Caminho local do arquivo (objeto) que sera carregado no Storage.
    :param file_key: Caminho do objeto dentro do bucket.

    :return: None
    :rtype: None
    """
    # Carregando credenciais
    pk_path = Variable.get('etl_extract_users_pk')
    credentials = service_account.Credentials.from_service_account_json(pk_path)

    file_key_name = (file_path if (file_key==None) else file_key)

    # Conectando ao Client do Storage e realizando o upload do arquivo
    try:
        client = storage.Client(credentials=credentials)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_key_name)
        blob.upload_from_filename(file_path)
    except Exception as e:
        raise AirflowException(f"[ERROR] Erro no upload do arquivo: {e}")

def _extract_data_api(url: str, ti: dict) -> str:
    """
    Funcao criada para extrair dados diretamente da API, salvar em um arquivo json na maquina local do Cloud Composer.
    E fazer upload no Cloud Storage do arquivo json com os dados da extracao.

    :param url: Url de requisicao da API. Essa informacao esta configurada nas variaveis (variables) do Cloud Composer.
    :param ti: Dicionario com os parametros referente a Task Instance (TI) das outras funcoes para disponibilizar variaveis de uma funcao para outra.

    :return: String com o nome do caminho do arquivo json dentro do Cloud Storage.
    :rtype: str 
    """
    import requests
    import json

    # Request da API
    params= 'results'
    qtd_requests = Variable.get('etl_extract_users_requests')

    response = requests.get(f'{url}/?{params}={qtd_requests}')
    data_json = response.json()

    # Recuperando a data e hora da execução
    date = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M")

    local_path = Variable.get('etl_extract_users_file_path')
    if not os.path.exists(local_path):
        os.makedirs(local_path)

    # Criando o arquivo json localmente
    filename_json = f'{local_path}{date}_random_users.json'

    with open(filename_json, 'w+') as f:
        json.dump(data_json, f, indent=4)
    
    # Fazendo upload do arquivo JSON para o bucket
    bucket_name= Variable.get('etl_extract_users_bucket')
    file_key = f"raw/{date}_random_users.json"
    
    upload_to_bucket(bucket_name=bucket_name, file_path=filename_json, file_key=file_key)
    bucket_file = f'{bucket_name}/{file_key}'

    ti.xcom_push(key='bucket-file-json', value=bucket_file)

def _transform_data(ti: dict) -> str:
    """
    Funcao criada para transformar o arquivo json em um csv preparando-o para ser escrito no BigQuery.

    :param ti: Dicionario com os parametros referente a Task Instance (TI) das outras funcoes para disponibilizar variaveis de uma funcao para outra.

    :return: String com o nome do caminho do arquivo csv dentro do Cloud Storage.
    :rtype: str 
    """
    import pandas as pd
    import json
    import time

    # Carregando credenciais
    pk_path = Variable.get('etl_extract_users_pk')
    credentials = service_account.Credentials.from_service_account_json(pk_path)

    # Buscando variaveis para upload do arquivo (objeto) no Storage
    bucket_name = Variable.get('etl_extract_users_bucket')
    json_object_key = ti.xcom_pull(key='bucket-file-json', task_ids='extract_data_random_user')
    bucket_key= json_object_key.replace(f"{bucket_name}/","")
    
    local_path = Variable.get('etl_extract_users_file_path')

    # Fazendo um download do arquivo json que esta no Storage
    try:
        client = storage.Client(credentials=credentials)
        bucket = client.get_bucket(bucket_name)
        blob= bucket.blob(bucket_key)

        local_filepath = f"{local_path}{bucket_key.split('/')[-1]}"
        try:
            blob.download_to_filename(local_filepath)
        except Exception as e:
            print(e)
    except Exception as e:
        raise AirflowException()

    time.sleep(30)

    file_json = open(local_filepath)
    json_data = json.load(file_json)
    file_json.close()

    # Convertendo em Pandas Dataframe para escrever em CSV
    df_data = pd.json_normalize(json_data, record_path='results')
    
    # Checando a existência do diretório local para armazenar o .csv
    local_path_csv = f'{local_path}stage'

    if not os.path.exists(local_path_csv):
        os.makedirs(local_path_csv)

    # Convertendo JSON para CSV
    csv_name = local_filepath.split("/")[-1]
    file_csv_name = f"{csv_name}.csv"
    full_path_csv = f"{local_path_csv}/{file_csv_name}"

    df_data.to_csv(full_path_csv, index=False)

    file_key = f'stage/{file_csv_name}'

    # Carregando CSV para o Storage
    upload_to_bucket(bucket_name=bucket_name, file_path=full_path_csv, file_key=file_key)

    bucket_file_csv = f"{bucket_name}/{file_key}"

    ti.xcom_push(key='bucket-file-csv', value=bucket_file_csv)

def _load_bigquery(tablename: str, ti:dict) -> None:
    """
    Funcao criada para carregar os dados do arquivo csv no BigQuery.

    :param tablename: Nome da tabela do BigQuery onde serao carregado os dados
    :param ti: Dicionario com os parametros referente a Task Instance (TI) das outras funcoes para disponibilizar variaveis de uma funcao para outra.

    :return: None
    :rtype: None
    """
    from google.cloud import bigquery

    # Carregando credenciais
    pk_path = Variable.get('etl_extract_users_pk')
    credentials = service_account.Credentials.from_service_account_json(pk_path)

    # Buscando variaveis referente ao BihQuery
    projectId = Variable.get("etl_extract_users_projectId")
    datasetId = Variable.get("etl_extract_users_datasetId")
    bucket_file_csv = ti.xcom_pull(key='bucket-file-csv', task_ids='transform_data')

    tableId = f'{projectId}.{datasetId}.{tablename}'

    client = bigquery.Client(credentials=credentials)

    schema=[
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("phone", "STRING"),
            bigquery.SchemaField("cell", "STRING"),
            bigquery.SchemaField("nat", "STRING"),
            bigquery.SchemaField("name_title", "STRING"),
            bigquery.SchemaField("name_first", "STRING"),
            bigquery.SchemaField("name_last", "STRING"),
            bigquery.SchemaField("location_street_number", "INTEGER"),
            bigquery.SchemaField("location_street_name", "STRING"),
            bigquery.SchemaField("location_city", "STRING"),
            bigquery.SchemaField("location_state", "STRING"),
            bigquery.SchemaField("location_country", "STRING"),
            bigquery.SchemaField("location_postcode", "STRING"),
            bigquery.SchemaField("location_coordinates_latitude", "STRING"),
            bigquery.SchemaField("location_coordinates_longitude", "STRING"),
            bigquery.SchemaField("location_timezone_offset", "STRING"),
            bigquery.SchemaField("location_timezone_description", "STRING"),
            bigquery.SchemaField("login_uuid", "STRING"),
            bigquery.SchemaField("login_username", "STRING"),
            bigquery.SchemaField("login_password", "STRING"),
            bigquery.SchemaField("login_salt", "STRING"), 
            bigquery.SchemaField("login_md5", "STRING"), 
            bigquery.SchemaField("login_sha1", "STRING"), 
            bigquery.SchemaField("login_sha256", "STRING"), 
            bigquery.SchemaField("dob_date", "STRING"), 
            bigquery.SchemaField("dob_age", "INTEGER"),
            bigquery.SchemaField("registered_date", "STRING"),
            bigquery.SchemaField("registered_age", "INTEGER"),
            bigquery.SchemaField("id_name", "STRING"),
            bigquery.SchemaField("id_value", "STRING"),
            bigquery.SchemaField("picture_large", "STRING"),
            bigquery.SchemaField("picture_medium", "STRING"),
            bigquery.SchemaField("picture_thumbnail", "STRING")
        ]

    # Verifica se tabela ja existe. Caso contrario, cria uma tabela no BigQuery.
    try:
        client.get_table(tableId)
    except:
        table = bigquery.Table(tableId, schema=schema)
        table = client.create_table(table) 

    # Carregando JOb Config para BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    # URI do arquivo CSV dentro do Storage
    uri_csv = f"gs://{bucket_file_csv}"

    job = client.load_table_from_uri(uri_csv, tableId, job_config=job_config)
    job.result()

    destination_table = client.get_table(tableId)
    logging.info(f"[SUCCESS] Carregado {destination_table.num_rows} linhas") 

with airflow.DAG('etl_extract_users', default_args=default_args, catchup=False) as dag:

    start = DummyOperator(
        task_id='start'
    )

    extract_data = PythonOperator(
        task_id='extract_data_random_user',
        python_callable=_extract_data_api,
        op_kwargs={
            "url": URL_API
            }
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=_transform_data,
    )

    load_data_bq = PythonOperator(
        task_id='load_data_bq',
        python_callable=_load_bigquery,
        op_kwargs={
            "tablename": "users"
        }
    )

    end = DummyOperator(
        task_id= 'end'
    )

    # Fluxo de Execução da DAG
    start >> extract_data >> transform_data >> load_data_bq >> end

