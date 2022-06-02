import datetime

import airflow
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator, BigQueryCreateEmptyTableOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

# Variaveis
PROJECT_ID = Variable.get('load_silver_projectId')
DATASET_NAME_SRC = Variable.get('load_silver_dataset_source')
DATASET_NAME_TGT = Variable.get('load_silver_dataset_target')
TABLE_NAME = Variable.get('load_silver_tablename')

START_DATE = datetime.datetime(2022, 6, 1)

default_args = {
    'owner': 'Robton R Brangaitis',
    'start_date': START_DATE,
    'schedule_interval': '@daily'
}

with airflow.DAG('load_silver_random_users', default_args=default_args, catchup=False) as dag:

    verify_table_source = BigQueryCheckOperator(
            task_id='verify_source_table',
            sql='''
            #standardSQL
            SELECT
            table_id
            FROM
            `{}.{}.__TABLES_SUMMARY__`
            WHERE
            table_id = "{}"
            '''.format(PROJECT_ID, DATASET_NAME_SRC, TABLE_NAME),
            use_legacy_sql=False,
            dag=dag
        )

    verify_table_target = BigQueryCheckOperator(
            task_id='verify_table_target',
            sql='''
            #standardSQL
            SELECT
            table_id
            FROM
            `{}.{}.__TABLES_SUMMARY__`
            WHERE
            table_id = "{}"
            '''.format(PROJECT_ID, DATASET_NAME_TGT, TABLE_NAME),
            use_legacy_sql=False,
            dag=dag
        )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME_TGT,
        table_id=TABLE_NAME,
        schema_fields=[
            {"name": "gender", "type": "STRING", "mode": "REQUIRED"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cell", "type": "STRING", "mode": "NULLABLE"},
            {"name": "name_first", "type": "STRING", "mode": "REQUIRED"},
            {"name": "name_last", "type": "STRING", "mode": "REQUIRED"},
            {"name": "location_street_number", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "location_street_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "location_city", "type": "STRING", "mode": "REQUIRED"},
            {"name": "location_country", "type": "STRING", "mode": "REQUIRED"},
            {"name": "location_postcode", "type": "STRING", "mode": "REQUIRED"},
            {"name": "login_username", "type": "STRING", "mode": "NULLABLE"},
            {"name": "login_password", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dob_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "age", "type": "INTEGER", "mode": "REQUIRED"},
        ],
        exists_ok=True
    )
    
    write_table_silver = BigQueryOperator(
            task_id='write_table_silver',    
            sql="""
            #standardSQL
            SELECT
                gender,
                email,
                phone,
                cell,
                name_first,
                name_last,
                location_street_number,
                location_street_name,
                location_city,
                location_state,
                location_country,
                location_postcode,
                login_username,
                login_password,
                dob_date,
                dob_age as age
            FROM
                `{}.{}`
            """.format(DATASET_NAME_SRC, TABLE_NAME),

            destination_dataset_table='{}.{}.{}'.format(
                PROJECT_ID, DATASET_NAME_TGT, TABLE_NAME
            ),
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=True,
            use_legacy_sql=False,
            dag=dag
        )

    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed_or_skipped'
    )

    verify_table_source >> create_table >> verify_table_target >> write_table_silver >> end


    
