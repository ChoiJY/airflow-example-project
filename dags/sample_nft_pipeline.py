import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pandas import json_normalize

default_args = {
    'start_date': datetime(2022, 1, 1),
}


def _process_nft(ti):
    assets = ti.xcom_pull(task_ids=['extract_nft'])
    if not len(assets):
        raise ValueError('Asset is not exists')
    ntf = assets[0]['assets'][0]

    processed_nft = json_normalize({
        'name': ntf['name'],
        'token_id': ntf['token_id'],
        'image_url': ntf['image_url'],
    })

    PostgresOperator(
        task_id='store_nft',
        sql=f'''
            INSERT IGNORE INTO nfts values (processed_nft['name'], processed_nft['token_id'], processed_nft['image_url']);
            '''
    )

    processed_nft.to_csv('/tmp/processed_nft.csv', index=None, header=False)


with DAG(dag_id='ntf-pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         tags=['nft', 'test'],
         catchup=False) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='default_postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS ntfs (
                name TEXT PRIMARY KEY,
                token_id TEXT NOT NULL,
                image_url TEXT NOT NULL
            )
        '''
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='opensea_api',
        endpoint='api/v1/assets?collection=doodles-official&limit=1'
    )

    extract_nft = SimpleHttpOperator(
        task_id='extract_nft',
        http_conn_id='opensea_api',
        endpoint='api/v1/assets?collection=doodles-official&limit=1',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_nft = PythonOperator(
        task_id='process_nft',
        python_callable=_process_nft
    )

    create_table >> is_api_available >> extract_nft >> process_nft
