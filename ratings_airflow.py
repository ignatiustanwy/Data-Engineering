import pandas as pd
import json
from urllib.request import urlopen
import os
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import DAG


def threshold():
    url = 'https://raw.githubusercontent.com/Papagoat/brain-assessment/main/restaurant_data.json'

    with urlopen(url) as response:
        source = response.read()

    restaurants = json.loads(source)

#tested to check if there are outliers such that certain rating_texts are classified wrongly.
#eg. an aggregate rating of 4.1 has been assigned to Good instead of Very Good.

    #rating_texts = {"Excellent": 0,
    #       "Very Good": 0,
    #       "Good": 0,
    #       "Average": 0,
    #       "Poor": 0}

    rating_texts = {"Excellent": float('inf'),
            "Very Good": float('inf'),
            "Good": float('inf'),
            "Average": float('inf'),
            "Poor": float('inf')}

    for results in restaurants:
        for restaurant in results['restaurants']:
            restaurant = restaurant['restaurant']
            rating_text = restaurant['user_rating']['rating_text']
            aggregate_rating = float(restaurant['user_rating']['aggregate_rating'])
            if rating_text in rating_texts:
                #rating_texts[rating_text] = max(rating_texts[rating_text], aggregate_rating)
                rating_texts[rating_text] = min(rating_texts[rating_text], aggregate_rating)

    for rating, benchmark in rating_texts.items():
        print(f'{rating}: {benchmark}')
    
    column_names = ['Rating Text','Rating Threshold']
    ratings_dataframe = pd.DataFrame(rating_texts.items(),columns = column_names)

    if not os.path.exists('/tmp'):
        os.makedirs('/tmp',exist_ok=True)
    ratings_dataframe.to_csv('/tmp/ratings_threshold.csv',index=False) 

with DAG(
    dag_id="task_3_etl",
    start_date=datetime(2023, 9, 8),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_process_data = PythonOperator(
         task_id = 'process_data',
         python_callable=threshold
    )
    
    to_gcs = LocalFilesystemToGCSOperator(
        task_id = "to_gcs",
        gcp_conn_id = 'gcp_dataengineering',
        bucket = 'engineerproj',
        src = '/tmp/ratings_threshold.csv',
        dst = 'ratings_threshold'
    )

    to_bq = GCSToBigQueryOperator(
        task_id= 'to_bq',
        gcp_conn_id = 'gcp_dataengineering',
        bucket = 'engineerproj',
        source_objects=['ratings_threshold'],
        destination_project_dataset_table= 'data-engineering-398512.result_tables.ratings_threshold',
        schema_fields=[
        {'name':'Rating Text','type':'STRING','mode':'NULLABLE'},
        {'name':'Aggregate Rating Threshold','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True
    )
task_process_data >> to_gcs >> to_bq