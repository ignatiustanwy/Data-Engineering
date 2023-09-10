import pandas as pd
import json
from urllib.request import urlopen
import os
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import DAG

def task_2_processing():

    url = 'https://raw.githubusercontent.com/Papagoat/brain-assessment/main/restaurant_data.json'

    with urlopen(url) as response:
        source = response.read()

    restaurants = json.loads(source)

    column_names = ['Event Id', 'Restaurant Id', 'Restaurant Name', 'Photo URL', 'Event Title', 'Event Start Date','Event End Date']
    events_dataframe = pd.DataFrame(columns = column_names)

    #Assumptions: 
    # 1)events that are in the month of April 2019 consists of events 
    # that start prior to April 2019, eg. Start Date 2019, End Date 2019 is counted as an 
    # event in April 2019
    # 2) As Event Id is collected, if the restaurant has more than 1 event in April 2019, 
    # both event Ids are collected, resulting in repeated restaurant id, and name
    # 3) I used photos attached to the events instead of using photos of the restaurant itself,
    # as there are restaurants with more than 1 events in the month of April 2019.

    apr_1st = datetime(2019,4,1) 

    for results in restaurants:
        for restaurant in results['restaurants']:
            restaurant = restaurant['restaurant']
        #checking if there is any events  
            try:
                events = restaurant['zomato_events'] 
                    
            except:
                continue

            else:
                for event in events:
                    start_date = event['event']['start_date']
                    end_date = event['event']['end_date']
                    start_date_obj = datetime.strptime(start_date,"%Y-%m-%d")
                    end_date_obj = datetime.strptime(end_date,"%Y-%m-%d")
        #checking if theres an event in the month of April 2019
                    if start_date_obj <= apr_1st <= end_date_obj or (start_date_obj.month == 4 and start_date_obj.year == 2019):
                        event_id = event['event']['event_id']
                        res_id = restaurant['R']['res_id']
                        name = restaurant['name']
                        #photo = restaurant['photos_url']
                        try:
                            photo = event['event']['photos'][0]['photo']['url'] 
                        except:
                            photo = 'NA'
                        title = event['event']['title']
                        values = [event_id,res_id,name,photo,title,start_date,end_date]
                        dic = {}
                        for i in range(len(column_names)):
                            dic[column_names[i]] = [values[i]]
            
                        df = pd.DataFrame(dic)
                        events_dataframe = pd.concat([events_dataframe, df])
                    else:
                        continue   

    if not os.path.exists('/tmp'):
        os.makedirs('/tmp',exist_ok=True)
    events_dataframe.to_csv('/tmp/restaurant_events.csv',index=False)  

with DAG(
    dag_id="task_2_etl",
    start_date=datetime(2023, 9, 8),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_process_data = PythonOperator(
         task_id = 'process_data',
         python_callable=task_2_processing
    )
    
    to_gcs = LocalFilesystemToGCSOperator(
        task_id = "to_gcs",
        gcp_conn_id = 'gcp_dataengineering',
        bucket = 'engineerproj',
        src = '/tmp/restaurant_events.csv',
        dst = 'restaurant_events'
    )

    to_bq = GCSToBigQueryOperator(
        task_id= 'to_bq',
        gcp_conn_id = 'gcp_dataengineering',
        bucket = 'engineerproj',
        source_objects=['restaurant_events'],
        destination_project_dataset_table= 'data-engineering-398512.result_tables.restaurant_events',
        schema_fields=[
        {'name':'Event Id','type':'STRING','mode':'NULLABLE'},
        {'name':'Restaurant Id','type':'STRING','mode':'NULLABLE'},
        {'name':'Restaurant Name','type':'STRING','mode':'NULLABLE'},
        {'name':'Photo URL','type':'STRING','mode':'NULLABLE'},
        {'name':'Event Title','type':'STRING','mode':'NULLABLE'},
        {'name':'Event Start Date','type':'DATE','mode':'NULLABLE'},
        {'name':'Event End Date','type':'DATE','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True
    )
task_process_data >> to_gcs >> to_bq