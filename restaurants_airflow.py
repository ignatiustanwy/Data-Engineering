import pandas as pd
import json
from urllib.request import urlopen
import os
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import DAG

def task_1_processing():

    #reading excel file into a dataframe
    
    #change to the path of the excel file containing the list of countries and their country codes
    country_file = '/Users/iggyten/Documents/GitHub/Data-Engineering/country-code.xlsx'
    
    country_code = pd.read_excel(country_file,'Sheet1')
    
    #create a hashmap of the dataframe
    country_dictionary = dict(zip(country_code['Country Code'],country_code['Country']))

    #print(country_dictionary)

    url = 'https://raw.githubusercontent.com/Papagoat/brain-assessment/main/restaurant_data.json'


    with urlopen(url) as response:
        source = response.read()

    restaurants = json.loads(source)


    # #to show one result for exploration
    # for results in restaurants:
    #     for restaurant in results['restaurants']:
    #         print(json.dumps(restaurant,indent=2))
    #         break
        
    #     break

    column_names = ['Restaurant Id', 'Restaurant Name', 'Country', 'City', 'User Rating Votes', 'User Aggregate Rating','Cuisines']
    restaurants_dataframe = pd.DataFrame(columns = column_names)

#assumption: res_id used to indicate restaurant id instead of id
#countries with different country id from the Country Code excel sheet are considered empty values and given "NA"

    for results in restaurants:
        for restaurant in results['restaurants']:
            restaurant = restaurant['restaurant']
            id = restaurant['R']['res_id']
            name = restaurant['name']
            country_id = restaurant['location']['country_id']
            try:
                country = country_dictionary[country_id]
#country id not in the list of countries in the excel sheet
            except KeyError:
                country = 'NA'
            city = restaurant['location']['city']
            votes = restaurant['user_rating']['votes']
            rating = float(restaurant['user_rating']['aggregate_rating'])
            cuisines = restaurant['cuisines']
        
            values = [id,name,country,city,votes,rating,cuisines]
            dic = {}
            for i in range(len(column_names)):
                dic[column_names[i]] = [values[i]]
        
            df = pd.DataFrame(dic)
            restaurants_dataframe = pd.concat([restaurants_dataframe, df])
        
    

    #checking number of entries
    print(len(restaurants_dataframe))

#export to csv
    if not os.path.exists('/tmp'):
        os.makedirs('/tmp',exist_ok=True)
    restaurants_dataframe.to_csv('/tmp/restaurants.csv',index=False)

with DAG(
    dag_id="task_1_etl",
    start_date=datetime(2023, 9, 8),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_process_data = PythonOperator(
         task_id = 'process_data',
         python_callable=task_1_processing
    )
    
    to_gcs = LocalFilesystemToGCSOperator(
        task_id = "to_gcs",
        gcp_conn_id = 'gcp_dataengineering',
        bucket = 'engineerproj',
        src = '/tmp/restaurants.csv',
        dst = 'restaurants'   
    )

    to_bq = GCSToBigQueryOperator(
        task_id= 'to_bq',
        gcp_conn_id = 'gcp_dataengineering',
        bucket = 'engineerproj',
        source_objects=['restaurants'],
        destination_project_dataset_table= 'data-engineering-398512.result_tables.restaurants',
        schema_fields=[
        {'name':'Restaurant Id','type':'STRING','mode':'NULLABLE'},
        {'name':'Restaurant Name','type':'STRING','mode':'NULLABLE'},
        {'name':'Country','type':'STRING','mode':'NULLABLE'},
        {'name':'City','type':'STRING','mode':'NULLABLE'},
        {'name':'User Rating Votes','type':'INTEGER','mode':'NULLABLE'},
        {'name':'User Aggregate Rating','type':'FLOAT','mode':'NULLABLE'},
        {'name':'Cuisines','type':'STRING','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1
    )
task_process_data >> to_gcs >> to_bq