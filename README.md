# README:

## Prerequisites/Services used:
Apache airflow 

Google Cloud Platform

Google BigQuery


## Steps to run the code
1) Initialising a virtual environment
```bash
virtualenv env

source env/bin/activate
```
2) Initialize apache airflow using local terminal, using the following commands:
```bash
airflow db init

airflow users create \
    --username admin \
    --firstname <firstname> \
    --lastname <lastname> \
    --role Admin \
    --email <email>

airflow webserver --port 8080 -D 
OR 
airflow webserver 

airflow scheduler
```

3) Go to [airflow dashboard](http://127.0.0.1:8080/) and log in using your username and password.

4) Under the home page, scroll under Admin > Connections to create a new Google Cloud connection, which is required to access to Google Cloud Storage and BigQuery.
[![image.png](https://i.postimg.cc/BvsvGwnF/image.png)](https://postimg.cc/pmGvfC8V)


5) Create a new google cloud connection with the following fields:

```text
Connection Id: gcp_dataengineering

Connection type: Google Cloud

Project id: data-engineering-398512

Keyfile JSON: [attached in email]
```
[![image.png](https://i.postimg.cc/cJ96MBd8/image.png)](https://postimg.cc/SX9ym82y)

6) Using the virtual environment, run the DAGs using the following commands:
```bash
airflow dags test task_1_etl 2023-9-9
airflow dags test task_2_etl 2023-9-9
airflow dags test task_3_etl 2023-9-9
```
## Additional Notes
For both **restaurants_airflow.py** and **restaurants.py**
Variable **country_file**: the file path should be changed to the path where the Country Code excel file (given) is stored in

[![image.png](https://i.postimg.cc/CLBCZdHN/image.png)](https://postimg.cc/JG8HvrFD)

## Assumptions / Considerations

1) Zomato being a platform that focuses on providing up-to-date information relating to restaurants, new data may be updated to Zomato on a regular basis. Thus, I chose to leverage on Apache Airflow, which will run DAGs to extract data from Zomato on a daily basis, and load the data into BigQuery for storage and easier querying for future use.

2) I chose not to store the excel sheet for country codes into BigQuery as a table on its own in my solution as I would have to query the country code for every restaurant to get its respective country, which would be expensive in the long run.

3) I chose not to remove restaurants with dummy values as dummy values may indicate incomplete/not updated information rather than false information.

**Task 1 Assumptions**:
1) “Res_id” was used instead of “id” for restaurant id.

2) Countries with different country ids not in the Country Code excel sheet are considered empty values and given “NA”

**Task 2 Assumptions**:
1) Definition of events in the month of April 2019: Events that start in the month of April 2019 and events that start prior to April 2019 that end on April 2019 or later. (eg. An Event with Start Date in March 2019 and End Date in May 2019 is considered an event in April 2019)

2) As Event Id is the primary key, if the restaurant has more than 1 event in April 2019, all events satisfying the condition are collected, resulting in repeated restaurant ids and names.
URLs stored in “Photo URL” column are photos of the events instead of photos for the restaurant.

## Cloud Services Deployment Idea:
As Zomato is a platform that provides up-to-date information relating to restaurants, I would expect data to be updated on a regular basis. Thus, in my solution, I leveraged on an orchestration tool like Apache Airflow to automate the ETL tasks on a daily basis, so that we will obtain the most up-to-date information for more accurate decision-making using downstream applications. Upon extraction, I then transformed the data locally, to be saved and stored as a csv file as per the requirements. To build upon my solution to draw insights of the data in the future, I uploaded my csv files using airflow operators to Google Cloud Storage, so that I can easily transfer the data to BigQuery. 

This can also be achieved fully on cloud by using an orchestration tool like Cloud Composer which is built on Apache Airflow, and Dataflow to handle the ETL processes all on the cloud, which will then pass the desired data directly to Google Cloud Storage to store and manage our refined data files. This approach will be much more efficient and scalable in the case where the dataset gets bigger.

As BigQuery is capable of handling big datasets and data can be easily queried at a fast pace from the platform, the files that were uploaded onto Google Cloud Storage are then loaded into BigQuery, where we create tables to structure the data. This ensures that the data is easily accessible and queried for more analysis.

With our data in BigQuery, we can then use that data to draw insights, via the use of downstream applications such as Vertex AI for machine learning on the data, and/or Tableau to visualize our data using interactive dashboards and reports. 

As Vertex AI, Google Cloud Storage, BigQuery, Cloud Composer, and Dataflow are all part of the Google Cloud ecosystem, it is easy to integrate them with one another to create an entire pipeline for end-to-end data processing. Although Tableau is not a Google Cloud-native service like the other services, Tableau has an in-built BigQuery connector for easy access of the data in BigQuery.

## Architecture Diagram
[![Simple-Architecture-Diagram-drawio.png](https://i.postimg.cc/VLxhvxMJ/Simple-Architecture-Diagram-drawio.png)](https://postimg.cc/qzGwZZ1r)

## Schema of BigQuery Tables Created

**Task 1**: List of Restaurants
[![image.png](https://i.postimg.cc/QN54P6vs/image.png)](https://postimg.cc/RqSLJTMb)

**Task 2** List of restaurants with events in the month of April 2019
[![image.png](https://i.postimg.cc/qv7GkQDN/image.png)](https://postimg.cc/RNjtd1Bm)

**Task 3** Threshold of the different rating text based on aggregate rating
[![image.png](https://i.postimg.cc/3JtgbmrY/image.png)](https://postimg.cc/9rqRD4qn)
