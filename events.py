import pandas as pd
import json
from urllib.request import urlopen
import os
from datetime import datetime

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
                    photo = restaurant['photos_url']
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

