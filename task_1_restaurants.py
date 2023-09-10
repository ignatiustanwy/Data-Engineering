import pandas as pd
import json
from urllib.request import urlopen
import os


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


#to show one result for exploration
for results in restaurants:
    for restaurant in results['restaurants']:
        print(json.dumps(restaurant,indent=2))
        break
        
    break

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
