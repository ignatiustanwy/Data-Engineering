import pandas as pd
import json
from urllib.request import urlopen
import os
from datetime import datetime

url = 'https://raw.githubusercontent.com/Papagoat/brain-assessment/main/restaurant_data.json'

with urlopen(url) as response:
    source = response.read()

restaurants = json.loads(source)

#tested to check if there are outliers such that certain rating_texts are classified wrongly.
#eg. an aggregate rating of 4.1 has been assigned to Good instead of Very Good.

def threshold():
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

threshold()
