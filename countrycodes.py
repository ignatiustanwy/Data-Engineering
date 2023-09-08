import pandas as pd

#reading excel file into a dataframe
country_file = 'country-code.xlsx'
country_code = pd.read_excel(country_file,'Sheet1')

#create a hashmap of the dataframe
country_dictionary = dict(zip(country_code['Country Code'],country_code['Country']))

print(country_dictionary)