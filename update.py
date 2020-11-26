#!/usr/bin/env python3

from math import isnan
from datetime import datetime
import json

import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 10)

data_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
events_url = "https://raw.githubusercontent.com/javfg/covinillos-data/master/events.csv"


################################################################################
print("[update data] fetch data")

data = pd.read_csv(data_url).fillna(0)
events = pd.read_csv(events_url)


################################################################################
print("[update data] prepare events")

events['events'] = events.apply(lambda row: {
    'description': row['description'],
    'group': row['group'],
    'reference': row['reference'],
}, axis=1)

events_merged = events.drop(['description', 'group', 'reference'], axis=1)

################################################################################
print("[update data] prepare data")

data_columns_to_pick = ['total_cases', 'total_deaths', 'new_cases', 'new_deaths', 'total_cases_per_million', 'new_cases_per_million', 'total_deaths_per_million', 'new_deaths_per_million']

for column in data_columns_to_pick:
    data[column] = data[column].astype(int)

data_new = pd.DataFrame({
    'country': data['location'],
    'date': data['date'],
    'data': data[data_columns_to_pick].values.tolist()
})

merged_df = data_new.merge(events_merged.groupby(['country', 'date'])['events'].apply(list), how='outer', left_on=['country', 'date'], right_on=['country', 'date'])

data_dict = merged_df.groupby('country').apply(lambda x: x.drop('country', axis=1).to_dict(orient='records')).to_dict()

for (country, country_data) in data_dict.items():
    for day in country_data:
        try:
            if isnan(day['events']):
                del(day['events'])
        except:
            pass

################################################################################
print("[update data] save data")
with open(f"./data/dataset.json", "w") as data_file:
    json.dump(data_dict, data_file)

with open(f"./data/events.json", "w") as events_file:
    json.dump(events.to_dict(orient="records"), events_file)
