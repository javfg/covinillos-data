#!/usr/bin/env python3

from datetime import datetime
import json

import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 10)

data_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
data_spain_path = "./spain_timeseries.csv"
recovered_jhu_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"
events_url = "https://raw.githubusercontent.com/javfg/covinillos-data/master/events.csv"


print("[data converter] getting data")
data = pd.read_csv(data_url).fillna(0)
data_spain = pd.read_csv(data_spain_path)
recovered_data = pd.read_csv(recovered_jhu_url)
events = pd.read_csv(events_url)


print("[data converter] fixing jhu recovered data")
# fill N/As, remove geo coordinates columns, aggregate by country
recovered_total = recovered_data.fillna(0).drop(['Lat', 'Long'], axis=1).groupby("Country/Region").sum()

# fix date format
recovered_total.columns = [str(datetime.strptime(x, "%m/%d/%y").date()) for x in list(recovered_total.columns)]

# fix column names and some country names
recovered_total.index.names = ["location"]
recovered_total = recovered_total.rename(index={'Country/Region': 'location', 'US': 'United States'})

# calculate daily cases
recovered_daily = recovered_total.diff(axis=1).fillna(recovered_total).clip(lower=0).astype(int)


# get or zero function for JHU data: returns value for a country/date or 0 if non-existant
def goz(df, country, date):
    result = 0
    try:
        result = df.loc[country][date]
    except KeyError:
        pass

    return result


print("[data converter] preparing data")

# get_events function, gets events for a country/date
def get_events(country, date):
    return events.loc[(events.country == country) & (events.date == date)][['description', 'group', 'reference']].to_dict(orient="records")


dataset = {}
countries = list(data['location'].unique())
countries.remove('Spain')


#TODO: make this the pandas way
for country in countries:
    dataset[country] = []

    for date in list(data.loc[data['location'] == country]['date'].unique()):
        day_data = data.loc[(data['location'] == country) & (data['date'] == date)]

        dataset[country].append({
            'date': date,
            'confirmed_total': int(day_data['total_cases']),
            'deaths_total': int(day_data['total_deaths']),
            'recovered_total': int(goz(recovered_total, country, date)),
            'confirmed_daily': int(day_data['new_cases']),
            'deaths_daily': int(day_data['new_deaths']),
            'recovered_daily': int(goz(recovered_daily, country, date)),
            'confirmed_pm_total': int(day_data['total_cases_per_million']),
            'confirmed_pm_daily': int(day_data['new_cases_per_million']),
            'deaths_pm_total': int(day_data['total_deaths_per_million']),
            'deaths_pm_daily': int(day_data['new_deaths_per_million']),
            'events': get_events(country, date)
        })

# spain data
print("[data converter] adding spain data")
def add_day(day):
    day_dict = day.to_dict()
    day_dict['events'] = get_events('Spain', day_dict['date'])
    spain.append(day_dict)

spain = []
data_spain.apply(add_day, axis=1);
dataset['Spain'] = spain


print("[data converter] reducing size")
small_dataset = {}

for (country, data) in dataset.items():
    small_dataset[country] = []

    for (index, day) in enumerate(data):
        small_dataset[country].append({
            'date': day['date'],
            'data': [val for (key, val) in day.items() if key not in ['date', 'events']]
        })

        if len(day['events']):
            small_dataset[country][index]['events'] = day['events']


print("[data converter] saving data")
with open(f"./data/dataset.json", "w") as data_file:
    json.dump(small_dataset, data_file)

with open(f"./data/events.json", "w") as events_file:
    json.dump(events.to_dict(orient="records"), events_file)

print("[data converter] DONE!")
