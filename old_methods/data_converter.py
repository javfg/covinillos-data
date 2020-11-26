#!/usr/bin/env python3.8

from datetime import datetime
import json
import sys

import pandas as pd


coord_cols = ["Lat", "Long"]
location_cols = ["Province/State", "Country/Region"]

covid_root_path = sys.argv[1]
covid_timeseries_path = f"{covid_root_path}/COVID-19/csse_covid_19_data/csse_covid_19_time_series/"


def fix_data(df, original_date_format):
    # remove coordinates columns
    df = df.drop(coord_cols, axis=1)

    # get columns with values
    data_cols = [x for x in df.columns if x not in location_cols]

    # fill zeros and convert to int in data columns
    df[df.columns[~df.columns.isin(location_cols)]] = df[df.columns[~df.columns.isin(location_cols)]].fillna(0).astype(int)

    # convert dates to ISO6801
    column_names = location_cols + [str(datetime.strptime(date, original_date_format).date()) for date in data_cols]
    df.columns = column_names

    return df


def aggregate_countries(df):
    return df.groupby("Country/Region").sum()


def sort_descending(df, col):
    return df.sort_values(by=col, ascending=False)


def fix_spanish_data():
    confirmed_total.loc['Spain']['2020-03-12'] = 3087
    deaths_total.loc['Spain']['2020-03-12'] = 84

    confirmed_total.loc['Spain']['2020-04-15'] = 177633
    deaths_total.loc['Spain']['2020-04-15'] = 18579

    confirmed_total.loc['Spain']['2020-04-16'] = 182816
    deaths_total.loc['Spain']['2020-04-16'] = 19130

    confirmed_total.loc['Spain']['2020-04-17'] = 188068
    deaths_total.loc['Spain']['2020-04-17'] = 19478
    recovered_total.loc['Spain']['2020-04-17'] = 72963

    recovered_total.loc['Spain']['2020-04-18'] = 74662

    confirmed_total.loc['Spain']['2020-04-19'] = 195944


confirmed_total = f"{covid_timeseries_path}time_series_covid19_confirmed_global.csv"
confirmed_total = aggregate_countries(fix_data(pd.read_csv(confirmed_total, sep=","), "%m/%d/%y"))
confirmed_total = sort_descending(confirmed_total, confirmed_total.columns[-1])

country_order = list(confirmed_total.index.values)

deaths_total = f"{covid_timeseries_path}time_series_covid19_deaths_global.csv"
deaths_total = aggregate_countries(fix_data(pd.read_csv(deaths_total, sep=","), "%m/%d/%y"))
deaths_total = deaths_total.loc[country_order]

recovered_total = f"{covid_timeseries_path}time_series_covid19_recovered_global.csv"
recovered_total = aggregate_countries(fix_data(pd.read_csv(recovered_total, sep=","), "%m/%d/%y"))
recovered_total = recovered_total.loc[country_order]

fix_spanish_data()

confirmed_daily = confirmed_total.diff(axis=1).fillna(confirmed_total).clip(lower=0).astype(int)
deaths_daily = deaths_total.diff(axis=1).fillna(deaths_total).clip(lower=0).astype(int)
recovered_daily = recovered_total.diff(axis=1).fillna(recovered_total).clip(lower=0).astype(int)


events = pd.read_csv(f"{covid_root_path}/covinillos-data/events.csv", skip_blank_lines=True)

dataset = {}

for country in country_order:
    dataset[country] = []

    for date in confirmed_total.columns:
        dataset[country].append({
            'date': date,
            'confirmed_total': int(confirmed_total.loc[country, date]),
            'deaths_total': int(deaths_total.loc[country, date]),
            'recovered_total': int(recovered_total.loc[country, date]),
            'confirmed_daily': int(confirmed_daily.loc[country, date]),
            'deaths_daily': int(deaths_daily.loc[country, date]),
            'recovered_daily': int(recovered_daily.loc[country, date]),
            'events': events.loc[(events.country == country) & (events.date == date)][['description', 'group', 'reference']].to_dict(orient="records")
        })


smallDataSet = {}

for (country, data) in dataset.items():
    smallDataSet[country] = []

    for (index, day) in enumerate(data):
        smallDataSet[country].append({
            'date': day['date'],
            'data': [val for (key, val) in day.items() if key not in ['date', 'events']]
        })

        if len(day['events']):
            smallDataSet[country][index]['events'] = day['events']

with open(f"{covid_root_path}/covinillos-data/data/dataset.json", "w") as data_file:
    json.dump(smallDataSet, data_file)


events_dict = events.to_dict(orient="records")

with open(f"{covid_root_path}/covinillos-data/data/events.json", "w") as events_file:
    json.dump(events_dict, events_file)

# Correct spanish data
