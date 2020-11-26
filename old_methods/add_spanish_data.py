#!/usr/bin/env python3

from datetime import datetime, timedelta
import json
import sys

import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 10)

if len(sys.argv) != 5:
    print("Usage: add_spanish_data iso_date confirmed deaths recovered")
    exit(1)

input = sys.argv[1:]


print("[add spanish data] loading spanish timeseries")
data_spain_path = "./spain_timeseries.csv"
data_spain = pd.read_csv(data_spain_path)
prev_day = data_spain.iloc[[-1]]


print("[add spanish data] checking stuff")
prev_date = datetime.fromisoformat(prev_day.date.values[0])
prev_next_date = prev_date + timedelta(days=1)
next_date = datetime.fromisoformat(input[0])

if prev_next_date != next_date:
    print(f"Wrong day! ({prev_date.date()} and {next_date.date()} are not correlative)")
    exit(1)


print("[add spanish data] adding new row")
spain_population = 46.94

confirmed_total = int(input[1])
deaths_total = int(input[2])
recovered_total = int(input[3])
confirmed_daily = int(confirmed_total - prev_day.confirmed_total)
deaths_daily = int(deaths_total - prev_day.deaths_total)
recovered_daily = int(recovered_total - prev_day.recovered_total)
confirmed_pm_total = confirmed_total / spain_population
confirmed_pm_daily = confirmed_daily / spain_population
deaths_pm_total = deaths_total / spain_population
deaths_pm_daily = deaths_daily / spain_population

next_day = [
    next_date.date(),
    confirmed_total,
    deaths_total,
    recovered_total,
    confirmed_daily,
    deaths_daily,
    recovered_daily,
    round(confirmed_pm_total, 2),
    round(confirmed_pm_daily, 2),
    round(deaths_pm_total, 2),
    round(deaths_pm_daily, 2),
]

data_spain.loc[len(data_spain)] = next_day


print("[add spanish data] saving data")
data_spain.to_csv(data_spain_path, index=False)
