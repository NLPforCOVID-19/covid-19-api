import json
import os
import shutil
import tempfile
import urllib.parse
import urllib.request

import pandas as pd


# fetch data from https://github.com/CSSEGISandData/COVID-19
def fetch_data(url):
    with urllib.request.urlopen(url) as response:
        with tempfile.NamedTemporaryFile() as tmp_file:
            shutil.copyfileobj(response, tmp_file)
            return pd.read_csv(tmp_file.name)


BASE = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
death_url = urllib.parse.urljoin(BASE, "time_series_covid19_deaths_global.csv")
confirmation_url = urllib.parse.urljoin(BASE, "time_series_covid19_confirmed_global.csv")

death_df = fetch_data(death_url)
confirmation_df = fetch_data(confirmation_url)

# load target countries
here = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(here, "data")
metadata_path = os.path.join(data_dir, "meta.json")
with open(metadata_path) as f:
    countries = json.load(f)["countries"]


# extract statistics
def get_last_update(df):
    return df.columns[-1]


def get_statistics(df, accessors):
    if "all" in accessors:
        tmp_df = df.copy()
    else:
        tmp_df = df[df["Country/Region"].isin(accessors)]

    total = int(tmp_df.iloc[:, -1].sum())
    today = total - int(tmp_df.iloc[:, -2].sum())
    return total, today


last_update = get_last_update(death_df)

stats = {}
for country in countries:
    death_total, death_today = get_statistics(death_df, country["dataRepository"])
    confirmation_total, confirmation_today = get_statistics(confirmation_df, country["dataRepository"])
    stats[country["country"]] = {
        "death_total": death_total,
        "confirmation_total": confirmation_total,
        "death_today": death_today,
        "confirmation_today": confirmation_today
    }

# concatenate all the extracted information
result = {
    "last_update": last_update,
    "stats": stats
}

# output the result as a JSON file
stats_path = os.path.join(data_dir, "stats.json")
with open(stats_path, "w") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
