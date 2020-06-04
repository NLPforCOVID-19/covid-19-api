import collections
import json
import os

from util import load_config
from database import COUNTRY_COUNTRIES_MAP

# load config
cfg = load_config()

with open(cfg["source"]) as f:
    info_sources = json.load(f)

here = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(here, "data")

sources = collections.defaultdict(list)
for display_country, actual_countries in COUNTRY_COUNTRIES_MAP.items():
    if display_country == 'all':
        continue
    for domain, meta in info_sources["domains"].items():
        if meta["region"] in actual_countries:
            sources[display_country].append(domain)

# output the result as a JSON file
sources_path = os.path.join(data_dir, "sources.json")
with open(sources_path, "w") as f:
    json.dump(sources, f, ensure_ascii=False, indent=2)
