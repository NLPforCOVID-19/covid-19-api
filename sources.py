import collections
import json
import os

from util import load_config
from database import ECOUNTRY_ICOUNTRIES_MAP

# load config
cfg = load_config()

with open(cfg["source"]) as f:
    info_sources = json.load(f)

here = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(here, "data")

sources = collections.defaultdict(list)
for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items():
    if ecountry == 'all':
        continue
    for domain, meta in info_sources["domains"].items():
        if domain == 'hazard.yahoo.co.jp':
            domain = 'hazard.yahoo.co.jp/article/20200207'
        if meta["region"] in icountries:
            sources[ecountry].append(f'http://{domain}')

# output the result as a JSON file
sources_path = os.path.join(data_dir, "sources.json")
with open(sources_path, "w") as f:
    json.dump(sources, f, ensure_ascii=False, indent=2)
