import collections
import json
import os

from util import load_config
from database import ECOUNTRY_ICOUNTRIES_MAP

# load config
cfg = load_config()

with open(cfg["source"]) as f:
    info_sources = json.load(f)

sources = collections.defaultdict(list)
for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items():
    if ecountry == 'all':
        continue
    for domain, meta in info_sources["domains"].items():
        if meta["region"] in icountries:
            for source in meta["sources"]:
                sources[ecountry].append(f'http://{source}')

# output the result as a JSON file
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(data_dir, exist_ok=True)
with open(os.path.join(data_dir, "sources.json"), "w") as f:
    json.dump(sources, f, ensure_ascii=False, indent=2)
