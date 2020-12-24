import os
import json


def load_config():
    here = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(here, 'config.json')
    with open(config_path, encoding='utf-8') as f:
        return json.load(f)
