import json
import os


class MetaDataHandler:

    def __init__(self):
        self.meta_data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        self.stats_path = os.path.join(self.meta_data_dir, 'stats.json')
        self.sources_path = os.path.join(self.meta_data_dir, 'sources.json')

    def read_stats(self):
        with open(self.stats_path) as f:
            return json.load(f)

    def read_sources(self):
        with open(self.sources_path) as f:
            return json.load(f)

    def update_stats(self, stats):
        os.makedirs(os.path.dirname(self.stats_path), exist_ok=True)
        with open(self.stats_path, 'w') as f:
            json.dump(stats, f, ensure_ascii=False)

    def update_sources(self, sources):
        os.makedirs(os.path.dirname(self.sources_path), exist_ok=True)
        with open(self.sources_path, 'w') as f:
            json.dump(sources, f, ensure_ascii=False)
