import json
import os
from typing import List

from util import COUNTRIES, TOPICS


class MetaDataHandler:

    def __init__(self):
        self.meta_data_dir = os.path.join(os.path.dirname(__file__), 'data')
        self.stats_path = os.path.join(self.meta_data_dir, 'stats.json')
        self.sources_path = os.path.join(self.meta_data_dir, 'sources.json')

    def get(self, lang: str):
        topics = self.get_topics(lang)
        countries = self.get_countries(lang)
        stats = self.get_stats()
        stats = stats['stats']  # dispose the value of `last_update`
        sources = self.get_sources()

        # Merge stats and sources into countries.
        country_code_index_map = {country['country']: i for i, country in enumerate(countries)}
        for country_code in stats:
            countries[country_code_index_map[country_code]]['stats'] = stats[country_code]
            countries[country_code_index_map[country_code]]['sources'] = sources[country_code]

        return {'topics': topics, 'countries': countries}

    @staticmethod
    def get_countries(lang: str) -> List[dict]:
        def reshape_country(country: dict) -> dict:
            return {
                'country': country['country'],
                'name': country['name'][lang],
                'language': country['language'],
                'representativeSiteUrl': country['representativeSiteUrl']
            }
        return list(map(reshape_country, COUNTRIES))

    @staticmethod
    def get_topics(lang: str) -> List[str]:
        def reshape_topic(topic: dict) -> str:
            return topic[lang]
        return list(map(reshape_topic, TOPICS))

    def get_stats(self):
        with open(self.stats_path) as f:
            return json.load(f)

    def get_sources(self):
        with open(self.sources_path) as f:
            return json.load(f)

    def set_stats(self, stats):
        os.makedirs(os.path.dirname(self.stats_path), exist_ok=True)
        with open(self.stats_path, 'w') as f:
            json.dump(stats, f, ensure_ascii=False)

    def set_sources(self, sources):
        os.makedirs(os.path.dirname(self.sources_path), exist_ok=True)
        with open(self.sources_path, 'w') as f:
            json.dump(sources, f, ensure_ascii=False)
