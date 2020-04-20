import json
import collections
import logging

from typing import List, Dict

from util import load_config
from pymongo import MongoClient, DESCENDING

TOPIC_TO_CLASSES = {
    '感染状況': ['感染状況'],
    '予防・緊急事態宣言': ['予防', '都市封鎖', '渡航制限・防疫', 'イベント中止'],
    '症状・治療・検査など医療情報': ['検査', '治療'],
    '経済・福祉政策': ['経済への影響', '就労', 'モノの不足'],
    '休校・オンライン授業': ['休校・オンライン授業'],
}
ADD_CROWDSOURCING_KEYS = ['is_useful', 'is_clear', 'is_about_false_rumor']
ADD_TOPICS = ['芸能・スポーツ', 'その他']


class HandlingPages:
    def __init__(self, host: str, port: int, db_name: str, collection_name: str) -> None:
        self.client = MongoClient(host=host, port=port)
        self.db = self.client[db_name]
        self.collection = self.db.get_collection(name=collection_name)

    def upsert_pages(self, documents: List[dict]) -> None:
        """Add pages to the database. At the same time, update pages that have already been registered."""
        for document in documents:
            document['is_about_COVID-19'] = 1 if document['classes']['COVID-19関連'] else 0
            for add_crowdsourcing_key in ADD_CROWDSOURCING_KEYS:
                document[add_crowdsourcing_key] = -1

            document['topics'] = dict()
            for topic, classes in TOPIC_TO_CLASSES.items():
                document['topics'][topic] = 1 if any([document['classes'][class_] for class_ in classes]) else 0
            for add_topic in ADD_TOPICS:
                document['topics'][add_topic] = -1
            del document['classes']
            self.collection.update_one(
                {'page.url': document['url']},
                {'$set': {'page': document}},
                upsert=True
            )

    @staticmethod
    def _postprocess_pages(filtered_pages: List[dict], start: int, limit: int) -> List[dict]:
        """Slice a list of filtered pages."""
        def extract_first_snippet(page: dict) -> dict:
            snippets = {class_: sentences[:1] for class_, sentences in page["snippets"].items()}
            page["snippets"] = snippets
            return page

        if start < len(filtered_pages):
            sliced_pages = filtered_pages[start:start + limit]
            sliced_pages = [extract_first_snippet(page) for page in sliced_pages]
            return sliced_pages
        else:
            return []

    @staticmethod
    def _reshape_pages_to_topic_pages_map(pages: List[dict]) -> Dict[str, List[dict]]:
        """Given a list of topics, reshape it into a dictionary where each key corresponds to a topic."""
        topic_pages_map = collections.defaultdict(list)
        for page in pages:
            for page_topic, has_topic in page["topics"].items():
                if has_topic:
                    topic_pages_map[page_topic].append(page)
        return topic_pages_map

    @staticmethod
    def _reshape_pages_to_country_pages_map(pages: List[dict]) -> Dict[str, List[dict]]:
        """Given a list of pages, reshape it into a dictionary where each key corresponds to a country."""
        country_pages_map = collections.defaultdict(list)
        for page in pages:
            page_country = page["country"]
            country_pages_map[page_country].append(page)
        return country_pages_map

    def get_filtered_pages(self, topic: str, country: str, start: int, limit: int) -> List[dict]:
        """Fetch pages based on given GET parameters."""
        filters = [
            {'page.is_about_COVID-19': 1}
        ]
        projection = {
            '_id': 0,
            'page.rawsentences': 0,
            'page.domain': 0,
            'page.ja_translated.file': 0,
            'page.ja_translated.xml_file': 0,
            'page.ja_translated.xml_timestamp': 0,
            'page.orig.file': 0,
            'page.snippets.COVID-19関連': 0
        }
        sort_ = [
            ('page.orig.timestamp', DESCENDING)
        ]
        if topic and country:
            if topic != 'all':
                filters.append({f'page.topics.{topic}': 1})
            countries = [country]
            if country == 'int':
                countries.append('eu')
            filters.append({'page.country': {'$in': countries}})
            result = self.collection.find(
                projection=projection,
                filter={'$and': filters},
                sort=sort_
            )
            reshaped_pages = [doc['page'] for doc in result]
            post_processed_pages = self._postprocess_pages(reshaped_pages, start, limit)
        elif topic:
            if topic != 'all':
                filters.append({f'page.topics.{topic}': 1})
            result = self.collection.find(
                projection=projection,
                filter={'$and': filters},
                sort=sort_
            )
            reshaped_pages = self._reshape_pages_to_country_pages_map([doc['page'] for doc in result])
            post_processed_pages = {
                _topic: self._postprocess_pages(_pages, start, limit)
                for _topic, _pages in reshaped_pages.items()
            }
        else:
            result = self.collection.find(
                projection=projection,
                filter={'$and': filters},
                sort=sort_
            )
            reshaped_pages = {
                _topic: self._reshape_pages_to_country_pages_map(_pages)
                for _topic, _pages in self._reshape_pages_to_topic_pages_map([doc['page'] for doc in result]).items()
            }
            post_processed_pages = {
                _topic: {
                    _country: self._postprocess_pages(_country_pages, start, limit)
                    for _country, _country_pages in _class_pages.items()
                }
                for _topic, _class_pages in reshaped_pages.items()
            }
        return post_processed_pages


def main():
    cfg = load_config()

    logger = logging.getLogger('Logging')
    logger.setLevel(20)
    fh = logging.FileHandler(cfg['database']['log_path'], mode='a')
    logger.addHandler(fh)
    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(levelname)s:%(message)s')
    fh.setFormatter(formatter)

    mongo = HandlingPages(
        host=cfg['database']['host'],
        port=cfg['database']['port'],
        db_name=cfg['database']['db_name'],
        collection_name=cfg['database']['collection_name']
    )

    with open(cfg['database']["input_page_path"]) as f:
        json_pages = [json.loads(line.strip()) for line in f]
        mongo.upsert_pages(json_pages)

    docs = [doc for doc in mongo.collection.find()]
    logger.log(20, f'Number of pages: {len(docs)}')


if __name__ == '__main__':
    main()
