import json
import collections
import logging

from typing import List, Dict

from util import load_config
from pymongo import MongoClient, DESCENDING


class HandlingPages:
    def __init__(self, host: str, port: int, db_name: str, collection_name: str) -> None:
        self.client = MongoClient(host=host, port=port)
        self.db = self.client[db_name]
        self.collection = self.db.get_collection(name=collection_name)

    def upsert_pages(self, documents: List[dict]) -> None:
        """
        Add the information of pages to database.
        Update its information if the URL of the page already exists.
        """
        for document in documents:
            self.collection.update_one(
                {'page.url': document['url']},
                {'$set': {'page': document}},
                upsert=True
            )

    @staticmethod
    def _slice_pages(filtered_pages: List[dict], start: int, limit: int) -> List[dict]:
        """
        Given a list of filtered pages, extract its part based on given GET parameters.
        """
        if start < len(filtered_pages):
            return filtered_pages[start:start + limit]
        else:
            return []

    @staticmethod
    def _reshape_pages_to_class_pages_map(pages: List[dict]) -> Dict[str, List[dict]]:
        """
        Given a list of pages, classify the pages into class.
        """
        class_pages_map = collections.defaultdict(list)
        for page in pages:
            for page_class, has_class in page["classes"].items():
                if has_class and page_class != 'COVID-19関連':
                    class_pages_map[page_class].append(page)
        return class_pages_map

    @staticmethod
    def _reshape_pages_to_country_pages_map(pages: List[dict]) -> Dict[str, List[dict]]:
        """
        Given a list of pages, classify the pages into country.
        """
        country_pages_map = collections.defaultdict(list)
        for page in pages:
            page_country = page["country"]
            country_pages_map[page_country].append(page)
        return country_pages_map

    def get_filtered_pages(self, class_: str, country: str, start: int, limit: int) -> List[dict]:
        """
        Get and filter pages based on given GET parameters
        """
        filters = [
            {'page.classes.COVID-19関連': 1}
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
        if class_ and country:
            if class_ != 'all':
                filters.append({f'page.classes.{class_}': 1})
            countries = [country]
            if country == 'int':
                countries.append('eu')
            filters.append({'page.country': {'$in': countries}})
            result = self.collection.find(
                projection=projection,
                filter={'$and': filters},
                sort=[('page.orig.timestamp', DESCENDING)]
            )
            reshaped_pages = [doc['page'] for doc in result]
            sliced_pages = self._slice_pages(reshaped_pages, start, limit)
        elif class_:
            if class_ != 'all':
                filters.append({f'page.classes.{class_}': 1})
            result = self.collection.find(
                projection=projection,
                filter={'$and': filters},
                sort=[('page.orig.timestamp', DESCENDING)]
            )
            reshaped_pages = self._reshape_pages_to_country_pages_map([doc['page'] for doc in result])
            sliced_pages = {_class: self._slice_pages(_pages, start, limit)
                            for _class, _pages in reshaped_pages.items()}
        else:
            result = self.collection.find(
                projection=projection,
                filter={'$and': filters},
                sort=[('page.orig.timestamp', DESCENDING)]
            )
            reshaped_pages = {
                _class: self._reshape_pages_to_country_pages_map(_pages)
                for _class, _pages in self._reshape_pages_to_class_pages_map([doc['page'] for doc in result]).items()
            }
            sliced_pages = {
                _class: {
                    _country: self._slice_pages(_country_pages, start, limit)
                    for _country, _country_pages in _class_pages.items()
                }
                for _class, _class_pages in reshaped_pages.items()
            }
        return sliced_pages


def main():
    cfg = load_config()

    logger = logging.getLogger('Logging')
    logger.setLevel(20)
    fh = logging.FileHandler(cfg['database']['log_path'], mode='a')
    logger.addHandler(fh)
    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(levelname)s:%(message)s')
    fh.setFormatter(formatter)

    mongo = HandlingPages(host=cfg['database']['host'],
                          port=cfg['database']['port'],
                          db_name=cfg['database']['db_name'],
                          collection_name=cfg['database']['collection_name'])

    with open(cfg['database']["input_page_path"]) as f:
        json_pages = [json.loads(line.strip()) for line in f]
        mongo.upsert_pages(json_pages)

    docs = [doc for doc in mongo.collection.find()]
    logger.log(20, f'Number of pages: {len(docs)}')


if __name__ == '__main__':
    main()
