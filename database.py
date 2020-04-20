import json
import collections
import logging

from typing import List, Dict

from util import load_config
from pymongo import MongoClient, DESCENDING

TAGS = {
    'COVID-19関連': -1,
    'usefulness': -1,
    'clarity': -1,
    'topic': {
        '感染状況': -1,
        '予防・緊急事態宣言': -1,
        '症状・治療・検査など医療情報': -1,
        '経済・福祉政策': -1,
        '休校・オンライン授業': -1,
        '芸能・スポーツ': -1,
        'デマに関する記事': -1,
        'その他': -1
    }
}


class HandlingPages:
    def __init__(self, host: str, port: int, db_name: str, collection_name: str) -> None:
        self.client = MongoClient(host=host, port=port)
        self.db = self.client[db_name]
        self.collection = self.db.get_collection(name=collection_name)

    def upsert_pages(self, documents: List[dict]) -> None:
        """Add pages to the database. At the same time, update pages that have already been registered."""
        for document in documents:
            document['tags'] = TAGS
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
    def _reshape_pages_to_class_pages_map(pages: List[dict]) -> Dict[str, List[dict]]:
        """Given a list of pages, reshape it into a dictionary where each key corresponds to a class."""
        class_pages_map = collections.defaultdict(list)
        for page in pages:
            for page_class, has_class in page["classes"].items():
                if has_class and page_class != 'COVID-19関連':
                    class_pages_map[page_class].append(page)
        return class_pages_map

    @staticmethod
    def _reshape_pages_to_country_pages_map(pages: List[dict]) -> Dict[str, List[dict]]:
        """Given a list of pages, reshape it into a dictionary where each key corresponds to a country."""
        country_pages_map = collections.defaultdict(list)
        for page in pages:
            page_country = page["country"]
            country_pages_map[page_country].append(page)
        return country_pages_map

    def get_filtered_pages(self, class_: str, country: str, start: int, limit: int) -> List[dict]:
        """Fetch pages based on given GET parameters."""
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
        sort_ = [
            ('page.orig.timestamp', DESCENDING)
        ]
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
                sort=sort_
            )
            reshaped_pages = [doc['page'] for doc in result]
            post_processed_pages = self._postprocess_pages(reshaped_pages, start, limit)
        elif class_:
            if class_ != 'all':
                filters.append({f'page.classes.{class_}': 1})
            result = self.collection.find(
                projection=projection,
                filter={'$and': filters},
                sort=sort_
            )
            reshaped_pages = self._reshape_pages_to_country_pages_map([doc['page'] for doc in result])
            post_processed_pages = {
                _class: self._postprocess_pages(_pages, start, limit)
                for _class, _pages in reshaped_pages.items()
            }
        else:
            result = self.collection.find(
                projection=projection,
                filter={'$and': filters},
                sort=sort_
            )
            reshaped_pages = {
                _class: self._reshape_pages_to_country_pages_map(_pages)
                for _class, _pages in self._reshape_pages_to_class_pages_map([doc['page'] for doc in result]).items()
            }
            post_processed_pages = {
                _class: {
                    _country: self._postprocess_pages(_country_pages, start, limit)
                    for _country, _country_pages in _class_pages.items()
                }
                for _class, _class_pages in reshaped_pages.items()
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
