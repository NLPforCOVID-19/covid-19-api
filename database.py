import json
import collections
import logging

from typing import List, Dict

from util import load_config
from pymongo import MongoClient, DESCENDING

TOPIC_TO_CLASSES = {
    "感染状況": ["感染状況"],
    "予防・緊急事態宣言": ["予防", "都市封鎖", "渡航制限・防疫", "イベント中止"],
    "症状・治療・検査など医療情報": ["検査", "治療"],
    "経済・福祉政策": ["経済への影響", "就労", "モノの不足"],
    "休校・オンライン授業": ["休校・オンライン授業"],
}
CROWDSOURCING_KEYS = ["is_useful", "is_clear", "is_about_false_rumor"]


class HandlingPages:
    def __init__(self, host: str, port: int, db_name: str, collection_name: str) -> None:
        self.client = MongoClient(host=host, port=port)
        self.db = self.client[db_name]
        self.collection = self.db.get_collection(name=collection_name)

    def upsert_page(self, document: dict) -> None:
        """Add a page to the database. If the page has already been registered, update the page."""
        document["is_about_COVID-19"] = 1 if document["classes"]["COVID-19関連"] else 0

        for crowdsourcing_key in CROWDSOURCING_KEYS:
            document[crowdsourcing_key] = -1

        document["topics"] = []
        snippets = {}
        for topic, classes in TOPIC_TO_CLASSES.items():
            has_topic = False
            snippet_list = []
            for class_ in classes:
                if document["classes"][class_]:
                    has_topic = True
                    snippet_list += document["snippets"][class_]
            if has_topic:
                document["topics"].append(topic)
                snippet_list = [snippet for snippet in snippet_list if snippet]
                snippets[topic] = snippet_list[0] if snippet_list else ''
        document["snippets"] = snippets

        del document["classes"]

        self.collection.update_one(
            {"page.url": document["url"]},
            {"$set": {"page": document}},
            upsert=True
        )

    @staticmethod
    def _slice_pages(filtered_pages: List[dict], start: int, limit: int) -> List[dict]:
        """Slice a list of filtered pages."""
        if start < len(filtered_pages):
            sliced_pages = filtered_pages[start:start + limit]
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
            {"page.is_about_COVID-19": 1}
        ]
        projection = {
            "_id": 0,
            "page.rawsentences": 0,
            "page.domain": 0,
            "page.ja_translated.file": 0,
            "page.ja_translated.xml_file": 0,
            "page.ja_translated.xml_timestamp": 0,
            "page.orig.file": 0,
        }
        sort_ = [
            ("page.orig.timestamp", DESCENDING)
        ]
        if topic and country:
            if topic != "all":
                filters.append({"page.topics": {"$all": [topic]}})
            countries = [country]
            if country == "int":
                countries.append("eu")
            filters.append({"page.country": {"$in": countries}})
            result = self.collection.find(
                projection=projection,
                filter={"$and": filters},
                sort=sort_
            )
            reshaped_pages = [doc["page"] for doc in result]
            sliced_pages = self._slice_pages(reshaped_pages, start, limit)
        elif topic:
            if topic != "all":
                filters.append({"page.topics": {"$all": [topic]}})
            result = self.collection.find(
                projection=projection,
                filter={"$and": filters},
                sort=sort_
            )
            reshaped_pages = self._reshape_pages_to_country_pages_map([doc["page"] for doc in result])
            sliced_pages = {
                _topic: self._slice_pages(_pages, start, limit)
                for _topic, _pages in reshaped_pages.items()
            }
        else:
            result = self.collection.find(
                projection=projection,
                filter={"$and": filters},
                sort=sort_
            )
            reshaped_pages = {
                _topic: self._reshape_pages_to_country_pages_map(_pages)
                for _topic, _pages in self._reshape_pages_to_topic_pages_map([doc["page"] for doc in result]).items()
            }
            sliced_pages = {
                _topic: {
                    _country: self._slice_pages(_country_pages, start, limit)
                    for _country, _country_pages in _class_pages.items()
                }
                for _topic, _class_pages in reshaped_pages.items()
            }
        return sliced_pages


def main():
    cfg = load_config()

    logger = logging.getLogger("Logging")
    logger.setLevel(20)
    fh = logging.FileHandler(cfg["database"]["log_path"], mode="a")
    logger.addHandler(fh)
    formatter = logging.Formatter("%(asctime)s:%(lineno)d:%(levelname)s:%(message)s")
    fh.setFormatter(formatter)

    mongo = HandlingPages(
        host=cfg["database"]["host"],
        port=cfg["database"]["port"],
        db_name=cfg["database"]["db_name"],
        collection_name=cfg["database"]["collection_name"]
    )

    with open(cfg["database"]["input_page_path"]) as f:
        for line in f:
            mongo.upsert_page(json.loads(line.strip()))

    num_docs = sum(1 for _ in mongo.collection.find())
    logger.log(20, f"Number of pages: {num_docs}")


if __name__ == "__main__":
    main()
