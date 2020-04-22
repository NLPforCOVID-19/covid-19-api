import json
import logging

from typing import List, Dict

from util import load_config
from pymongo import MongoClient, DESCENDING

TOPIC_CLASSES_MAP = {
    "感染状況": ["感染状況"],
    "予防・緊急事態宣言": ["予防", "都市封鎖", "渡航制限・防疫", "イベント中止"],
    "症状・治療・検査など医療情報": ["検査", "治療"],
    "経済・福祉政策": ["経済への影響", "就労", "モノの不足"],
    "休校・オンライン授業": ["休校・オンライン授業"],
}


class HandlingPages:
    def __init__(self, host: str, port: int, db_name: str, collection_name: str) -> None:
        self.client = MongoClient(host=host, port=port)
        self.db = self.client[db_name]
        self.collection = self.db.get_collection(name=collection_name)

    def upsert_page(self, document: dict) -> None:
        """Add a page to the database. If the page has already been registered, update the page."""
        def convert_classe_flag_map_to_topics(class_flag_map: Dict[str, int]) -> List[str]:
            topics = []
            for topic, classes_about_topic in TOPIC_CLASSES_MAP.items():
                if any(class_flag_map[class_] for class_ in classes_about_topic):
                    topics.append(topic)
            return topics

        def reshape_snippets(snippets: Dict[str, List[str]]) -> Dict[str, str]:
            reshaped = {}
            for topic, classes_about_topic in TOPIC_CLASSES_MAP.items():
                snippets_about_topic = []
                for class_ in classes_about_topic:
                    snippets_about_topic += snippets.get(class_, [])
                if snippets_about_topic:
                    reshaped[topic] = snippets_about_topic[0]
            return reshaped

        is_about_covid_19 = 1 if document["classes"]["COVID-19関連"] else 0
        country = document["country"]
        orig = {
            "title": document["orig"]["title"],
            "timestamp": document["orig"]["timestamp"],
        }
        ja_translated = {
            "title": document["ja_translated"]["title"],
            "timestamp": document["ja_translated"]["timestamp"],
        }
        url = document["url"]
        topics = convert_classe_flag_map_to_topics(document["classes"])
        snippets = reshape_snippets(document["snippets"])
        is_useful = -1
        is_clear = -1
        is_about_false_rumor = -1
        document_ = {
            "country": country,
            "orig": orig,
            "ja_translated": ja_translated,
            "url": url,
            "topics": topics,
            "snippets": snippets,
            "is_about_COVID-19": is_about_covid_19,
            "is_useful": is_useful,
            "is_clear": is_clear,
            "is_about_false_rumor": is_about_false_rumor
        }
        self.collection.update_one(
            {"page.url": url},
            {"$set": {"page": document_}},
            upsert=True
        )

    @staticmethod
    def _reshape_page(page: dict) -> dict:
        page["topics"] = [{"name": topic, "snippet": page["snippets"].get(topic, "")} for topic in page["topics"]]
        del page["snippets"]
        return page

    @staticmethod
    def _slice_pages(filtered_pages: List[dict], start: int, limit: int) -> List[dict]:
        """Slice a list of filtered pages."""
        return filtered_pages[start:start + limit] if start < len(filtered_pages) else []

    @staticmethod
    def _reshape_pages_to_topic_pages_map(pages: List[dict]) -> Dict[str, List[dict]]:
        """Given a list of topics, reshape it into a dictionary where each key corresponds to a topic."""
        topic_pages_map = dict()
        for page in pages:
            for page_topic in page["topics"]:
                topic_pages_map.setdefault(page_topic, []).append(page)
        return topic_pages_map

    @staticmethod
    def _reshape_pages_to_country_pages_map(pages: List[dict]) -> Dict[str, List[dict]]:
        """Given a list of pages, reshape it into a dictionary where each key corresponds to a country."""
        country_pages_map = dict()
        for page in pages:
            page_country = page["country"]
            country_pages_map.setdefault(page_country, []).append(page)
        return country_pages_map

    def get_filtered_pages(self, topic: str, country: str, start: int, limit: int) -> List[dict]:
        """Fetch pages based on given GET parameters."""
        # set default filters
        filters = [{"page.is_about_COVID-19": 1}]
        projection = {"_id": 0}
        sort_ = [("page.orig.timestamp", DESCENDING)]

        # add filters based on the given parameters
        if topic and topic != "all":
            filters.append({"page.topics": topic})

        if country and country != "all":
            countries = [country]
            if country == "int":
                countries.append("eu")
            filters.append({"page.country": {"$in": countries}})

        # get documents
        result = self.collection.find(
            projection=projection,
            filter={"$and": filters},
            sort=sort_
        )
        pages = [doc["page"] for doc in result]

        # reshape the results
        if topic and country:
            reshaped_pages = [self._reshape_page(page) for page in self._slice_pages(pages, start, limit)]
        elif topic:
            reshaped_pages = {
                _country: [self._reshape_page(page) for page in self._slice_pages(_country_pages, start, limit)]
                for _country, _country_pages in self._reshape_pages_to_country_pages_map(pages).items()
            }
        else:
            reshaped_pages = {
                _topic: {
                    _country: [self._reshape_page(page) for page in self._slice_pages(_country_pages, start, limit)]
                    for _country, _country_pages in self._reshape_pages_to_country_pages_map(_pages).items()
                }
                for _topic, _pages in self._reshape_pages_to_topic_pages_map(pages).items()
            }
        return reshaped_pages


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
