import os
import json
import logging
import copy

from typing import List, Dict

from util import load_config
from pymongo import MongoClient, DESCENDING

COUNTRY_REGION_MAP = {
    "eu": "eur",
    "fr": "eur",
    "es": "eur",
    "de": "eur",
    "in": "asia",
    "kr": "asia"
}
REGION_COUNTRIES_MAP = {
    region: [country for country, region_ in COUNTRY_REGION_MAP.items() if region_ == region]
    for region in COUNTRY_REGION_MAP.values()
}
TOPIC_CLASSES_MAP = {
    "感染状況": ["感染状況"],
    "予防・緊急事態宣言": ["予防", "都市封鎖", "渡航制限・防疫", "イベント中止"],
    "症状・治療・検査など医療情報": ["検査", "治療"],
    "経済・福祉政策": ["経済への影響", "就労", "モノの不足"],
    "休校・オンライン授業": ["休校・オンライン授業"],
}
TAGS = ["is_about_COVID-19", "is_useful", "is_clear", "is_about_false_rumor"]
MAX_USEFUL_PAGES = 10


class DBHandler:
    def __init__(self, host: str, port: int, db_name: str, collection_name: str, useful_white_list: List) -> None:
        self.client = MongoClient(host=host, port=port)
        self.db = self.client[db_name]
        self.collection = self.db.get_collection(name=collection_name)
        self.useful_white_list = useful_white_list

    def upsert_page(self, document: dict) -> None:
        """Add a page to the database. If the page has already been registered, update the page."""
        def convert_class_flag_map_to_topics(class_flag_map: Dict[str, int]) -> List[str]:
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
                    reshaped[topic] = snippets_about_topic[0].strip()
            return reshaped

        is_about_covid_19 = 1 if document["classes"]["COVID-19関連"] else 0
        country = document["country"]
        orig = {
            "title": document["orig"]["title"].strip(),
            "timestamp": document["orig"]["timestamp"],
        }
        ja_translated = {
            "title": document["ja_translated"]["title"].strip(),
            "timestamp": document["ja_translated"]["timestamp"],
        }
        url = document["url"]
        topics = convert_class_flag_map_to_topics(document["classes"])
        snippets = reshape_snippets(document["snippets"])
        is_checked = 0
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
            "is_checked": is_checked,
            "is_about_COVID-19": is_about_covid_19,
            "is_useful": is_useful,
            "is_clear": is_clear,
            "is_about_false_rumor": is_about_false_rumor
        }

        existing_page = self.collection.find_one({"page.url": url})
        if existing_page and orig["timestamp"] > existing_page["page"]["orig"]["timestamp"]:
            self.collection.update_one(
                {"page.url": url},
                {"$set": {"page": document_}},
                upsert=True
            )
        elif not existing_page:
            self.collection.insert_one({"page": document_})

    @staticmethod
    def _reshape_page(page: dict) -> dict:
        copied_page = copy.deepcopy(page)
        copied_page["topics"] = [
            {"name": topic, "snippet": copied_page["snippets"].get(topic, "")}
            for topic in copied_page["topics"]
        ]
        del copied_page["snippets"]
        return copied_page

    def _postprocess_pages(self, filtered_pages: List[dict], start: int, limit: int) -> List[dict]:
        """Prioritize useful pages and slice a list of filtered pages."""
        useful_whitelist_pages, useful_pages, other_pages = [], [], []
        if start < len(filtered_pages):
            for i, filtered_page in enumerate(filtered_pages):
                if len(useful_pages) == MAX_USEFUL_PAGES:
                    other_pages.extend(filtered_pages[i:])
                    break
                elif filtered_page['is_useful'] == 2:
                    if filtered_page['url'] in self.useful_white_list:
                        useful_whitelist_pages.append(filtered_page)
                    else:
                        useful_pages.append(filtered_page)
                else:
                    other_pages.append(filtered_page)
            postprocessed_pages = useful_whitelist_pages + useful_pages + other_pages
            return postprocessed_pages[start:start+limit]
        else:
            return []

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
        filters = [
            # filter out pages that are not about COVID-19
            {"page.is_about_COVID-19": 1},
            # filter out pages that have been manually checked and regarded as not useful ones
            {"$or": [
                {"page.is_checked": {"$ne": 1}},
                {"page.is_useful": {"$ne": 0}},
                {"page.is_about_false_rumor": {"$ne": 0}}
            ]},
        ]
        projection = {"_id": 0}
        sort_ = [("page.orig.timestamp", DESCENDING)]

        preliminary_result = self.collection.find(
            projection=projection,
            filter={"page.is_checked": 1},
            sort=sort_
        )
        last_crowd_sourcing_time = "2020-01-01T00:00:00.000000"
        for doc in preliminary_result:
            last_crowd_sourcing_time = doc["page"]["orig"]["timestamp"]
            break
        filters.append(
            {"$or": [
                {"page.is_checked": {"$ne": 0}},
                {"page.orig.timestamp": {"$gt": last_crowd_sourcing_time}}
            ]}
        )

        # add filters based on the given parameters
        if topic and topic != "all":
            topics = [topic]
            if topic == "その他":
                topics.append("芸能・スポーツ")
            filters.append({"page.topics": {"$in": topics}})

        if country and country != "all":
            countries = [country]
            countries.extend(REGION_COUNTRIES_MAP.get(country, []))
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
            reshaped_pages = [self._reshape_page(page) for page in self._postprocess_pages(pages, start, limit)]
        elif topic:
            reshaped_pages = {
                COUNTRY_REGION_MAP.get(_country, _country):
                    [self._reshape_page(page) for page in self._postprocess_pages(_country_pages, start, limit)]
                for _country, _country_pages in self._reshape_pages_to_country_pages_map(pages).items()
            }
        else:
            reshaped_pages = {
                _topic: {
                    COUNTRY_REGION_MAP.get(_country, _country):
                        [self._reshape_page(page) for page in self._postprocess_pages(_country_pages, start, limit)]
                    for _country, _country_pages in self._reshape_pages_to_country_pages_map(_topic_pages).items()
                }
                for _topic, _topic_pages in self._reshape_pages_to_topic_pages_map(pages).items()
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

    with open(cfg["crowdsourcing"]["useful_white_list"], mode='r') as f:
        useful_white_list = [line.strip() for line in f.readlines()]
    mongo = DBHandler(
        host=cfg["database"]["host"],
        port=cfg["database"]["port"],
        db_name=cfg["database"]["db_name"],
        collection_name=cfg["database"]["collection_name"],
        useful_white_list=useful_white_list
    )

    # add pages to the database or update pages
    with open(cfg["database"]["input_page_path"]) as f:
        for line in f:
            mongo.upsert_page(json.loads(line.strip()))
    num_docs = sum(1 for _ in mongo.collection.find())
    logger.log(20, f"Number of pages: {num_docs}")

    # reflect the crowdsourcing results
    if os.path.isdir(cfg["crowdsourcing"]["result_dir"]):
        with open(f'{cfg["crowdsourcing"]["result_dir"]}/crowdsourcing_all.jsonl') as f:
            json_tags = [json.loads(line.strip()) for line in f]

        for json_tag in json_tags:
            search_result = mongo.collection.find_one({"page.url": json_tag["url"]})
            if search_result:
                page = search_result["page"]
                page["is_checked"] = 1
                for tag in TAGS:
                    page[tag] = json_tag["tags"][tag]

                new_topics = [topic for topic, has_topic in json_tag["tags"]["topics"].items() if has_topic]
                page["topics"] = new_topics

                old_snippets = page["snippets"]
                new_snippets = {}
                for new_topic in new_topics:
                    new_snippets[new_topic] = old_snippets[new_topic] if new_topic in old_snippets.keys() else ""

                mongo.collection.update_one(
                    {"page.url": json_tag["url"]},
                    {"$set": {"page": page}}
                )


if __name__ == "__main__":
    main()
