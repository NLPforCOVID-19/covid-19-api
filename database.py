import copy
import glob
import itertools
import json
import logging
import os
from typing import List, Dict

from pymongo import MongoClient, DESCENDING

from util import load_config

COUNTRY_COUNTRIES_MAP = {
    "jp": ["jp"],
    "cn": ["cn"],
    "us": ["us"],
    "eu": ["eu"],
    "fr": ["fr"],
    "es": ["es"],
    "de": ["de"],
    "in": ["in"],
    "kr": ["kr"],
    "eur": ["eu", "fr", "es", "de"],
    "asia": ["in", "kr"],
}
COUNTRY_COUNTRIES_MAP["all"] = list(set(itertools.chain(*COUNTRY_COUNTRIES_MAP.values())))

TOPIC_TOPICS_MAP = {
    "感染状況": ["感染状況"],
    "予防・緊急事態宣言": ["予防・緊急事態宣言"],
    "症状・治療・検査など医療情報": ["症状・治療・検査など医療情報"],
    "経済・福祉政策": ["経済・福祉政策"],
    "休校・オンライン授業": ["休校・オンライン授業"],
    "その他": ["その他", "芸能・スポーツ"]
}
TOPIC_TOPICS_MAP["all"] = list(set(itertools.chain(*TOPIC_TOPICS_MAP.values())))

TOPIC_CLASSES_MAP = {
    "感染状況": ["感染状況"],
    "予防・緊急事態宣言": ["予防", "都市封鎖", "渡航制限・防疫", "イベント中止"],
    "症状・治療・検査など医療情報": ["検査", "治療"],
    "経済・福祉政策": ["経済への影響", "就労", "モノの不足"],
    "休校・オンライン授業": ["休校・オンライン授業"],
}

TAGS = ["is_about_COVID-19", "is_useful", "is_clear", "is_about_false_rumor"]


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

    def get_filtered_pages(self, topic: str, country: str, start: int, limit: int) -> List[dict]:
        """Fetch pages based on given GET parameters."""
        # set default filters
        base_filters = [
            # filter out pages that are not about COVID-19
            {"$or": [
                {"page.country": {"$ne": "jp"}},  # already filtered
                {"$and": [
                    {"page.country": "jp"},
                    {"page.is_about_COVID-19": 1}
                ]}
            ]},
            # filter out pages that have been manually checked and regarded as not useful ones
            {"$or": [
                {"page.is_checked": 0},
                {"page.is_useful": {"$ne": 0}},
                {"page.is_about_false_rumor": 1}
            ]},
        ]
        sort_ = [("page.orig.timestamp", DESCENDING)]

        last_crowd_sourcing_time = "2020-01-01T00:00:00.000000"
        for doc in self.collection.find(filter={"page.is_checked": 1}, sort=sort_).limit(1):
            last_crowd_sourcing_time = doc["page"]["orig"]["timestamp"]
        base_filters.append(
            # filter out pages that have not been manually checked due to the thinning process
            {"$or": [
                {"page.is_checked": 1},
                {"page.orig.timestamp": {"$gt": last_crowd_sourcing_time}}
            ]}
        )

        if topic and country:
            topic_filters = [{"page.topics": {"$in": TOPIC_TOPICS_MAP.get(topic, [])}}]
            country_filters = [{"page.country": {"$in": COUNTRY_COUNTRIES_MAP.get(country, [])}}]
            filter_ = {"$and": base_filters + topic_filters + country_filters}
            cur = self.collection.find(filter=filter_, sort=sort_)
            reshaped_pages = [self._reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
        elif topic:
            reshaped_pages = {}
            topic_filters = [{"page.topics": {"$in": TOPIC_TOPICS_MAP.get(topic, [])}}]
            for country, countries in COUNTRY_COUNTRIES_MAP.items():
                if country == 'all':
                    continue
                country_filters = [{"page.country": {"$in": countries}}]
                filter_ = {"$and": base_filters + topic_filters + country_filters}
                cur = self.collection.find(filter=filter_, sort=sort_)
                reshaped_pages[country] = [self._reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
        else:
            reshaped_pages = {}
            for topic, topics in TOPIC_TOPICS_MAP.items():
                if topic == 'all':
                    continue
                topic_filters = [{"page.topics": {"$in": topics}}]
                reshaped_pages[topic] = {}
                for country, countries in COUNTRY_COUNTRIES_MAP.items():
                    if country == 'all':
                        continue
                    country_filters = [{"page.country": {"$in": countries}}]
                    filter_ = {"$and": base_filters + topic_filters + country_filters}
                    cur = self.collection.find(filter=filter_, sort=sort_)
                    reshaped_pages[topic][country] = \
                        [self._reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
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
        for input_path in sorted(glob.glob(f'{cfg["crowdsourcing"]["result_dir"]}/20*.jsonl')):
            file_name = os.path.splitext(os.path.basename(input_path))[0]
            crowd_sourcing_date = file_name.split('_')[0]
            crowd_sourcing_timestamp = \
                f"{crowd_sourcing_date[:4]}-{crowd_sourcing_date[4:6]}-{crowd_sourcing_date[6:]}T00:00:00.000000"

            with open(input_path, 'r') as f:
                json_tags = [json.loads(line.strip()) for line in f]
            for json_tag in json_tags:
                search_result = mongo.collection.find_one({"page.url": json_tag["url"]})
                if search_result:
                    page = search_result["page"]
                    existing_timestamp = page["orig"]["timestamp"]
                    if crowd_sourcing_timestamp > existing_timestamp:
                        page["is_checked"] = 1
                        for tag in TAGS:
                            page[tag] = json_tag["tags"][tag]

                        new_topics = [topic for topic, has_topic in json_tag["tags"]["topics"].items() if has_topic]
                        page["topics"] = new_topics

                        old_snippets = page["snippets"]
                        new_snippets = {}
                        for new_topic in new_topics:
                            new_snippets[new_topic] = \
                                old_snippets[new_topic] if new_topic in old_snippets.keys() else ""

                        mongo.collection.update_one(
                            {"page.url": json_tag["url"]},
                            {"$set": {"page": page}}
                        )


if __name__ == "__main__":
    main()
