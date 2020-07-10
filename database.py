import copy
import itertools
import json
import logging
from datetime import datetime
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
    "int": ["int"],
    "eur": ["eu", "fr", "es", "de"],
    "asia": ["in", "kr"],
}
COUNTRY_COUNTRIES_MAP["all"] = list(set(itertools.chain(*COUNTRY_COUNTRIES_MAP.values())))

TOPIC_TOPICS_MAP = {
    "感染状況": ["感染状況"],
    "予防・防疫・緩和": ["予防・防疫・緩和", "予防・緊急事態宣言"],
    "症状・治療・検査など医療情報": ["症状・治療・検査など医療情報"],
    "経済・福祉政策": ["経済・福祉政策"],
    "教育関連": ["教育関連", "休校・オンライン授業"],
    "その他": ["その他", "芸能・スポーツ"]
}
TOPICS_TOPIC_MAP = {
    internal_topic: display_topic
    for display_topic, internal_topics in TOPIC_TOPICS_MAP.items()
    for internal_topic in internal_topics
}
TOPIC_TOPICS_MAP["all"] = list(set(itertools.chain(*TOPIC_TOPICS_MAP.values())))

TAGS = ["is_about_COVID-19", "is_useful", "is_clear", "is_about_false_rumor"]


class DBHandler:
    def __init__(self, host: str, port: int, db_name: str, collection_name: str, useful_white_list: List) -> None:
        self.client = MongoClient(host=host, port=port)
        self.db = self.client[db_name]
        self.collection = self.db.get_collection(name=collection_name)
        self.useful_white_list = useful_white_list

    def upsert_page(self, document: dict) -> None:
        """Add a page to the database. If the page has already been registered, update the page."""

        def _extract_general_snippet(snippets: Dict[str, List[str]]) -> str:
            for topics in TOPIC_TOPICS_MAP.values():
                for topic in topics:
                    for snippet in snippets.get(topic, []):
                        return snippet
            return ''

        def _reshape_snippets(snippets: Dict[str, List[str]]) -> Dict[str, str]:
            reshaped = {}
            general_snippet = _extract_general_snippet(snippets)
            for rep_topic, topics in TOPIC_TOPICS_MAP.items():
                snippets_about_topic = []
                for topic in topics:
                    snippets_about_topic += snippets.get(topic, [])
                if snippets_about_topic:
                    reshaped[rep_topic] = snippets_about_topic[0].strip()
                elif general_snippet:
                    reshaped[rep_topic] = general_snippet
            return reshaped

        is_about_covid_19 = document["classes"]["is_about_COVID-19"]
        country = document["country"]
        orig = {
            "title": document["orig"]["title"].strip(),
            "timestamp": document["orig"]["timestamp"],
        }
        if document["ja_translated"]["title"]:
            ja_translated = {
                "title": document["ja_translated"]["title"].strip(),
                "timestamp": document["ja_translated"]["timestamp"],
            }
        else:
            return None
        url = document["url"]

        topics = [label for label in document["labels"] if label != "is_about_COVID-19"]
        snippets = _reshape_snippets(document["snippets"])
        is_checked = 0
        is_useful = document["classes"]["is_useful"]
        is_clear = document["classes"]["is_clear"]
        is_about_false_rumor = document["classes"]["is_about_false_rumor"]

        domain = document.get("domain", "")
        domain_label = document.get("domain_label", "")
        document_ = {
            "country": country,
            "displayed_country": country,
            "orig": orig,
            "ja_translated": ja_translated,
            "url": url,
            "topics": topics,
            "snippets": snippets,
            "is_checked": is_checked,
            "is_about_COVID-19": is_about_covid_19,
            "is_useful": is_useful,
            "is_clear": is_clear,
            "is_about_false_rumor": is_about_false_rumor,
            "domain": domain,
            "domain_label": domain_label
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
        page["topics"] = [
            {
                "name": TOPICS_TOPIC_MAP.get(topic, topic),
                "snippet": page["snippets"].get(TOPICS_TOPIC_MAP.get(topic, topic), "")
            }
            for topic in page["topics"]
        ]
        del page["snippets"]
        return page

    def classes(self, topic: str, country: str, start: int, limit: int) -> List[dict]:
        base_filters = self.get_base_filters()
        sort_ = self.get_sort_metrics()
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

    def countries(self, country: str, topic: str, start: int, limit: int) -> List[dict]:
        base_filters = self.get_base_filters()
        sort_ = self.get_sort_metrics()
        if country and topic:
            country_filters = [{"page.country": {"$in": COUNTRY_COUNTRIES_MAP.get(country, [])}}]
            topic_filters = [{"page.topics": {"$in": TOPIC_TOPICS_MAP.get(topic, [])}}]
            filter_ = {"$and": base_filters + country_filters + topic_filters}
            cur = self.collection.find(filter=filter_, sort=sort_)
            reshaped_pages = [self._reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
        elif country:
            reshaped_pages = {}
            country_filters = [{"page.country": {"$in": COUNTRY_COUNTRIES_MAP.get(country, [])}}]
            for topic, topics in TOPIC_TOPICS_MAP.items():
                if topic == 'all':
                    continue
                topic_filters = [{"page.topics": {"$in": topics}}]
                filter_ = {"$and": base_filters + topic_filters + country_filters}
                cur = self.collection.find(filter=filter_, sort=sort_)
                reshaped_pages[topic] = [self._reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
        else:
            reshaped_pages = {}
            for country, countries in COUNTRY_COUNTRIES_MAP.items():
                if country == 'all':
                    continue
                country_filters = [{"page.country": {"$in": countries}}]
                reshaped_pages[country] = {}
                for topic, topics in TOPIC_TOPICS_MAP.items():
                    if topic == 'all':
                        continue
                    topic_filters = [{"page.topics": {"$in": topics}}]
                    filter_ = {"$and": base_filters + topic_filters + country_filters}
                    cur = self.collection.find(filter=filter_, sort=sort_)
                    reshaped_pages[country][topic] = \
                        [self._reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
        return reshaped_pages

    @staticmethod
    def get_base_filters():
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
        return base_filters

    @staticmethod
    def get_sort_metrics():
        return [("page.orig.timestamp", DESCENDING)]

    def update_page(self, url, is_about_covid_19, is_useful, new_country, new_topics, notes, category_check_log_path):
        self.collection.update_one(
            {"page.url": url},
            {"$set": {
                "page.is_about_COVID-19": 1 if is_about_covid_19 else 0,
                "page.is_useful": 1 if is_useful else 0,
                "page.is_checked": 1,
                "page.displayed_country": new_country,
                "page.topics": new_topics
            }
            },
            upsert=True
        )
        updated = {
            "url": url,
            "is_about_COVID-19": 1 if is_about_covid_19 else 0,
            "is_useful": 1 if is_useful else 0,
            "new_country": new_country,
            "new_topics": new_topics,
            "notes": notes,
            "time": datetime.now().isoformat()
        }
        with open(category_check_log_path, mode='a') as f:
            json.dump(updated, f, ensure_ascii=False)
            f.write('\n')
        return updated


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
    # if os.path.isdir(cfg["crowdsourcing"]["result_dir"]):
    #     for input_path in sorted(glob.glob(f'{cfg["crowdsourcing"]["result_dir"]}/20*.jsonl')):
    #         file_name = os.path.splitext(os.path.basename(input_path))[0]
    #         crowd_sourcing_date = file_name.split('_')[0]
    #         crowd_sourcing_timestamp = \
    #             f"{crowd_sourcing_date[:4]}-{crowd_sourcing_date[4:6]}-{crowd_sourcing_date[6:]}T00:00:00.000000"
    #
    #         with open(input_path, 'r') as f:
    #             json_tags = [json.loads(line.strip()) for line in f]
    #         for json_tag in json_tags:
    #             search_result = mongo.collection.find_one({"page.url": json_tag["url"]})
    #             if search_result:
    #                 page = search_result["page"]
    #                 existing_timestamp = page["orig"]["timestamp"]
    #                 if crowd_sourcing_timestamp > existing_timestamp:
    #                     page["is_checked"] = 1
    #                     for tag in TAGS:
    #                         page[tag] = json_tag["tags"][tag]
    #
    #                     new_topics = [topic for topic, has_topic in json_tag["tags"]["topics"].items() if has_topic]
    #                     page["topics"] = new_topics
    #
    #                     old_snippets = page["snippets"]
    #                     new_snippets = {}
    #                     for new_topic in new_topics:
    #                         new_snippets[new_topic] = old_snippets.get(new_topic, "")
    #
    #                     mongo.collection.update_one(
    #                         {"page.url": json_tag["url"]},
    #                         {"$set": {"page": page}}
    #                     )
    # add category-checked pages
    with open(cfg["database"]["category_check_log_path"], mode='r') as f:
        for line in f:
            if line.strip():
                category_checked_page = json.loads(line.strip())
                existing_page = mongo.collection.find_one({"page.url": category_checked_page['url']})
                if existing_page:
                    mongo.collection.update_one(
                        {"page.url": category_checked_page['url']},
                        {"$set": {
                            "page.is_about_COVID-19": category_checked_page["is_about_COVID-19"],
                            "page.is_useful": category_checked_page["is_useful"],
                            "page.is_checked": 1,
                            "page.displayed_country": category_checked_page["new_country"],
                            "page.topics": category_checked_page["new_topics"]
                        }
                        },
                    )


if __name__ == "__main__":
    main()
