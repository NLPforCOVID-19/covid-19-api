import itertools
import json
import logging
from datetime import datetime
from typing import List, Dict

from pymongo import MongoClient, DESCENDING

from util import load_config

ECOUNTRY_ICOUNTRIES_MAP = {
    "jp": ["jp"],
    "cn": ["cn"],
    "us": ["us"],
    "eur": ["eur", "eu", "fr", "es", "de"],
    "asia": ["asia", "kr", "in"],
    "sa": ["sa", "br"],
    "int": ["int"]
}
ICOUNTRY_ECOUNTRY_MAP = {
    icountry: ecountry
    for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items()
    for icountry in icountries
}
ECOUNTRIES = list(ECOUNTRY_ICOUNTRIES_MAP.keys())
ICOUNTRIES = list(itertools.chain(*ECOUNTRY_ICOUNTRIES_MAP.values()))
ECOUNTRY_ICOUNTRIES_MAP["all"] = ICOUNTRIES

ETOPIC_ITOPICS_MAP = {
    "感染状況": ["感染状況"],
    "予防・防疫・緩和": ["予防・緊急事態宣言"],
    "症状・治療・検査など医療情報": ["症状・治療・検査など医療情報"],
    "経済・福祉政策": ["経済・福祉政策"],
    "教育関連": ["休校・オンライン授業"],
    "その他": ["その他", "芸能・スポーツ"]
}
ITOPIC_ETOPIC_MAP = {
    itopic: etopic
    for etopic, itopics in ETOPIC_ITOPICS_MAP.items()
    for itopic in itopics
}
ETOPICS = list(ETOPIC_ITOPICS_MAP.keys())
ITOPICS = list(itertools.chain(*ETOPIC_ITOPICS_MAP.values()))
ETOPIC_ITOPICS_MAP["all"] = ITOPICS


class DBHandler:

    def __init__(self, host: str, port: int, db_name: str, collection_name: str):
        self.client = MongoClient(host=host, port=port)
        self.db = self.client[db_name]
        self.collection = self.db.get_collection(name=collection_name)

    def upsert_page(self, document: dict) -> None:
        """Add a page to the database. If the page has already been registered, update the page."""

        def extract_general_snippet(snippets: Dict[str, List[str]]) -> str:
            for itopic in ITOPICS:
                for snippet in snippets.get(itopic, []):
                    return snippet.strip()
            return ""

        def reshape_snippets(snippets: Dict[str, List[str]]) -> Dict[str, str]:
            reshaped = {}
            general_snippet = extract_general_snippet(snippets)
            for itopic in ITOPICS:
                snippets_about_topic = snippets.get(itopic, [])
                if snippets_about_topic:
                    reshaped[itopic] = snippets_about_topic[0].strip()
                elif general_snippet:
                    reshaped[itopic] = general_snippet
                else:
                    reshaped[itopic] = ""
            return reshaped

        is_about_covid_19: int = document["classes"]["is_about_COVID-19"]
        country: str = document["country"]
        orig = {
            "title": document["orig"]["title"].strip(),  # type: str
            "timestamp": document["orig"]["timestamp"],  # type: str
        }
        if not document["ja_translated"]["title"]:
            return
        ja_translated = {
            "title": document["ja_translated"]["title"].strip(),  # type: str
            "timestamp": document["ja_translated"]["timestamp"],  # type: str
        }
        url: str = document["url"]
        topics: List[str] = list(filter(lambda label: label in ITOPICS, document["labels"]))
        snippets = reshape_snippets(document["snippets"])

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
    def reshape_page(page: dict) -> dict:
        page["topics"] = [
            {
                "name": ITOPIC_ETOPIC_MAP[itopic],
                "snippet": page["snippets"][itopic]
            }
            for itopic in page["topics"]
        ]
        del page["snippets"]
        return page

    def classes(self, etopic: str, ecountry: str, start: int, limit: int) -> List[dict]:
        base_filters = self.get_base_filters()
        sort_ = self.get_sort_metrics()
        if etopic and ecountry:
            topic_filters = [{"page.topics": {"$in": ETOPIC_ITOPICS_MAP.get(etopic, [])}}]
            country_filters = [{"page.country": {"$in": ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])}}]
            filter_ = {"$and": base_filters + topic_filters + country_filters}
            cur = self.collection.find(filter=filter_, sort=sort_)
            reshaped_pages = [self.reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
        elif etopic:
            reshaped_pages = {}
            topic_filters = [{"page.topics": {"$in": ETOPIC_ITOPICS_MAP.get(etopic, [])}}]
            for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items():
                if ecountry == 'all':
                    continue
                country_filters = [{"page.country": {"$in": icountries}}]
                filter_ = {"$and": base_filters + topic_filters + country_filters}
                cur = self.collection.find(filter=filter_, sort=sort_)
                reshaped_pages[ecountry] = [self.reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
        else:
            reshaped_pages = {}
            for etopic, itopics in ETOPIC_ITOPICS_MAP.items():
                if etopic == 'all':
                    continue
                topic_filters = [{"page.topics": {"$in": itopics}}]
                reshaped_pages[etopic] = {}
                for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items():
                    if ecountry == 'all':
                        continue
                    country_filters = [{"page.country": {"$in": icountries}}]
                    filter_ = {"$and": base_filters + topic_filters + country_filters}
                    cur = self.collection.find(filter=filter_, sort=sort_)
                    reshaped_pages[etopic][ecountry] = \
                        [self.reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
        return reshaped_pages

    def countries(self, ecountry: str, etopic: str, start: int, limit: int) -> List[dict]:
        base_filters = self.get_base_filters()
        sort_ = self.get_sort_metrics()
        if ecountry and etopic:
            country_filters = [{"page.country": {"$in": ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])}}]
            topic_filters = [{"page.topics": {"$in": ETOPIC_ITOPICS_MAP.get(etopic, [])}}]
            filter_ = {"$and": base_filters + country_filters + topic_filters}
            cur = self.collection.find(filter=filter_, sort=sort_)
            reshaped_pages = [self.reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
        elif ecountry:
            reshaped_pages = {}
            country_filters = [{"page.country": {"$in": ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])}}]
            for etopic, itopics in ETOPIC_ITOPICS_MAP.items():
                if etopic == 'all':
                    continue
                topic_filters = [{"page.topics": {"$in": itopics}}]
                filter_ = {"$and": base_filters + topic_filters + country_filters}
                cur = self.collection.find(filter=filter_, sort=sort_)
                reshaped_pages[etopic] = [self.reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
        else:
            reshaped_pages = {}
            for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items():
                if ecountry == 'all':
                    continue
                country_filters = [{"page.country": {"$in": icountries}}]
                reshaped_pages[ecountry] = {}
                for etopic, itopics in ETOPIC_ITOPICS_MAP.items():
                    if etopic == 'all':
                        continue
                    topic_filters = [{"page.topics": {"$in": itopics}}]
                    filter_ = {"$and": base_filters + topic_filters + country_filters}
                    cur = self.collection.find(filter=filter_, sort=sort_)
                    reshaped_pages[ecountry][etopic] = \
                        [self.reshape_page(doc["page"]) for doc in cur.skip(start).limit(limit)]
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

    def update_page(self, url, is_about_covid_19, is_useful, new_ecountry, new_etopics, notes, category_check_log_path):
        self.collection.update_one(
            {"page.url": url},
            {"$set": {
                "page.is_about_COVID-19": 1 if is_about_covid_19 else 0,
                "page.is_useful": 1 if is_useful else 0,
                "page.is_checked": 1,
                "page.displayed_country": ECOUNTRY_ICOUNTRIES_MAP[new_ecountry][0],
                "page.topics": [ETOPIC_ITOPICS_MAP[new_etopic] for new_etopic in new_etopics]
            }},
            upsert=True
        )

        updated = {
            "url": url,
            "is_about_COVID-19": 1 if is_about_covid_19 else 0,
            "is_useful": 1 if is_useful else 0,
            "new_country": ECOUNTRY_ICOUNTRIES_MAP[new_ecountry][0],
            "new_topics": [ETOPIC_ITOPICS_MAP[new_etopic] for new_etopic in new_etopics],
            "notes": notes,
            "time": datetime.now().isoformat()
        }
        with open(category_check_log_path, mode='a') as f:
            json.dump(updated, f, ensure_ascii=False)
            f.write('\n')
        return updated


def main():
    cfg = load_config()

    logger = logging.getLogger(__file__)
    logger.setLevel(20)
    fh = logging.FileHandler(cfg["database"]["log_path"], mode="a")
    logger.addHandler(fh)
    formatter = logging.Formatter("%(asctime)s:%(lineno)d:%(levelname)s:%(message)s")
    fh.setFormatter(formatter)

    mongo = DBHandler(
        host=cfg["database"]["host"],
        port=cfg["database"]["port"],
        db_name=cfg["database"]["db_name"],
        collection_name=cfg["database"]["collection_name"],
    )

    # add pages to the database or update pages
    with open(cfg["database"]["input_page_path"]) as f:
        for line in f:
            mongo.upsert_page(json.loads(line.strip()))
    num_docs = sum(1 for _ in mongo.collection.find())
    logger.log(20, f"Number of pages: {num_docs}")

    # add category-checked pages
    with open(cfg["database"]["category_check_log_path"], mode='r') as f:
        for line in f:
            if not line.strip():
                continue
            category_checked_page = json.loads(line.strip())
            existing_page = mongo.collection.find_one({"page.url": category_checked_page['url']})
            if not existing_page:
                continue

            mongo.collection.update_one(
                {"page.url": category_checked_page['url']},
                {"$set": {
                    "page.is_about_COVID-19": category_checked_page["is_about_COVID-19"],
                    "page.is_useful": category_checked_page["is_useful"],
                    "page.is_checked": 1,
                    "page.displayed_country": category_checked_page["new_country"],
                    "page.topics": category_checked_page["new_topics"]
                }},
            )


if __name__ == "__main__":
    main()
