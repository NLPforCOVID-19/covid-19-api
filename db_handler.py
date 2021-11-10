from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, asdict
from typing import List, Dict, Union, Optional

from elasticsearch import Elasticsearch
from pymongo import MongoClient, UpdateOne, DESCENDING

from util import (
    ITOPICS,
    ITOPIC_ETOPIC_MAP,
    ETOPIC_ITOPICS_MAP,
    ECOUNTRY_ICOUNTRIES_MAP,
    ETOPIC_TRANS_MAP,
    ECOUNTRY_TRANS_MAP,
    SENTIMENT_THRESHOLD
)


class Status(Enum):
    UPDATED = 0
    INSERTED = 1
    IGNORED = 2


@dataclass
class Tweet:
    _id: str
    name: str
    verified: bool
    username: str
    avatar: str
    timestamp: str
    simpleTimestamp: str
    contentOrig: str
    contentJaTrans: str
    contentEnTrans: str
    retweetCount: int
    country: str
    lang: str

    def as_api_ret(self, lang: str):
        return {
            "id": self._id,
            "contentTrans": self.contentJaTrans
            if lang == "ja"
            else self.contentEnTrans,
            **{
                key: getattr(self, key)
                for key in [
                    "name",
                    "verified",
                    "username",
                    "avatar",
                    "timestamp",
                    "contentOrig",
                    "lang",
                    "country",
                    "retweetCount",
                ]
            },
        }


class DBHandler:
    def __init__(
        self,
        mongo_host: str,
        mongo_port: int,
        mongo_db_name: str,
        mongo_article_collection_name: str,
        mongo_tweet_collection_name: str,
        es_host: str,
        es_port: int,
    ):
        self.mongo_cli = MongoClient(mongo_host, mongo_port)
        self.mongo_db = self.mongo_cli.get_database(mongo_db_name)
        self.article_coll = self.mongo_db.get_collection(
            name=mongo_article_collection_name
        )
        self.tweet_coll = self.mongo_db.get_collection(name=mongo_tweet_collection_name)
        self.es = Elasticsearch(f"{es_host}:{es_port}")

    def upsert_page(self, document: dict) -> Optional[Dict[str, str]]:
        """Add a page to the database. If the page has already been registered, update the page."""
        existing_page = self.article_coll.find_one({"page.url": document["url"]})
        if (
            existing_page
            and document["orig"]["timestamp"]
            > existing_page["page"]["orig"]["timestamp"]
        ):
            self.article_coll.update_one(
                {"page.url": document["url"]}, {"$set": {"page": document}}, upsert=True
            )
            document["status"] = Status.UPDATED
        elif not existing_page:
            self.article_coll.insert_one({"page": document})
            document["status"] = Status.INSERTED
        else:
            document["status"] = Status.IGNORED
        return document

    def upsert_tweets(self, tweets: List[Tweet]) -> Status:
        upserts = [
            UpdateOne({"_id": tweet._id}, {"$setOnInsert": asdict(tweet)}, upsert=True)
            for tweet in tweets
        ]
        self.tweet_coll.bulk_write(upserts)
        return Status.INSERTED

    def get_articles_sorted_by_topic(
        self, etopic: str, ecountry: str, start: int, limit: int, lang: str, query: str
    ):
        etopic = ETOPIC_TRANS_MAP.get((etopic, "ja"), etopic)
        ecountry = ECOUNTRY_TRANS_MAP.get((ecountry, "ja"), ecountry)
        if etopic and ecountry:
            if (
                etopic != "search" and etopic not in ETOPIC_ITOPICS_MAP
            ) or ecountry not in ECOUNTRY_ICOUNTRIES_MAP:
                return []
            reshaped_pages = self.get_articles(
                etopic, ecountry, start, limit, lang, query
            )
        elif etopic:
            reshaped_pages = {}
            for ecountry in filter(
                lambda ecountry_: ecountry_ != "all", ECOUNTRY_ICOUNTRIES_MAP.keys()
            ):
                if etopic != "search" and etopic not in ETOPIC_ITOPICS_MAP:
                    reshaped_pages[ecountry] = []
                else:
                    reshaped_pages[ecountry] = self.get_articles(
                        etopic, ecountry, start, limit, lang, query
                    )
        else:
            reshaped_pages = {}
            for etopic in filter(
                lambda etopic_: etopic_ != "all", ETOPIC_ITOPICS_MAP.keys()
            ):
                reshaped_pages[etopic] = {}
                for ecountry in filter(
                    lambda ecountry_: ecountry_ != "all", ECOUNTRY_ICOUNTRIES_MAP.keys()
                ):
                    reshaped_pages[etopic][ecountry] = self.get_articles(
                        etopic, ecountry, start, limit, lang, ""
                    )
        return reshaped_pages

    def get_articles_sorted_by_country(
        self, ecountry: str, etopic: str, start: int, limit: int, lang: str, query: str
    ):
        ecountry = ECOUNTRY_TRANS_MAP.get((ecountry, "ja"), ecountry)
        etopic = ETOPIC_TRANS_MAP.get((etopic, "ja"), etopic)
        if ecountry and etopic:
            if (
                etopic != "search" and etopic not in ETOPIC_ITOPICS_MAP
            ) or ecountry not in ECOUNTRY_ICOUNTRIES_MAP:
                return []
            reshaped_pages = self.get_articles(
                etopic, ecountry, start, limit, lang, query
            )
        elif ecountry:
            reshaped_pages = {}
            for etopic in filter(
                lambda ecountry_: ecountry_ != "all", ETOPIC_ITOPICS_MAP.keys()
            ):
                reshaped_pages[etopic] = self.get_articles(
                    etopic, ecountry, start, limit, lang, query
                )
        else:
            reshaped_pages = {}
            for ecountry in filter(
                lambda ecountry_: ecountry_ != "all", ECOUNTRY_ICOUNTRIES_MAP.keys()
            ):
                reshaped_pages[ecountry] = {}
                for etopic in filter(
                    lambda etopic_: etopic_ != "all", ETOPIC_ITOPICS_MAP.keys()
                ):
                    reshaped_pages[ecountry][etopic] = self.get_articles(
                        etopic, ecountry, start, limit, lang, ""
                    )
        return reshaped_pages

    def get_articles(
        self, etopic: str, ecountry: str, start: int, limit: int, lang: str, query: str, sentiment: bool = False
    ):
        # Utility functions.
        def get_sort(itopics_: List[str] = None):
            sort_ = [("page.orig.simple_timestamp", DESCENDING)]
            if itopics_:
                sort_ += [(f"page.topics.{itopic}", DESCENDING) for itopic in itopics_]
            return sort_

        def trim_snippet(search_snippet: str):
            if len(search_snippet) <= 70:
                return search_snippet
            else:
                split = search_snippet.split("<em>")
                prev_context, rest = split[0], "<em>".join(split[1:])
                prev_context = prev_context.split("、")[-1]
                return f"{prev_context}<em>{rest}"

        def reshape_article(doc: dict, search_snippet=None) -> dict:
            doc["topics"] = [
                {
                    "name": ETOPIC_TRANS_MAP[(ITOPIC_ETOPIC_MAP[itopic], lang)],
                    "snippet": doc[f"{lang}_snippets"][itopic],
                    "relatedness": doc["topics"][itopic],
                }
                for itopic in doc["topics"]
                if itopic in ITOPICS
            ]
            if search_snippet:
                doc["topics"].append(
                    {
                        "name": "Search",
                        "snippet": search_snippet[0],
                        "relatedness": -1.0,
                    }
                )
            doc["translated"] = doc[f"{lang}_translated"]
            doc["domain_label"] = doc[f"{lang}_domain_label"]
            doc["is_about_false_rumor"] = (
                1 if doc["domain"] == "fij.info" else doc["is_about_false_rumor"]
            )
            del doc["ja_snippets"]
            del doc["en_snippets"]
            del doc["ja_translated"]
            del doc["en_translated"]
            del doc["ja_domain_label"]
            del doc["en_domain_label"]
            return doc

        # Use ElasticSearch to search for articles.
        if etopic and etopic == "search":
            icountries = ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])
            r = self.es.search(
                index="covid19-pages-ja" if lang == "ja" else "covid19-pages-en",
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "should": [
                                            {"term": {"region": icountry}}
                                            for icountry in icountries
                                        ]
                                    }
                                },
                                {"match": {"text": query}},
                            ]
                        }
                    },
                    "highlight": {"fields": {"text": {}}},
                    "sort": [
                        {
                            "timestamp.local": {
                                "order": "desc",
                                "nested": {"path": "timestamp"},
                            }
                        }
                    ],
                    "from": start,
                    "size": limit,
                },
            )
            hits = r["hits"]["hits"]
            if len(hits) == 0:
                return []

            url_to_hit = {hit["_source"]["url"]: hit for hit in hits}
            cur = self.article_coll.find(
                filter={"$or": [{"page.url": hit["_source"]["url"]} for hit in hits]},
                sort=get_sort(),
            )
            return [
                reshape_article(
                    d["page"],
                    trim_snippet(url_to_hit[d["page"]["url"]]["highlight"]["text"]),
                )
                for d in cur
            ]

        # Use MongoDB to search for articles.
        itopics = ETOPIC_ITOPICS_MAP.get(etopic, [])
        icountries = ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])

        filters = [{"$and": [{"page.is_about_COVID-19": 1}, {"page.is_hidden": 0}]}]

        if itopics:
            temp_itopics = itopics[:]

            # Hide articles about 感染状況 unless the selected topic is 感染状況 explicitly when
            # retrieving articles for the positive news.
            if sentiment and temp_itopics != ['感染状況'] and '感染状況' in temp_itopics:
                temp_itopics.remove('感染状況')

            filters += [
                {
                    "$or": [
                        {f"page.topics.{itopic}": {"$exists": True}}
                        for itopic in temp_itopics
                    ]
                }
            ]
        if ecountry:
            filters += [{"page.displayed_country": {"$in": icountries}}]
        if sentiment:
            timestamp_threshold = datetime.now() - timedelta(days=30)

            filters += [
                {"$or": [{"page.is_positive": {"$exists": False}}, {"page.is_positive": 1}]},
                {"page.sentiment": {"$exists": True}},
                {"page.sentiment": {"$gte": SENTIMENT_THRESHOLD}},
                {"page.orig.timestamp": {"$gte": timestamp_threshold.isoformat()}}
            ]
        filter_ = {"$and": filters}
        sort_ = get_sort(itopics)
        if sentiment:
            sort_.insert(1, ("page.sentiment", DESCENDING))
        cur = self.article_coll.find(filter=filter_, sort=sort_)
        reshaped_articles = [
            reshape_article(doc["page"]) for doc in cur.skip(start).limit(limit)
        ]
        return reshaped_articles

    def get_positive_articles(self, etopic: str, ecountry: str, lang: str, query: str):
        if etopic is None:
            etopic = "all"
        if ecountry is None:
            ecountry = "all"
        etopic = ETOPIC_TRANS_MAP.get((etopic, "ja"), etopic)
        return self.get_articles(etopic, ecountry, 0, 5, lang, "", sentiment=True)

    def get_tweets_sorted_by_topic(
        self, etopic: str, ecountry: str, start: int, limit: int, lang: str, query: str
    ):
        etopic = ETOPIC_TRANS_MAP.get((etopic, "ja"), etopic)
        ecountry = ECOUNTRY_TRANS_MAP.get((ecountry, "ja"), ecountry)
        if etopic and ecountry:
            if (
                etopic != "search" and etopic not in ETOPIC_ITOPICS_MAP
            ) or ecountry not in ECOUNTRY_ICOUNTRIES_MAP:
                return []
            reshaped_tweets = self.get_tweets(
                etopic, ecountry, start, limit, lang, query
            )
        elif etopic:
            reshaped_tweets = {}
            for ecountry in filter(
                lambda ecountry_: ecountry_ != "all", ECOUNTRY_ICOUNTRIES_MAP.keys()
            ):
                if etopic != "search" and etopic not in ETOPIC_ITOPICS_MAP:
                    reshaped_tweets[ecountry] = []
                else:
                    reshaped_tweets[ecountry] = self.get_tweets(
                        etopic, ecountry, start, limit, lang, query
                    )
        else:
            reshaped_tweets = {}
            for etopic in ["all"]:
                reshaped_tweets[etopic] = {}
                for ecountry in filter(
                    lambda ecountry_: ecountry_ != "all", ECOUNTRY_ICOUNTRIES_MAP.keys()
                ):
                    reshaped_tweets[etopic][ecountry] = self.get_tweets(
                        etopic, ecountry, start, limit, lang, query
                    )
        return reshaped_tweets

    def get_tweets_sorted_by_country(
        self, ecountry: str, etopic: str, start: int, limit: int, lang: str, query: str
    ):
        ecountry = ECOUNTRY_TRANS_MAP.get((ecountry, "ja"), ecountry)
        etopic = ETOPIC_TRANS_MAP.get((etopic, "ja"), etopic)
        if ecountry and etopic:
            if (
                etopic != "search" and etopic not in ETOPIC_ITOPICS_MAP
            ) or ecountry not in ECOUNTRY_ICOUNTRIES_MAP:
                return []
            reshaped_tweets = self.get_tweets(
                etopic, ecountry, start, limit, lang, query
            )
        elif ecountry:
            reshaped_tweets = {}
            for etopic in ["all"]:
                reshaped_tweets[etopic] = self.get_tweets(
                    etopic, ecountry, start, limit, lang, query
                )
        else:
            reshaped_tweets = {}
            for ecountry in filter(
                lambda ecountry_: ecountry_ != "all", ECOUNTRY_ICOUNTRIES_MAP.keys()
            ):
                reshaped_tweets[ecountry] = {}
                for etopic in ["all"]:
                    reshaped_tweets[ecountry][etopic] = self.get_tweets(
                        etopic, ecountry, start, limit, lang, query
                    )
        return reshaped_tweets

    def get_tweets(
        self, etopic: str, ecountry: str, start: int, limit: int, lang: str, query: str
    ) -> List[dict]:
        # Use ElasticSearch to search for articles.
        if etopic and etopic == "search":
            icountries = ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])
            r = self.es.search(
                index="covid19-tweets-ja" if lang == "ja" else "covid19-tweets-en",
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "should": [
                                            {"term": {"country": icountry}}
                                            for icountry in icountries
                                        ]
                                    }
                                },
                                {"match": {"text": query}},
                            ]
                        }
                    },
                    "sort": [
                        {
                            "timestamp.local": {
                                "order": "desc",
                                "nested": {"path": "timestamp"},
                            }
                        }
                    ],
                    "from": start,
                    "size": limit,
                },
            )
            hits = r["hits"]["hits"]
            if len(hits) == 0:
                return []
            cur = self.tweet_coll.find(
                filter={"_id": {"$in": [hit["_id"] for hit in hits]}},
                sort=[("simpleTimestamp", DESCENDING)],
            )
            return [Tweet(**doc).as_api_ret(lang) for doc in cur]

        # Use MongoDB to search for articles.
        if etopic != "all":
            return (
                []
            )  # This is because tweets are not categorized by topics at the moment.
        icountries = ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])
        filter_ = {"country": {"$in": icountries}}
        sort_ = [
            ("simpleTimestamp", DESCENDING),
            ("retweetCount", DESCENDING),
            ("timestamp", DESCENDING),
        ]
        cur = self.tweet_coll.find(filter=filter_, sort=sort_)
        return [Tweet(**doc).as_api_ret(lang) for doc in cur.skip(start).limit(limit)]

    def update_page(
        self,
        url: str,
        is_hidden: bool,
        is_about_covid_19: bool,
        is_useful: bool,
        is_about_false_rumor: bool,
        is_positive: bool,
        icountry: str,
        etopics: List[str],
        notes: str,
    ) -> Dict[str, Union[int, str, List[str]]]:
        new_is_hidden = 1 if is_hidden else 0
        new_is_about_covid_19 = 1 if is_about_covid_19 else 0
        new_is_useful = 1 if is_useful else 0
        new_is_about_false_rumor = 1 if is_about_false_rumor else 0
        new_is_positive = 1 if is_positive else 0
        new_etopics = {ETOPIC_ITOPICS_MAP[etopic][0]: 1.0 for etopic in etopics}

        self.article_coll.update_one(
            {"page.url": url},
            {
                "$set": {
                    "page.is_hidden": new_is_hidden,
                    "page.is_about_COVID-19": new_is_about_covid_19,
                    "page.is_useful": new_is_useful,
                    "page.is_about_false_rumor": new_is_about_false_rumor,
                    "page.is_positive": new_is_positive,
                    "page.is_checked": 1,
                    "page.displayed_country": icountry,
                    "page.topics": new_etopics,
                }
            },
            upsert=True,
        )
        return {
            "url": url,
            "is_hidden": new_is_hidden,
            "is_about_COVID-19": new_is_about_covid_19,
            "is_useful": new_is_useful,
            "is_about_false_rumor": new_is_about_false_rumor,
            "is_positive": new_is_positive,
            "new_country": icountry,
            "new_topics": list(new_etopics.keys()),
            "notes": notes,
            "time": datetime.now().isoformat(),
        }
