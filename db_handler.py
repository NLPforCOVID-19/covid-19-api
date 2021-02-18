from datetime import datetime
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
    SCORE_THRESHOLD,
    RUMOR_THRESHOLD,
    USEFUL_THRESHOLD
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
    retweetCount: int
    country: str
    contentJaTrans: str = ''
    contentEnTrans: str = ''

    def as_api_ret(self, lang: str):
        return {
            'id': str(self._id),
            'contentTrans': self.contentJaTrans if lang == 'ja' else self.contentEnTrans,
            **{key: getattr(self, key) for key in
               ['name', 'verified', 'username', 'avatar', 'timestamp', 'contentOrig']},
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
        self.article_coll = self.mongo_db.get_collection(name=mongo_article_collection_name)
        self.tweet_coll = self.mongo_db.get_collection(name=mongo_tweet_collection_name)
        self.es = Elasticsearch(f'{es_host}:{es_port}')

    def upsert_page(self, document: dict) -> Optional[Dict[str, str]]:
        """Add a page to the database. If the page has already been registered, update the page."""
        if any((
                not document['orig']['title'],
                not document['ja_translated']['title'],
                not document['en_translated']['title'],
        )):
            return

        def reshape_snippets(snippets: Dict[str, List[str]]) -> Dict[str, str]:
            # Find a general snippet.
            general_snippet = ''
            for itopic in ITOPICS:
                if itopic in snippets:
                    general_snippet = snippets[itopic][0] if snippets[itopic] else ''
                    break

            # Reshape snippets.
            reshaped = {}
            for itopic in ITOPICS:
                snippets_about_topic = snippets.get(itopic, [])
                if snippets_about_topic and snippets_about_topic[0]:
                    reshaped[itopic] = snippets_about_topic[0].strip()
                elif general_snippet:
                    reshaped[itopic] = general_snippet
                else:
                    reshaped[itopic] = ''
            return reshaped

        is_about_covid_19: int = document['classes']['is_about_COVID-19']
        country: str = document['country']
        orig: Dict[str, str] = {
            'title': document['orig']['title'].strip(),
            'timestamp': document['orig']['timestamp'],
            'simple_timestamp': datetime.fromisoformat(document['orig']['timestamp']).date().isoformat(),
        }
        ja_translated: Dict[str, str] = {
            'title': document['ja_translated']['title'].strip(),
            'timestamp': document['ja_translated']['timestamp'],
        }
        en_translated: Dict[str, str] = {
            'title': document['en_translated']['title'].strip(),
            'timestamp': document['en_translated']['timestamp'],
        }
        url: str = document['url']
        topics_to_score: Dict[str, float] = {
            key: value for key, value in document['classes_bert'].items() if key in ITOPICS and value > 0.5
        }
        topics: Dict[str, float] = dict()
        for idx, (topic, score) in enumerate(sorted(topics_to_score.items(), key=lambda x: x[1], reverse=True)):
            if idx == 0 or score > SCORE_THRESHOLD:
                topics[topic] = float(score)
            else:
                break
        ja_snippets = reshape_snippets(document['snippets'])
        en_snippets = reshape_snippets(document['snippets_en'])

        is_checked = 0
        is_useful = 1 if document['classes_bert']['is_useful'] > USEFUL_THRESHOLD else 0
        is_clear = document['classes']['is_clear']
        is_about_false_rumor = 1 if document['classes_bert']['is_about_false_rumor'] > RUMOR_THRESHOLD else 0

        domain = document.get('domain', '')
        ja_domain_label = document.get('domain_label', '')
        en_domain_label = document.get('domain_label_en', '')
        document_ = {
            'country': country,
            'displayed_country': country,
            'orig': orig,
            'ja_translated': ja_translated,
            'en_translated': en_translated,
            'url': url,
            'topics': topics,
            'ja_snippets': ja_snippets,
            'en_snippets': en_snippets,
            'is_checked': is_checked,
            'is_hidden': 0,
            'is_about_COVID-19': is_about_covid_19,
            'is_useful': is_useful,
            'is_clear': is_clear,
            'is_about_false_rumor': is_about_false_rumor,
            'domain': domain,
            'ja_domain_label': ja_domain_label,
            'en_domain_label': en_domain_label
        }

        existing_page = self.article_coll.find_one({'page.url': url})
        if existing_page and orig['timestamp'] > existing_page['page']['orig']['timestamp']:
            self.article_coll.update_one({'page.url': url}, {'$set': {'page': document_}}, upsert=True)
            document_['status'] = Status.UPDATED
        elif not existing_page:
            self.article_coll.insert_one({'page': document_})
            document_['status'] = Status.INSERTED
        else:
            document_['status'] = Status.IGNORED
        return document_

    def upsert_tweets(self, tweets: List[Tweet]) -> Status:
        upserts = [UpdateOne({'_id': tweet._id}, {'$setOnInsert': asdict(tweet)}, upsert=True) for tweet in tweets]
        self.tweet_coll.bulk_write(upserts)
        return Status.INSERTED

    def get_articles_sorted_by_topic(self, etopic: str, ecountry: str, start: int, limit: int, lang: str, query: str):
        etopic = ETOPIC_TRANS_MAP.get((etopic, 'ja'), etopic)
        ecountry = ECOUNTRY_TRANS_MAP.get((ecountry, 'ja'), ecountry)
        if etopic and ecountry:
            if (etopic != 'search' and etopic not in ETOPIC_ITOPICS_MAP) or ecountry not in ECOUNTRY_ICOUNTRIES_MAP:
                return []
            reshaped_pages = self.get_articles(etopic, ecountry, start, limit, lang, query)
        elif etopic:
            reshaped_pages = {}
            for ecountry in filter(lambda ecountry_: ecountry_ != 'all', ECOUNTRY_ICOUNTRIES_MAP.keys()):
                if etopic != 'search' and etopic not in ETOPIC_ITOPICS_MAP:
                    reshaped_pages[ecountry] = []
                else:
                    reshaped_pages[ecountry] = self.get_articles(etopic, ecountry, start, limit, lang, query)
        else:
            reshaped_pages = {}
            for etopic in filter(lambda etopic_: etopic_ != 'all', ETOPIC_ITOPICS_MAP.keys()):
                reshaped_pages[etopic] = {}
                for ecountry in filter(lambda ecountry_: ecountry_ != 'all', ECOUNTRY_ICOUNTRIES_MAP.keys()):
                    reshaped_pages[etopic][ecountry] = self.get_articles(etopic, ecountry, start, limit, lang, '')
        return reshaped_pages

    def get_articles_sorted_by_country(self, ecountry: str, etopic: str, start: int, limit: int, lang: str, query: str):
        ecountry = ECOUNTRY_TRANS_MAP.get((ecountry, 'ja'), ecountry)
        etopic = ETOPIC_TRANS_MAP.get((etopic, 'ja'), etopic)
        if ecountry and etopic:
            if (etopic != 'search' and etopic not in ETOPIC_ITOPICS_MAP) or ecountry not in ECOUNTRY_ICOUNTRIES_MAP:
                return []
            reshaped_pages = self.get_articles(etopic, ecountry, start, limit, lang, query)
        elif ecountry:
            reshaped_pages = {}
            for etopic in filter(lambda ecountry_: ecountry_ != 'all', ETOPIC_ITOPICS_MAP.keys()):
                reshaped_pages[etopic] = self.get_articles(etopic, ecountry, start, limit, lang, query)
        else:
            reshaped_pages = {}
            for ecountry in filter(lambda ecountry_: ecountry_ != 'all', ECOUNTRY_ICOUNTRIES_MAP.keys()):
                reshaped_pages[ecountry] = {}
                for etopic in filter(lambda etopic_: etopic_ != 'all', ETOPIC_ITOPICS_MAP.keys()):
                    reshaped_pages[ecountry][etopic] = self.get_articles(etopic, ecountry, start, limit, lang, '')
        return reshaped_pages

    def get_articles(self, etopic: str, ecountry: str, start: int, limit: int, lang: str, query: str):
        # Utility functions.
        def get_sort(itopics_: List[str] = None):
            sort_ = [('page.orig.simple_timestamp', DESCENDING)]
            if itopics_:
                sort_ += [(f'page.topics.{itopic}', DESCENDING) for itopic in itopics_]
            return sort_

        def trim_snippet(search_snippet: str):
            if len(search_snippet) <= 70:
                return search_snippet
            else:
                split = search_snippet.split('<em>')
                prev_context, rest = split[0], '<em>'.join(split[1:])
                prev_context = prev_context.split('、')[-1]
                return f'{prev_context}<em>{rest}'

        def reshape_article(doc: dict, search_snippet=None) -> dict:
            doc['topics'] = [{
                'name': ETOPIC_TRANS_MAP[(ITOPIC_ETOPIC_MAP[itopic], lang)],
                'snippet': doc[f'{lang}_snippets'][itopic],
                'relatedness': doc['topics'][itopic]
            } for itopic in doc['topics'] if itopic in ITOPICS]
            if search_snippet:
                doc['topics'].append({
                    'name': 'Search',
                    'snippet': search_snippet[0],
                    'relatedness': -1.
                })
            doc['translated'] = doc[f'{lang}_translated']
            doc['domain_label'] = doc[f'{lang}_domain_label']
            doc['is_about_false_rumor'] = 1 if doc['domain'] == 'fij.info' else doc['is_about_false_rumor']
            del doc['ja_snippets']
            del doc['en_snippets']
            del doc['ja_translated']
            del doc['en_translated']
            del doc['ja_domain_label']
            del doc['en_domain_label']
            return doc

        # Use ElasticSearch to search for articles.
        if etopic and etopic == 'search':
            icountries = ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])
            r = self.es.search(
                index='covid19-pages-ja' if lang == 'ja' else 'covid19-pages-en',
                body={
                    'query': {'bool': {'must': [
                        {'bool': {'should': [{'term': {'region': icountry}} for icountry in icountries]}},
                        {'match': {'text': query}}
                    ]}},
                    'highlight': {'fields': {'text': {}}},
                    'sort': [{'timestamp.local': {
                        'order': 'desc',
                        'nested': {'path': 'timestamp'}
                    }}],
                    'from': start,
                    'size': limit,
                }
            )
            hits = r['hits']['hits']
            if len(hits) == 0:
                return []

            url_to_hit = {hit['_source']['url']: hit for hit in hits}
            cur = self.article_coll.find(
                filter={'$or': [{'page.url': hit['_source']['url']} for hit in hits]},
                sort=get_sort()
            )
            return [reshape_article(
                d['page'],
                trim_snippet(url_to_hit[d['page']['url']]['highlight']['text'])
            ) for d in cur]

        # Use MongoDB to search for articles.
        itopics = ETOPIC_ITOPICS_MAP.get(etopic, [])
        icountries = ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])

        filters = [{'$and': [{'page.is_about_COVID-19': 1}, {'page.is_hidden': 0}]}]
        if itopics:
            filters += [{'$or': [{f'page.topics.{itopic}': {'$exists': True}} for itopic in itopics]}]
        if ecountry:
            filters += [{'page.displayed_country': {'$in': icountries}}]
        filter_ = {'$and': filters}
        sort_ = get_sort(itopics)
        cur = self.article_coll.find(filter=filter_, sort=sort_)
        reshaped_articles = [reshape_article(doc['page'], lang) for doc in cur.skip(start).limit(limit)]
        return reshaped_articles

    def get_tweets_sorted_by_topic(self, etopic: str, ecountry: str, start: int, limit: int, lang: str, query: str):
        etopic = ETOPIC_TRANS_MAP.get((etopic, 'ja'), etopic)
        ecountry = ECOUNTRY_TRANS_MAP.get((ecountry, 'ja'), ecountry)
        if etopic and ecountry:
            if (etopic != 'search' and etopic not in ETOPIC_ITOPICS_MAP) or ecountry not in ECOUNTRY_ICOUNTRIES_MAP:
                return []
            reshaped_tweets = self.get_tweets(etopic, ecountry, start, limit, lang, query)
        elif etopic:
            reshaped_tweets = {}
            for ecountry in filter(lambda ecountry_: ecountry_ != 'all', ECOUNTRY_ICOUNTRIES_MAP.keys()):
                if etopic != 'search' and etopic not in ETOPIC_ITOPICS_MAP:
                    reshaped_tweets[ecountry] = []
                else:
                    reshaped_tweets[ecountry] = self.get_tweets(etopic, ecountry, start, limit, lang, query)
        else:
            reshaped_tweets = {}
            for etopic in ['all']:
                reshaped_tweets[etopic] = {}
                for ecountry in filter(lambda ecountry_: ecountry_ != 'all', ECOUNTRY_ICOUNTRIES_MAP.keys()):
                    reshaped_tweets[etopic][ecountry] = self.get_tweets(etopic, ecountry, start, limit, lang, query)
        return reshaped_tweets

    def get_tweets_sorted_by_country(self, ecountry: str, etopic: str, start: int, limit: int, lang: str, query: str):
        ecountry = ECOUNTRY_TRANS_MAP.get((ecountry, 'ja'), ecountry)
        etopic = ETOPIC_TRANS_MAP.get((etopic, 'ja'), etopic)
        if ecountry and etopic:
            if (etopic != 'search' and etopic not in ETOPIC_ITOPICS_MAP) or ecountry not in ECOUNTRY_ICOUNTRIES_MAP:
                return []
            reshaped_tweets = self.get_tweets(etopic, ecountry, start, limit, lang, query)
        elif ecountry:
            reshaped_tweets = {}
            for etopic in ['all']:
                reshaped_tweets[etopic] = self.get_tweets(etopic, ecountry, start, limit, lang, query)
        else:
            reshaped_tweets = {}
            for ecountry in filter(lambda ecountry_: ecountry_ != 'all', ECOUNTRY_ICOUNTRIES_MAP.keys()):
                reshaped_tweets[ecountry] = {}
                for etopic in ['all']:
                    reshaped_tweets[ecountry][etopic] = self.get_tweets(etopic, ecountry, start, limit, lang, query)
        return reshaped_tweets

    def get_tweets(self, etopic: str, ecountry: str, start: int, limit: int, lang: str, query: str) -> List[dict]:
        # Use ElasticSearch to search for articles.
        if etopic and etopic == 'search':
            icountries = ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])
            r = self.es.search(
                index='covid19-tweets-ja' if lang == 'ja' else 'covid19-tweets-en',
                body={
                    'query': {'bool': {'must': [
                        {'bool': {'should': [{'term': {'country': icountry}} for icountry in icountries]}},
                        {'match': {'text': query}},
                    ]}},
                    'sort': [{'timestamp.local': {
                        'order': 'desc',
                        'nested': {'path': 'timestamp'}
                    }}],
                    'from': start,
                    'size': limit,
                }
            )
            hits = r['hits']['hits']
            if len(hits) == 0:
                return []
            cur = self.tweet_coll.find(
                filter={'_id': {'$in': [int(hit['_id']) for hit in hits]}},
                sort=[('simpleTimestamp', DESCENDING)]
            )
            return [Tweet(**doc).as_api_ret(lang) for doc in cur]

        # Use MongoDB to search for articles.
        if etopic != 'all':
            return []  # This is because tweets are not categorized by topics at the moment.
        icountries = ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])
        filter_ = {'country': {'$in': icountries}}
        sort_ = [('simpleTimestamp', DESCENDING), ('retweetCount', DESCENDING)]
        cur = self.tweet_coll.find(filter=filter_, sort=sort_)
        return [Tweet(**doc).as_api_ret(lang) for doc in cur.skip(start).limit(limit)]

    def update_page(
            self,
            url: str,
            is_hidden: bool,
            is_about_covid_19: bool,
            is_useful: bool,
            is_about_false_rumor: bool,
            icountry: str,
            etopics: List[str],
            notes: str
    ) -> Dict[str, Union[int, str, List[str]]]:
        new_is_hidden = 1 if is_hidden else 0
        new_is_about_covid_19 = 1 if is_about_covid_19 else 0
        new_is_useful = 1 if is_useful else 0
        new_is_about_false_rumor = 1 if is_about_false_rumor else 0
        new_etopics = {ETOPIC_ITOPICS_MAP[etopic][0]: 1.0 for etopic in etopics}

        self.article_coll.update_one(
            {'page.url': url},
            {'$set': {
                'page.is_hidden': new_is_hidden,
                'page.is_about_COVID-19': new_is_about_covid_19,
                'page.is_useful': new_is_useful,
                'page.is_about_false_rumor': new_is_about_false_rumor,
                'page.is_checked': 1,
                'page.displayed_country': icountry,
                'page.topics': new_etopics
            }},
            upsert=True
        )
        return {
            'url': url,
            'is_hidden': new_is_hidden,
            'is_about_COVID-19': new_is_about_covid_19,
            'is_useful': new_is_useful,
            'is_about_false_rumor': new_is_about_false_rumor,
            'new_country': icountry,
            'new_topics': list(new_etopics.keys()),
            'notes': notes,
            'time': datetime.now().isoformat()
        }
