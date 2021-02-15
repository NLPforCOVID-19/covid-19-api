from datetime import datetime
from enum import Enum
from typing import List, Dict, Union, Optional

from elasticsearch import Elasticsearch
from pymongo import MongoClient, DESCENDING

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


class DBHandler:

    def __init__(
            self,
            mongo_host: str,
            mongo_port: int,
            mongo_db_name: str,
            mongo_collection_name: str,
            es_host: str,
            es_port: int,
    ):
        self.mongo = MongoClient(mongo_host, mongo_port)
        self.db = self.mongo.get_database(mongo_db_name)
        self.collection = self.db.get_collection(name=mongo_collection_name)
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

        existing_page = self.collection.find_one({'page.url': url})
        if existing_page and orig['timestamp'] > existing_page['page']['orig']['timestamp']:
            self.collection.update_one({'page.url': url}, {'$set': {'page': document_}}, upsert=True)
            document_['status'] = Status.UPDATED
        elif not existing_page:
            self.collection.insert_one({'page': document_})
            document_['status'] = Status.INSERTED
        else:
            document_['status'] = Status.IGNORED
        return document_

    def articles(self, etopic: str, ecountry: str, start: int, limit: int, lang: str, query: str):
        if etopic == 'search':
            return self.search(ecountry, start, limit, lang, query)

        etopic = ETOPIC_TRANS_MAP.get((etopic, 'ja'), etopic)
        ecountry = ECOUNTRY_TRANS_MAP.get((ecountry, 'ja'), ecountry)

        if etopic and ecountry:
            if etopic not in ETOPIC_ITOPICS_MAP or ecountry not in ECOUNTRY_ICOUNTRIES_MAP:
                return []
            itopics = ETOPIC_ITOPICS_MAP[etopic]
            icountries = ECOUNTRY_ICOUNTRIES_MAP[ecountry]
            reshaped_pages = self.get_pages(itopics, icountries, start, limit, lang)
        elif etopic:
            if etopic not in ETOPIC_ITOPICS_MAP:
                return {ecountry: [] for ecountry in ECOUNTRY_ICOUNTRIES_MAP.keys()}
            itopics = ETOPIC_ITOPICS_MAP[etopic]
            reshaped_pages = {}
            for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items():
                if ecountry == 'all':
                    continue
                reshaped_pages[ecountry] = self.get_pages(itopics, icountries, start, limit, lang)
        else:
            reshaped_pages = {}
            for etopic, itopics in ETOPIC_ITOPICS_MAP.items():
                if etopic == 'all':
                    continue
                reshaped_pages[etopic] = {}
                for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items():
                    if ecountry == 'all':
                        continue
                    reshaped_pages[etopic][ecountry] = self.get_pages(itopics, icountries, start, limit, lang)
        return reshaped_pages

    def tweets(self, ecountry: str, start: int, limit: int, lang: str, query: str):
        if ecountry:
            return [
                {
                    'id': '1357006259413635076',
                    'name': "nlpforcovid-19",
                    'verified': True,
                    'username': "nlpforcovid",
                    'avatar': "https://pbs.twimg.com/profile_images/1347024085952331778/3oBHXOOn_bigger.jpg",
                    'contentOrig': "欧州がより多くのワクチンを求めている（ヨーロッパ，経済・福祉政策のニュース，France 24",
                    'contentTrans': None,
                    'timestamp': "2021-02-10 14:45:03"
                },
            ]
        else:
            return {
                ecountry: {
                    'id': '1357006259413635076',
                    'name': "nlpforcovid-19",
                    'verified': True,
                    'username': "nlpforcovid",
                    'avatar': "https://pbs.twimg.com/profile_images/1347024085952331778/3oBHXOOn_bigger.jpg",
                    'contentOrig': "欧州がより多くのワクチンを求めている（ヨーロッパ，経済・福祉政策のニュース，France 24",
                    'contentTrans': None,
                    'timestamp': "2021-02-10 14:45:03"
                } for ecountry in ECOUNTRY_ICOUNTRIES_MAP.keys()
            }

    def search(self, ecountry: str, start: int, limit: int, lang: str, query: str):
        def get_es_query(regions: List[str]):
            return {
                'query': {
                    'bool': {
                        'must': [
                            {
                                'bool': {
                                    'should': [
                                        {
                                            'term': {
                                                'region': region
                                            }
                                        }
                                        for region in regions
                                    ]
                                }
                            },
                            {
                                'match': {
                                    'text': query
                                }
                            },
                        ],
                    }
                },
                "highlight": {
                    "fields": {
                        "text": {}
                    }
                },
                'sort': [{
                    'timestamp.local': {
                        'order': 'desc',
                        'nested': {
                            'path': 'timestamp'
                        }
                    }
                }],
                'from': start,
                'size': limit,
            }

        def convert_hits_to_pages(hits: list) -> list:
            if len(hits) == 0:
                return []
            url_to_hit = {hit['_source']['url']: hit for hit in hits}
            cur = self.collection.find(
                filter={'$or': [{'page.url': hit['_source']['url']} for hit in hits]},
                sort=self.get_sort()
            )
            return [
                self.reshape_page(d['page'], lang, self.trim_snippet(url_to_hit[d['page']['url']]['highlight']['text']))
                for d in cur
            ]

        index = 'covid19-pages-ja' if lang == 'ja' else 'covid19-pages-en'

        if ecountry:
            body = get_es_query([c for c in ECOUNTRY_ICOUNTRIES_MAP.get(ecountry, [])])
            r = self.es.search(index=index, body=body)
            return convert_hits_to_pages(r['hits']['hits'])
        else:
            reshaped_pages = {}
            for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items():
                if ecountry == 'all':
                    continue
                body = get_es_query(icountries)
                r = self.es.search(index=index, body=body)
                reshaped_pages[ecountry] = convert_hits_to_pages(r['hits']['hits'])
            return reshaped_pages

    def get_pages(self, itopics: List[str], icountries: List[str], start: int, limit: int, lang: str) -> List[dict]:
        filter_ = self.get_filter(itopics, icountries)
        sort_ = self.get_sort(itopics)
        cur = self.collection.find(filter=filter_, sort=sort_)
        return [self.reshape_page(doc['page'], lang) for doc in cur.skip(start).limit(limit)]

    @staticmethod
    def get_filter(itopics: List[str] = None, icountries: List[str] = None) -> Dict[str, List]:
        filters = [{'$and': [{'page.is_about_COVID-19': 1}, {'page.is_hidden': 0}]}]
        if itopics:
            filters += [{'$or': [{f'page.topics.{itopic}': {'$exists': True}} for itopic in itopics]}]
        if icountries:
            filters += [{'page.displayed_country': {'$in': icountries}}]
        return {'$and': filters}

    @staticmethod
    def get_sort(itopics: List[str] = None):
        sort_ = [('page.orig.simple_timestamp', DESCENDING)]
        if itopics:
            sort_ += [(f'page.topics.{itopic}', DESCENDING) for itopic in itopics]
        return sort_

    @staticmethod
    def reshape_page(page: dict, lang: str, search_snippet=None) -> dict:
        page['topics'] = [
            {
                'name': ETOPIC_TRANS_MAP[(ITOPIC_ETOPIC_MAP[itopic], lang)],
                'snippet': page[f'{lang}_snippets'][itopic],
                'relatedness': page['topics'][itopic]
            }
            for itopic in page['topics'] if itopic in ITOPICS
        ]
        if search_snippet:
            page['topics'].append(
                {
                    'name': 'Search',
                    'snippet': search_snippet[0],
                    'relatedness': -1.
                }
            )
        page['translated'] = page[f'{lang}_translated']
        page['domain_label'] = page[f'{lang}_domain_label']
        page['is_about_false_rumor'] = 1 if page['domain'] == 'fij.info' else page['is_about_false_rumor']
        del page['ja_snippets']
        del page['en_snippets']
        del page['ja_translated']
        del page['en_translated']
        del page['ja_domain_label']
        del page['en_domain_label']
        return page

    @staticmethod
    def trim_snippet(search_snippet):
        if len(search_snippet) <= 70:
            return search_snippet
        else:
            split = search_snippet.split('<em>')
            prev_context, rest = split[0], '<em>'.join(split[1:])
            prev_context = prev_context.split('、')[-1]
            return f'{prev_context}<em>{rest}'

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

        self.collection.update_one(
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
