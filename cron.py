import argparse
import collections
import json
import logging
import random
import shutil
import tempfile
import time
import pathlib
from datetime import datetime, timedelta
import urllib.parse
import urllib.request

import pandas as pd
from fcache.cache import FileCache

from db_handler import DBHandler, Status, Tweet
from log_handler import LogHandler
from meta_data_handler import MetaDataHandler
from twitter_handler import TwitterHandler
from util import load_config, COUNTRIES, ECOUNTRY_ICOUNTRIES_MAP

logger = logging.getLogger(__file__)

cfg = load_config()

meta_data_handler = MetaDataHandler()
db_handler = DBHandler(**cfg['db_handler'])
log_handler = LogHandler(**cfg['log_handler'])
twitter_handler = TwitterHandler(**cfg['twitter_handler'])


def update_database(do_tweet: bool = False):
    logger.debug('Add automatically categorized pages.')
    data_path = cfg['data']['article_list']
    offset_cache = FileCache('offset_cache', flag='cs', app_cache_dir='./')
    offset = offset_cache.get("offset", 0)
    maybe_tweeted_ds = []
    with open(data_path, mode='r', encoding='utf-8') as f:
        f.seek(offset)
        for line in f:
            d = db_handler.upsert_page(json.loads(line))
            if d and do_tweet and d['status'] == Status.INSERTED and d['is_useful']:
                maybe_tweeted_ds.append(d)
        else:
            offset_cache["offset"] = f.tell()
    num_docs = db_handler.article_coll.count_documents({})
    log_handler.extend_page_number_log([f'{time.asctime()}:The number of pages is {num_docs}.'])

    logger.debug('Add manually checked pages.')
    for line in log_handler.iterate_topic_check_log():
        log = json.loads(line)
        existing_page = db_handler.article_coll.find_one({'page.url': log['url']})
        if not existing_page:
            continue
        db_handler.article_coll.update_one(
            {'page.url': log['url']},
            {'$set': {
                'page.is_about_COVID-19': log['is_about_COVID-19'],
                'page.is_useful': log['is_useful'],
                'page.is_about_false_rumor': log.get('is_about_false_rumor', 0),
                'page.is_checked': 1,
                'page.is_hidden': log.get('is_hidden', 0),
                'page.displayed_country': log['new_country'],
                'page.topics': {new_topic: 1.0 for new_topic in log['new_topics']}
            }}
        )

    logger.debug('Tweet a useful new page.')
    if do_tweet:
        if not maybe_tweeted_ds:
            logger.debug('No such pages. Skip to tweet a page.')
            return
        d = random.choice(maybe_tweeted_ds)
        text = twitter_handler.create_text(d)
        twitter_handler.post(text)

    logger.debug('Add tweets posted in the last 2 days.')
    data_path = cfg['data']['tweet_list']

    def add_tweets(dt):
        buf = []
        glob_pat = f'*/orig/{dt.strftime("%Y")}/{dt.strftime("%m")}/{dt.strftime("%d")}/*/*.json'
        for path in pathlib.Path(data_path).glob(glob_pat):
            with path.open() as f:
                raw_data = json.load(f)

            meta_path = path.parent.joinpath(f'{path.stem}.metadata')
            with meta_path.open() as f:
                meta_data = json.load(f)

            ja_path = pathlib.Path(str(path).replace('orig', 'ja_translated').replace('.json', '.txt'))
            ja_translated_data = ''
            if ja_path.exists():
                with ja_path.open() as f:
                    ja_translated_data = f.read().strip()

            en_path = pathlib.Path(str(path).replace('orig', 'en_translated').replace('.json', '.txt'))
            en_translated_data = ''
            if en_path.exists():
                with en_path.open() as f:
                    en_translated_data = f.read().strip()

            buf.append(Tweet(
                _id=raw_data['id_str'],
                name=raw_data['user']['name'],
                verified=raw_data['user']['verified'],
                username=raw_data['user']['screen_name'],
                avatar=raw_data['user']['profile_image_url'],
                timestamp=datetime.strptime(raw_data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                                  .strftime('%Y-%m-%d %H:%M:%S'),
                simpleTimestamp=datetime.strptime(raw_data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                                        .strftime('%Y-%m-%d'),
                contentOrig=raw_data.get('full_text', '') or raw_data['text'],
                contentJaTrans=ja_translated_data,
                contentEnTrans=en_translated_data,
                retweetCount=meta_data['count'],
                country=meta_data['country_code'].lower() if meta_data['country_code'] else 'unk',
                lang=raw_data['lang']
            ))

            if len(buf) == 1000:
                logger.debug('Write 1000 tweets.')
                _ = db_handler.upsert_tweets(buf)
                buf = []

        if buf:
            _ = db_handler.upsert_tweets(buf)

    add_tweets(datetime.today())
    add_tweets(datetime.today() - timedelta(1))


def update_stats():
    logger.debug('Update stats.')
    base = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data' \
           '/csse_covid_19_time_series/ '
    death_url = urllib.parse.urljoin(base, 'time_series_covid19_deaths_global.csv')
    confirmation_url = urllib.parse.urljoin(base, 'time_series_covid19_confirmed_global.csv')

    def fetch_data(url):
        with urllib.request.urlopen(url) as response:
            with tempfile.NamedTemporaryFile() as tmp_file:
                shutil.copyfileobj(response, tmp_file)
                return pd.read_csv(tmp_file.name)

    def get_last_update(df):
        return df.columns[-1]

    def get_stats(df, accessors):
        if 'all' in accessors:
            tmp_df = df.copy()
        else:
            tmp_df = df[df['Country/Region'].isin(accessors)]
        total = int(tmp_df.iloc[:, -1].sum())
        today = total - int(tmp_df.iloc[:, -2].sum())
        return total, today

    death_df = fetch_data(death_url)
    confirmation_df = fetch_data(confirmation_url)

    last_update = get_last_update(death_df)

    stats = {}
    for country in COUNTRIES:
        death_total, death_today = get_stats(death_df, country['dataRepository'])
        confirmation_total, confirmation_today = get_stats(confirmation_df, country['dataRepository'])
        stats[country['country']] = {
            'death_total': death_total,
            'confirmation_total': confirmation_total,
            'death_today': death_today,
            'confirmation_today': confirmation_today
        }

    meta_data_handler.set_stats({'last_updated': last_update, 'stats': stats})


def update_sources():
    logger.debug('Update sources.')
    data_path = cfg['data']['site_list']
    with open(data_path) as f:
        d = json.load(f)

    sources = collections.defaultdict(list)
    for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items():
        if ecountry == 'all':
            continue
        for domain, domain_info in d['domains'].items():
            if domain_info['region'] in icountries:
                for source in domain_info['sources']:
                    sources[ecountry].append(f'http://{source}')

    meta_data_handler.set_sources(sources)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--update_all', action='store_true', help='If true, update everything.')
    parser.add_argument('--update_database', action='store_true', help='If true, update the database.')
    parser.add_argument('--update_stats', action='store_true', help='If true, update the stats information.')
    parser.add_argument('--update_sources', action='store_true', help='If true, update the source information.')
    parser.add_argument('--do_tweet', action='store_true', help='If true, randomly tweet a newly registered page.')
    args = parser.parse_args()

    logging.basicConfig(level='DEBUG')

    if args.update_all or args.update_database:
        update_database(do_tweet=args.do_tweet)

    if args.update_all or args.update_stats:
        update_stats()

    if args.update_all or args.update_sources:
        update_sources()


if __name__ == '__main__':
    main()
