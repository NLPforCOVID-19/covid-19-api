import os
import json

from dotenv import load_dotenv

load_dotenv()

config = {
    'activator': os.getenv('ACTIVATOR'),
    'password': os.getenv('PASSWORD'),
    'log_handler': {
        'log_dir': os.getenv('LOG_HANDLER_LOG_DIR'),
    },
    'cors': {
        'origins': os.getenv('CORS_ORIGINS'),
    },
    'db_handler': {
        'mongo_host': os.getenv('DB_HANDLER_MONGO_HOST'),
        'mongo_port': int(os.getenv('DB_HANDLER_MONGO_PORT')),
        'mongo_db_name': os.getenv('DB_HANDLER_MONGO_DB_NAME'),
        'mongo_collection_name': os.getenv('DB_HANDLER_MONGO_COLLECTION_NAME'),
        'es_host': os.getenv('DB_HANDLER_ES_HOST'),
        'es_port': int(os.getenv('DB_HANDLER_ES_PORT'))
    },
    'twitter_handler': {
        'token': os.getenv('TWITTER_HANDLER_OAUTH_TOKEN'),
        'token_secret': os.getenv('TWITTER_HANDLER_OAUTH_TOKEN_SECRET'),
        'consumer_key': os.getenv('TWITTER_HANDLER_OAUTH_CONSUMER_KEY'),
        'consumer_secret': os.getenv('TWITTER_HANDLER_OAUTH_CONSUMER_SECRET'),
    },
    'slack_handlers': [
        {
            'access_token': access_token,
            'app_channel': app_channel
        }
        for access_token, app_channel
        in zip(os.getenv('SLACK_HANDLER_ACCESS_TOKENS').split(), os.getenv('SLACK_HANDLER_APP_CHANNELS').split())
    ],
    'data': {
        'article_list': os.getenv('ORIGINAL_DATA_HANDLER_ARTICLE_LIST'),
        'site_list': os.getenv('ORIGINAL_DATA_HANDLER_SITE_LIST')
    }
}

with open('config.json', 'w') as f:
    json.dump(config, f, ensure_ascii=False, indent=4)
