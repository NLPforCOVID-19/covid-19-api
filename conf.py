import os
import json

from dotenv import load_dotenv

load_dotenv()

config = {
    'activator': os.getenv('ACTIVATOR'),
    'access_control_allow_origin': '*',
    'database': {
        'input_page_path': os.getenv('DB_INPUT_PAGE_PATH'),
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
        'db_name': os.getenv('DB_DB_NAME'),
        'collection_name': os.getenv('DB_COLLECTION_NAME'),
        'log_path': os.getenv('DB_UPDATE_LOG_PATH'),
        'category_check_log_path': os.getenv('DB_CATEGORY_CHECK_LOG_PATH')
    },
    'es': {
        'host': os.getenv('ES_HOST'),
        'port': int(os.getenv('ES_PORT'))
    },
    'crowdsourcing': {
        'result_dir': os.getenv('CS_RESULT_DIR')
    },
    'feedback': {
        'slack': [
            {
                'access_token': access_token,
                'channel': channel,
            }
            for access_token, channel
            in zip(os.getenv('SLACK_ACCESS_TOKENS').split(' '), os.getenv('SLACK_APP_CHANNELS').split(' '))
        ],
        'feedback_log_file': os.getenv('FB_LOG_FILE')
    },
    'source': os.getenv('SOURCE'),
    'password': os.getenv('PASSWORD'),
    'webcite_url': os.getenv('WEBSITE_URL'),
    'twitter': {
        'api_key': os.getenv('TWITTER_API_KEY'),
        'api_secret_key': os.getenv('TWITTER_API_SECRET_KEY'),
        'token': os.getenv('TWITTER_TOKEN'),
        'secret_token': os.getenv('TWITTER_SECRET_TOKEN'),
    }
}

with open('config.json', 'w') as f:
    json.dump(config, f, ensure_ascii=False, indent=4)
