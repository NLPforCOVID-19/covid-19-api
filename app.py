from datetime import datetime

from flask import Flask, request, jsonify
from flask_cors import CORS
from mojimoji import han_to_zen

from constants import TOPICS, COUNTRIES
from handlers import DBHandler, MetaDataHandler, SlackHandler, LogHandler
from util import load_config


class APIError(Exception):

    status_code = None

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


class InvalidUsage(APIError):

    status_code = 400


class InvalidPassword(APIError):

    status_code = 403


cfg = load_config()

meta_data_handler = MetaDataHandler()
log_handler = LogHandler(**cfg['log_handler'])
slack_handlers = [SlackHandler(**args) for args in cfg['slack_handlers']]
db_handler = DBHandler(**cfg['db_handler'])

app = Flask(__name__)
CORS(app, **cfg['cors'])


def get_start() -> int:
    start = request.args.get('start', '0')  # NOTE: set the default value as a string.
    if not start.isdecimal():
        raise InvalidUsage('Parameter \'start\' must be an integer.')
    return int(start)


def get_limit() -> int:
    limit = request.args.get('limit', '10')  # NOTE: set the default value as a string.
    if not limit.isdecimal():
        raise InvalidUsage('Parameter \'limit\' must be an integer.')
    return int(limit)


def get_lang() -> str:
    lang = request.args.get('lang', 'ja')
    if lang not in {'ja', 'en'}:
        raise InvalidUsage('Allowed languages are \'ja\' and \'en.\'')
    return lang


def get_query() -> str:
    return request.args.get('query', '')


@app.route('/classes')
@app.route('/classes/<class_>')
@app.route('/classes/<class_>/<country>')
def classes(class_=None, country=None):
    return jsonify(db_handler.classes(class_, country, get_start(), get_limit(), get_lang(), get_query()))


@app.route('/countries')
@app.route('/countries/<country>')
@app.route('/countries/<country>/<class_>')
def countries(country=None, class_=None):
    return jsonify(db_handler.countries(country, class_, get_start(), get_limit(), get_lang()))


@app.route('/update', methods=['POST'])
def update():
    data = request.get_json()

    if data.get('password') != cfg['password']:
        raise InvalidPassword('Invalid password')

    return jsonify(db_handler.update_page(
        url=data.get('url'),
        is_hidden=data.get('is_hidden'),
        is_about_covid_19=data.get('is_about_COVID-19'),
        is_useful=data.get('is_useful'),
        is_about_false_rumor=data.get('is_about_false_rumor'),
        icountry=data.get('new_displayed_country'),
        etopics=data.get('new_classes'),
        notes=han_to_zen(str(data.get('notes')))
    ))


@app.route('/history', methods=['GET'])
def history():
    return jsonify(log_handler.find_category_check_log(url=request.args.get('url')))


@app.route('/feedback', methods=['POST'])
def feedback():
    data = request.get_json()
    feedback_content = data.get('content', '')
    if feedback_content == '':
        raise InvalidUsage('Feedback content is empty.')
    if len(feedback_content) > 1000:
        raise InvalidUsage('Feedback content is too long.')
    for slack_handler in slack_handlers:
        slack_handler.post(feedback_content)
    log_handler.extend_feedback_log([f'{datetime.today()}\t{feedback_content}'])
    return jsonify({})


@app.route('/meta')
def meta():
    meta_info = {'topics': TOPICS, 'countries': COUNTRIES}
    stats = meta_data_handler.read_stats()
    stats = stats['stats']
    sources = meta_data_handler.read_sources()
    country_code_index_map = {country['code']: i for i, country in enumerate(meta_info['countries'])}
    for country_code in stats:
        meta_info['countries'][country_code_index_map[country_code]]['stats'] = stats[country_code]
        meta_info['countries'][country_code_index_map[country_code]]['sources'] = sources[country_code]
    return jsonify(meta_info)


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.errorhandler(InvalidPassword)
def handle_invalid_password(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response
