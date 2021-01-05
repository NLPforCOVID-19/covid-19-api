"""An API server for covid-19-ui."""
from datetime import datetime

from flask import Flask, request, jsonify
from flask_cors import CORS
from mojimoji import han_to_zen

from db_handler import DBHandler
from log_handler import LogHandler
from meta_data_handler import MetaDataHandler
from slack_handler import SlackHandler
from util import load_config


class InvalidUsage(Exception):

    status_code = 400

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


class InvalidPassword(Exception):

    status_code = 403

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


cfg = load_config()

app = Flask(__name__)
CORS(app, **cfg['cors'])


@app.route('/')
def index():
    return jsonify({})


def get_start() -> int:
    start = request.args.get('start', '0')  # NOTE: set the default value as a string object.
    if not start.isdecimal():
        raise InvalidUsage('Parameter `start` must be an integer.')
    return int(start)


def get_limit() -> int:
    limit = request.args.get('limit', '10')  # NOTE: set the default value as a string object.
    if not limit.isdecimal():
        raise InvalidUsage('Parameter `limit` must be an integer.')
    return int(limit)


def get_lang() -> str:
    lang = request.args.get('lang', 'ja')
    if lang not in {'ja', 'en'}:
        raise InvalidUsage('Allowed languages are `ja` and `en`.')
    return lang


def get_query() -> str:
    return request.args.get('query', '')


@app.route('/classes')
@app.route('/classes/<class_>')
@app.route('/classes/<class_>/<country>')
def classes(class_=None, country=None):
    db_handler = DBHandler(**cfg['db_handler'])
    return jsonify(db_handler.classes(class_, country, get_start(), get_limit(), get_lang(), get_query()))


@app.route('/countries')
@app.route('/countries/<country>')
@app.route('/countries/<country>/<class_>')
def countries(country=None, class_=None):
    db_handler = DBHandler(**cfg['db_handler'])
    return jsonify(db_handler.countries(country, class_, get_start(), get_limit(), get_lang()))


@app.route('/update', methods=['POST'])
def update():
    data = request.get_json()

    if data.get('password') != cfg['password']:
        raise InvalidPassword('The password is not correct')

    db_handler = DBHandler(**cfg['db_handler'])
    return jsonify(db_handler.update_page(
        url=data.get('url'),
        is_hidden=data.get('is_hidden'),
        is_about_covid_19=data.get('is_about_COVID-19'),
        is_useful=data.get('is_useful'),
        is_about_false_rumor=data.get('is_about_false_rumor'),
        icountry=data.get('new_displayed_country'),
        etopics=data.get('new_classes'),
        notes=han_to_zen(str(data.get('notes'))),
    ))


@app.route('/history', methods=['GET'])
def history():
    log_handler = LogHandler(**cfg['log_handler'])
    return jsonify(log_handler.find_topic_check_log(url=request.args.get('url')))


@app.route('/feedback', methods=['POST'])
def feedback():
    data = request.get_json()
    feedback_content = data.get('content', '')
    if feedback_content == '':
        raise InvalidUsage('Feedback content is empty.')
    if len(feedback_content) > 1000:
        raise InvalidUsage('Feedback content is too long.')

    slack_handlers = [SlackHandler(**args) for args in cfg['slack_handlers']]
    for slack_handler in slack_handlers:
        slack_handler.post(feedback_content)

    log_handler = LogHandler(**cfg['log_handler'])
    log_handler.extend_feedback_log([f'{datetime.today()}\t{feedback_content}'])

    return jsonify({})


@app.route('/meta')
def meta():
    meta_data_handler = MetaDataHandler()
    return jsonify(meta_data_handler.get(get_lang()))


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
