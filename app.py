"""An API server for covid-19-ui."""
import json
import os

from flask import Flask, request, jsonify
from flask_cors import CORS

from util import load_config
from database import DBHandler

here = os.path.dirname(os.path.abspath(__file__))
cfg = load_config()

app = Flask(__name__)
CORS(app, origins=cfg["access_control_allow_origin"])

with open(cfg["crowdsourcing"]["useful_white_list"], mode='r') as f:
    useful_white_list = [line.strip() for line in f.readlines()]
mongo = DBHandler(
    host=cfg['database']['host'],
    port=cfg['database']['port'],
    db_name=cfg['database']['db_name'],
    collection_name=cfg['database']['collection_name'],
    useful_white_list=useful_white_list
)


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


@app.route('/')
def index():
    return "it works"


@app.route('/classes')
@app.route('/classes/<class_>')
@app.route('/classes/<class_>/<country>')
def classes(class_=None, country=None):
    start = request.args.get("start", "0")  # NOTE: set the default value as a `str` object
    limit = request.args.get("limit", "10")  # NOTE: set the default value as a `str` object
    if start.isdecimal() and limit.isdecimal():
        start = int(start)
        limit = int(limit)
    else:
        raise InvalidUsage('Parameters `start` and `limit` must be integers')
    filtered_pages = mongo.classes(topic=class_, country=country, start=start, limit=limit)
    return jsonify(filtered_pages)


@app.route('/countries')
@app.route('/countries/<country>')
@app.route('/countries/<country>/<class_>')
def countries(country=None, class_=None):
    start = request.args.get("start", "0")  # NOTE: set the default value as a `str` object
    limit = request.args.get("limit", "10")  # NOTE: set the default value as a `str` object
    if start.isdecimal() and limit.isdecimal():
        start = int(start)
        limit = int(limit)
    else:
        raise InvalidUsage('Parameters `start` and `limit` must be integers')
    filtered_pages = mongo.countries(country=country, topic=class_, start=start, limit=limit)
    return jsonify(filtered_pages)


@app.route('/update', methods=["POST"])
def update():
    data = request.get_json()
    password = data.get('password')
    if password == cfg['password']:
        url = data.get('url')
        new_country = data.get('new_displayed_country')
        new_classes = data.get('new_classes')
        updated = mongo.update_page(url=url,
                                    new_country=new_country,
                                    new_topics=new_classes,
                                    category_check_log_path=cfg['database']['category_check_log_path'])
        return jsonify(updated)
    else:
        raise InvalidPassword('The password is not correct')


@app.route('/meta')
def meta():
    with open(os.path.join(here, "data", "meta.json")) as f:
        meta_info = json.load(f)

    with open(os.path.join(here, "data", "stats.json")) as f:
        stats_info = json.load(f)["stats"]

    with open(os.path.join(here, "data", "sources.json")) as f:
        sources_info = json.load(f)

    country_code_index_map = {country["country"]: i for i, country in enumerate(meta_info["countries"])}
    for country_code in stats_info:
        meta_info["countries"][country_code_index_map[country_code]]["stats"] = stats_info[country_code]
        meta_info["countries"][country_code_index_map[country_code]]["sources"] = sources_info[country_code]

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
