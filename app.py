"""An API server for covid-19-ui."""
import json
import os

from mojimoji import han_to_zen
from flask import Flask, request, jsonify
from flask_cors import CORS

from util import load_config
from database import DBHandler

here = os.path.dirname(os.path.abspath(__file__))
cfg = load_config()

app = Flask(__name__)
CORS(app, origins=cfg["access_control_allow_origin"])

mongo = DBHandler(
    host=cfg['database']['host'],
    port=cfg['database']['port'],
    db_name=cfg['database']['db_name'],
    collection_name=cfg['database']['collection_name'],
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


def get_start() -> int:
    start = request.args.get("start", "0")  # NOTE: set the default value as a `str` object
    if start.isdecimal():
        start = int(start)
    else:
        raise InvalidUsage('Parameter `start` must be integers')
    return start


def get_limit() -> int:
    limit = request.args.get("limit", "10")  # NOTE: set the default value as a `str` object
    if limit.isdecimal():
        limit = int(limit)
    else:
        raise InvalidUsage('Parameter `limit` must be integers')
    return limit


def get_lang() -> str:
    lang = request.args.get("lang", "ja")
    if lang not in {"ja", "en"}:
        raise InvalidUsage('Allowed languages are `ja` and `en`')
    return lang


@app.route('/classes')
@app.route('/classes/<class_>')
@app.route('/classes/<class_>/<country>')
def classes(class_=None, country=None):
    return jsonify(mongo.classes(class_, country, start=get_start(), limit=get_limit(), lang=get_lang()))


@app.route('/countries')
@app.route('/countries/<country>')
@app.route('/countries/<country>/<class_>')
def countries(country=None, class_=None):
    return jsonify(mongo.countries(country, class_, start=get_start(), limit=get_limit(), lang=get_lang()))


@app.route('/update', methods=["POST"])
def update():
    data = request.get_json()
    password = data.get('password')
    if password == cfg['password']:
        url = data.get('url')
        is_about_covid_19 = data.get('is_about_COVID-19')
        is_useful = data.get('is_useful')
        is_about_false_rumor = data.get('is_about_false_rumor')
        country = data.get('new_displayed_country')
        classes_ = data.get('new_classes')
        notes = han_to_zen(str(data.get('notes')))
        updated = mongo.update_page(url=url,
                                    is_about_covid_19=is_about_covid_19,
                                    is_useful=is_useful,
                                    is_about_false_rumor=is_about_false_rumor,
                                    icountry=country,
                                    etopics=classes_,
                                    notes=notes,
                                    category_check_log_path=cfg['database']['category_check_log_path'])
        return jsonify(updated)
    else:
        raise InvalidPassword('The password is not correct')


@app.route('/history', methods=["GET"])
def history():
    url = request.args.get('url')
    with open(cfg['database']['category_check_log_path'], mode='r') as f:
        for line in f.readlines()[::-1]:
            if line.strip():
                edited_info = json.loads(line.strip())
                if edited_info.get('url', '') == url:
                    edited_info['is_checked'] = 1
                    return jsonify(edited_info)

    return jsonify({"url": url, "is_checked": 0})


@app.route('/meta')
def meta():
    lang = get_lang()
    with open(os.path.join(here, "data", f"meta.json")) as f:
        meta_info = json.load(f)

    def reshape_country(country):
        return {
            "country": country["country"],
            "name": country["name"][lang],
            "language": country["language"],
            "representativeSiteUrl": country["representativeSiteUrl"]
        }

    meta_info = {
        "topics": [topic[lang] for topic in meta_info["topics"]],
        "countries": [reshape_country(country) for country in meta_info["countries"]]
    }

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
