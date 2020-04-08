"""An API server for covid-19-ui."""
import json
import os

from flask import Flask, request, jsonify
from flask_cors import CORS

from util import load_config
from database import HandlingPages

here = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)
CORS(app)

cfg = load_config()
mongo = HandlingPages(host=cfg['database']['host'],
                      port=cfg['database']['port'],
                      db_name=cfg['database']['db_name'],
                      collection_name=cfg['database']['collection_name'])


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


@app.route('/')
def index():
    return "it works"


@app.route('/classes')
@app.route('/classes/<class_>')
@app.route('/classes/<class_>/<country>')
def classes(class_=None, country=None):
    # load GET parameters
    start = request.args.get("start", "0")  # NOTE: set the default value as a `str` object
    limit = request.args.get("limit", "10")  # NOTE: set the default value as a `str` object
    if start.isdecimal() and limit.isdecimal():
        start = int(start)
        limit = int(limit)
    else:
        raise InvalidUsage('Parameters `start` and `limit` must be integers')

    filtered_pages = mongo.get_filtered_pages(class_=class_, country=country, start=start, limit=limit)
    return jsonify(filtered_pages)


@app.route('/meta')
def meta():
    with open(os.path.join(here, "data", "meta.json")) as f:
        return jsonify(json.load(f))


@app.route('/stats')
def stats():
    with open(os.path.join(here, "data", "stats.json")) as f:
        return jsonify(json.load(f))


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response
