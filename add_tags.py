import json
import glob

from util import load_config
from database import HandlingPages


def main():
    cfg = load_config()
    mongo = HandlingPages(host=cfg['database']['host'],
                          port=cfg['database']['port'],
                          db_name=cfg['database']['db_name'],
                          collection_name=cfg['database']['collection_name'])
    for input_path in glob.glob(f"{cfg['crowdsourcing']['result_dir']}/*.jsonl"):
        with open(input_path) as f:
            json_tags = [json.loads(line.strip()) for line in f]

        for json_tag in json_tags:
            mongo.collection.update_one(
                {'page.url': json_tag['url']},
                {'$set': {'page.tags': json_tag['tags']}}
            )


if __name__ == '__main__':
    main()
