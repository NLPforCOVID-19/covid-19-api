import json
import glob

from util import load_config
from database import HandlingPages

TAGS = ["is_about_COVID-19", "is_useful", "is_clear", "is_about_false_rumor"]


def main():
    cfg = load_config()
    mongo = HandlingPages(host=cfg["database"]["host"],
                          port=cfg["database"]["port"],
                          db_name=cfg["database"]["db_name"],
                          collection_name=cfg["database"]["collection_name"])
    for input_path in glob.glob(f'{cfg["crowdsourcing"]["result_dir"]}/*.jsonl'):
        with open(input_path) as f:
            json_tags = [json.loads(line.strip()) for line in f]

        for json_tag in json_tags:
            search_result = mongo.collection.find_one({"page.url": json_tag["url"]})
            if search_result:
                page = search_result["page"]
                for tag in TAGS:
                    page[tag] = json_tag["tags"][tag]

                new_topics = [topic for topic, has_topic in json_tag["tags"]["topics"].items() if has_topic]
                page["topics"] = new_topics

                old_snippets = page["snippets"]
                new_snippets = {}
                for new_topic in new_topics:
                    new_snippets[new_topic] = old_snippets[new_topic] if new_topic in old_snippets.keys() else ""

                mongo.collection.update_one(
                    {"page.url": json_tag["url"]},
                    {"$set": {"page": page}}
                )


if __name__ == "__main__":
    main()
