import os
import json
import itertools

# external country -> internal countries
ECOUNTRY_ICOUNTRIES_MAP = {
    "jp": ["jp"],
    "cn": ["cn"],
    "us": ["us"],
    "eur": ["eur", "eu", "fr", "es", "de"],
    "asia": ["asia", "kr", "in"],
    "sa": ["sa", "br"],
    "int": ["int"]
}

# internal country -> external country
ICOUNTRY_ECOUNTRY_MAP = {
    icountry: ecountry for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items() for icountry in icountries
}

# external countries
ECOUNTRIES = list(ECOUNTRY_ICOUNTRIES_MAP.keys())

# internal countries
ICOUNTRIES = list(itertools.chain(*ECOUNTRY_ICOUNTRIES_MAP.values()))

# add a special country "all"
ECOUNTRY_ICOUNTRIES_MAP["all"] = ICOUNTRIES

# external topic -> internal topics
ETOPIC_ITOPICS_MAP = {
    "感染状況": ["感染状況"],
    "予防・防疫・緩和": ["予防・緊急事態宣言"],
    "症状・治療・検査など医療情報": ["症状・治療・検査など医療情報"],
    "経済・福祉政策": ["経済・福祉政策"],
    "教育関連": ["休校・オンライン授業"],
    "その他": ["その他", "芸能・スポーツ"]
}

# internal topic -> external topic
ITOPIC_ETOPIC_MAP = {
    itopic: etopic for etopic, itopics in ETOPIC_ITOPICS_MAP.items() for itopic in itopics
}

# external topics
ETOPICS = list(ETOPIC_ITOPICS_MAP.keys())

# internal topics
ITOPICS = list(itertools.chain(*ETOPIC_ITOPICS_MAP.values()))

# external topics
ETOPIC_ITOPICS_MAP["all"] = ITOPICS


here = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(here, "data", f"meta.json")) as f:
    meta_info = json.load(f)

# external topic + language -> external topic in the language
LANGUAGES = ("ja", "en")
topics = meta_info["topics"]
ETOPIC_TRANS_MAP = {
    (etopic, lang): topic[lang]
    for lang in LANGUAGES
    for topic in topics
    for etopic in topic.values()
}

# external country _ language -> external country in the language
countries = meta_info["countries"]
ECOUNTRY_TRANS_MAP = {
    (ecountry, lang): country["name"][lang]
    for lang in LANGUAGES
    for country in countries
    for ecountry in country["name"].items()
}
