import os
import json
import itertools

SCORE_THRESHOLD = 0.7
RUMOR_THRESHOLD = 0.92
USEFUL_THRESHOLD = 0.9
SENTIMENT_THRESHOLD = 0.9  # is_positive flag

TOPICS = [
    {"ja": "感染状況", "en": "Current state of infection"},
    {"ja": "予防・防疫・規制", "en": "Prevention and regulation"},
    {
        "ja": "症状・治療・ワクチンなど医療情報",
        "en": "Medical info such as symptoms, treatments, and vaccines",
    },
    {"ja": "経済・福祉政策", "en": "Economic and welfare policies"},
    {"ja": "教育関連", "en": "Education"},
    # {"ja": "オリンピック", "en": "Olympics and Paralympics"},
    {"ja": "その他", "en": "Other"},
]

COUNTRIES = [
    {
        "country": "jp",
        "name": {"ja": "日本", "en": "Japan"},
        "dataRepository": ["Japan"],
        "language": "ja",
        "representativeSiteUrl": "https://www.kantei.go.jp/jp/headline/kansensho/coronavirus.html",
    },
    {
        "country": "cn",
        "name": {"ja": "中国", "en": "China"},
        "dataRepository": ["China"],
        "language": "zh",
        "representativeSiteUrl": "http://www.gov.cn/fuwu/zt/yqfkzq/index.htm",
    },
    {
        "country": "us",
        "name": {"ja": "USA", "en": "USA"},
        "dataRepository": ["US"],
        "language": "en",
        "representativeSiteUrl": "https://www.cdc.gov/coronavirus/index.html",
    },
    {
        "country": "eur",
        "name": {"ja": "ヨーロッパ", "en": "Europe"},
        "dataRepository": [
            "Belgium",
            "Bulgaria",
            "Czechia",
            "Denmark",
            "Germany",
            "Estonia",
            "Ireland",
            "Greece",
            "Spain",
            "France",
            "Croatia",
            "Italy",
            "Cyprus",
            "Latvia",
            "Lithuania",
            "Luxembourg",
            "Hungary",
            "Malta",
            "Netherlands",
            "Austria",
            "Poland",
            "Portugal",
            "Romania",
            "Slovenia",
            "Slovakia",
            "Finland",
            "Sweden",
            "United Kingdom",
        ],
        "language": "en",
        "representativeSiteUrl": "https://www.ecdc.europa.eu/en/covid-19-pandemic",
    },
    {
        "country": "asia",
        "name": {"ja": "アジア (日本・中国以外)", "en": "Asia (other than Japan & China)"},
        "dataRepository": [
            "Indonesia",
            "India",
            "Korea, South",
            "Thailand",
            "Vietnam",
            "Singapore",
            "Philippines",
            "Malaysia",
            "Pakistan",
            "Iran",
            "Israel",
            "Mongolia",
            "Maldives",
            "Cambodia",
            "Saudi Arabia",
            "Nepal",
            "Bangladesh",
            "Afghanistan",
            "Sri Lanka",
            "Laos",
            "Uzbekistan",
            "Iraq",
            "Syria",
            "United Arab Emirates",
            "Armenia",
            "Lebanon",
            "Brunei",
            "Jordan",
            "Qatar",
            "Palestine",
            "Yemen",
            "Tajikistan",
            "Timor-Leste",
            "Bhutan",
            "Kuwait",
            "Oman",
            "Turkmenistan",
            "Kyrgyzstan",
            "Bahrain",
        ],
        "language": "en",
        "representativeSiteUrl": "#",
    },
    {
        "country": "sa",
        "name": {"ja": "アメリカ（USA以外）", "en": "America (other than USA)"},
        "dataRepository": [
            "Brazil",
            "Argentina",
            "Colombia",
            "Peru",
            "Chile",
            "Ecuador",
            "Bolivia",
            "Venezuela",
            "Guyana",
            "Uruguay",
            "Suriname",
            "Paraguay",
        ],
        "language": "en",
        "representativeSiteUrl": "#",
    },
    {
        "country": "oceania",
        "name": {"ja": "オセアニア", "en": "Oceania"},
        "dataRepository": ["New Zealand", "Australia", "Fiji", "Papua New Guinea"],
        "language": "en",
        "representativeSiteUrl": "#",
    },
    {
        "country": "africa",
        "name": {"ja": "アフリカ", "en": "Africa"},
        "dataRepository": [
            "South Africa",
            "Nigeria",
            "Kenya",
            "Ghana",
            "Ethiopia",
            "Congo (Brazzaville)",
            "Congo (Kinshasa",
            "Tanzania",
            "Morocco",
            "Senegal",
            "Mali",
            "Uganda",
            "Cote d'Ivoire",
            "Madagascar",
            "Angola",
            "Zimbabwe",
            "Sudan",
            "Cameron",
            "Zambia",
            "Algeria",
            "Somalia",
            "Libya",
            "Rwanda",
            "Namibia",
            "Niger",
            "Tunisia",
            "Mauritania",
            "Mozambique",
            "Central African Republic",
            "Botswana",
            "Guinea",
            "Togo",
            "Burkina Faso",
            "Benin",
            "Mauritius",
            "Gambia",
            "Djibouti",
            "Malawi",
            "Eritrea",
            "Chad",
            "Gabon",
            "Western Sahara",
            "Seychelles",
            "South Sudan",
            "Sierra Leone",
            "Eswatini",
            "Lesotho",
            "Burundi",
            "Equatorial Guinea",
        ],
        "language": "en",
        "representativeSiteUrl": "#",
    },
    {
        "country": "int",
        "name": {"ja": "一般", "en": "General"},
        "dataRepository": ["all"],
        "language": "en",
        "representativeSiteUrl": "https://www.who.int/emergencies/diseases/novel-coronavirus-2019",
    },
]

# external country -> internal countries
ECOUNTRY_ICOUNTRIES_MAP = {
    "jp": ["jp"],
    "cn": ["cn"],
    "us": ["us", "us_other"],
    "eur": ["eur", "eu", "fr", "gb", "es", "de", "it", "ru", "eur_other"],
    "asia": ["asia", "kr", "id", "in", "np", "my", "sg", "af", "asia_other"],
    "sa": ["sa", "br", "mx", "sa_other"],
    "oceania": ["au", "oceania_other"],
    "africa": ["za", "africa_other"],
    "int": ["int", "int_other"],
}

# internal country -> external country
ICOUNTRY_ECOUNTRY_MAP = {
    icountry: ecountry
    for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items()
    for icountry in icountries
}

# external countries
ECOUNTRIES = list(ECOUNTRY_ICOUNTRIES_MAP.keys())

# internal countries
ICOUNTRIES = list(itertools.chain(*ECOUNTRY_ICOUNTRIES_MAP.values()))

# add a special country 'all'
ECOUNTRY_ICOUNTRIES_MAP["all"] = ICOUNTRIES

# external topic -> internal topics
ETOPIC_ITOPICS_MAP = {
    "感染状況": ["感染状況"],
    "予防・防疫・規制": ["予防・緊急事態宣言"],
    "症状・治療・ワクチンなど医療情報": ["症状・治療・検査など医療情報"],
    "経済・福祉政策": ["経済・福祉政策"],
    "教育関連": ["休校・オンライン授業"],
    # "オリンピック": ["オリンピック"],
    "その他": ["その他", "芸能・スポーツ"],
}

# internal topic -> external topic
ITOPIC_ETOPIC_MAP = {
    itopic: etopic
    for etopic, itopics in ETOPIC_ITOPICS_MAP.items()
    for itopic in itopics
}

# external topics
ETOPICS = list(ETOPIC_ITOPICS_MAP.keys())

# internal topics
ITOPICS = list(itertools.chain(*ETOPIC_ITOPICS_MAP.values()))

# external topics
ETOPIC_ITOPICS_MAP["all"] = ITOPICS

# external topic + language -> external topic in the language
LANGUAGES = ("ja", "en")
ETOPIC_TRANS_MAP = {
    (etopic, lang): topic[lang]
    for lang in LANGUAGES
    for topic in TOPICS
    for etopic in topic.values()
}

# external country _ language -> external country in the language
ECOUNTRY_TRANS_MAP = {
    (ecountry, lang): country["name"][lang]
    for lang in LANGUAGES
    for country in COUNTRIES
    for ecountry in country["name"].items()
}


def load_config():
    here = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(here, "config.json")
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)
