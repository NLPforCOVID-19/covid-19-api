#
# Country
#
COUNTRIES = [
    {
        'code': 'jp',
        'name': {
            'ja': '日本',
            'en': 'Japan'
        },
        'dataRepository': ['Japan'],
        'representativeSiteUrl': 'https://www.kantei.go.jp/jp/headline/kansensho/coronavirus.html'
    },
    {
        'code': 'cn',
        'name': {
            'ja': '中国',
            'en': 'China'
        },
        'dataRepository': ['China'],
        'representativeSiteUrl': 'http://www.gov.cn/fuwu/zt/yqfkzq/index.htm'
    },
    {
        'code': 'us',
        'name': {
            'ja': 'アメリカ',
            'en': 'United States'
        },
        'dataRepository': ['US'],
        'representativeSiteUrl': 'https://www.cdc.gov/coronavirus/index.html'
    },
    {
        'code': 'eur',
        'name': {
            'ja': 'ヨーロッパ',
            'en': 'Europe'
        },
        'dataRepository': ['Belgium', 'Bulgaria', 'Czechia', 'Denmark', 'Germany', 'Estonia', 'Ireland', 'Greece',
                           'Spain', 'France', 'Croatia', 'Italy', 'Cyprus', 'Latvia', 'Lithuania', 'Luxembourg',
                           'Hungary', 'Malta', 'Netherlands', 'Austria', 'Poland', 'Portugal', 'Romania', 'Slovenia',
                           'Slovakia', 'Finland', 'Sweden'],
        'representativeSiteUrl': 'https://www.ecdc.europa.eu/en/covid-19-pandemic'
    },
    {
        'code': 'asia',
        'name': {
            'ja': 'アジア (日本・中国を除く)',
            'en': 'Asia (other than Japan & China)'
        },
        'dataRepository': ['Indonesia', 'India', 'Korea, South', 'Thailand', 'Vietnam', 'Singapore', 'Philippines',
                           'Malaysia', 'Pakistan', 'Iran', 'Israel', 'Mongolia', 'Maldives', 'Cambodia', 'Saudi Arabia',
                           'Nepal', 'Bangladesh', 'Afghanistan', 'Sri Lanka', 'Laos', 'Uzbekistan', 'Iraq', 'Syria',
                           'United Arab Emirates', 'Armenia', 'Lebanon', 'Brunei', 'Jordan', 'Qatar', 'Palestine',
                           'Yemen', 'Tajikistan', 'Timor-Leste', 'Bhutan', 'Kuwait', 'Oman', 'Turkmenistan',
                           'Kyrgyzstan', 'Bahrain'],
        'representativeSiteUrl': '#'
    },
    {
        'code': 'sa',
        'name': {
            'ja': '南アメリカ',
            'en': 'South America'
        },
        'dataRepository': ['Brazil', 'Argentina', 'Colombia', 'Peru', 'Chile', 'Ecuador', 'Bolivia', 'Venezuela',
                           'Guyana', 'Uruguay', 'Suriname', 'Paraguay'],
        'representativeSiteUrl': '#'
    },
    {
        'code': 'oceania',
        'name': {
            'ja': 'オセアニア',
            'en': 'Oceania'
        },
        'dataRepository': ['New Zealand', 'Australia', 'Fiji', 'Papua New Guinea'],
        'representativeSiteUrl': '#'
    },
    {
        'code': 'africa',
        'name': {
            'ja': 'アフリカ',
            'en': 'Africa'
        },
        'dataRepository': ['South Africa', 'Nigeria', 'Kenya', 'Ghana', 'Ethiopia', 'Congo (Brazzaville)',
                           'Congo (Kinshasa', 'Tanzania', 'Morocco', 'Senegal', 'Mali', 'Uganda', 'Cote d\'Ivoire',
                           'Madagascar', 'Angola', 'Zimbabwe', 'Sudan', 'Cameron', 'Zambia', 'Algeria', 'Somalia',
                           'Libya', 'Rwanda', 'Namibia', 'Niger', 'Tunisia', 'Mauritania', 'Mozambique',
                           'Central African Republic', 'Botswana', 'Guinea', 'Togo', 'Burkina Faso', 'Benin',
                           'Mauritius', 'Gambia', 'Djibouti', 'Malawi', 'Eritrea', 'Chad', 'Gabon', 'Western Sahara',
                           'Seychelles', 'South Sudan', 'Sierra Leone', 'Eswatini', 'Lesotho', 'Burundi',
                           'Equatorial Guinea'],
        'representativeSiteUrl': '#'
    },
    {
        'code': 'int',
        'dataRepository': ['all'],
        'representativeSiteUrl': 'https://www.who.int/emergencies/diseases/novel-coronavirus-2019'
    }
]

# A map from an external country to its corresponding internal countries
ECOUNTRY_ICOUNTRIES_MAP = {
    'jp': ['jp'],
    'cn': ['cn'],
    'us': ['us', 'us_other'],
    'eur': ['eur', 'eu', 'fr', 'es', 'de', 'eur_other'],
    'asia': ['asia', 'kr', 'in', 'np', 'my', 'sg', 'asia_other'],
    'sa': ['sa', 'br', 'sa_other'],
    'oceania': ['au', 'oceania_other'],
    'africa': ['za', 'africa_other'],
    'int': ['int', 'int_other']
}

# A map from an internal country to its corresponding external country
ICOUNTRY_ECOUNTRY_MAP = {
    icountry: ecountry
    for ecountry, icountries in ECOUNTRY_ICOUNTRIES_MAP.items()
    for icountry in icountries
}

# A list of external countries
ECOUNTRIES = [ecountry for ecountry in ECOUNTRY_ICOUNTRIES_MAP.keys()]

# A list of internal countries
ICOUNTRIES = [icountry for icountries in ECOUNTRY_ICOUNTRIES_MAP.values() for icountry in icountries]

# Add a special country 'all'
ECOUNTRY_ICOUNTRIES_MAP['all'] = ICOUNTRIES

#
# Topic
#
TOPICS = [
    {
        'code': 'currentStateOfInfection',
        'name': {
            'ja': '感染状況',
            'en': 'Current State of Infection'
        }
    },
    {
        'code': 'preventionAndMitigationMeasures',
        'name': {
            'ja': '予防・防疫・緩和',
            'en': 'Prevention and mitigation measures'
        }
    },
    {
        'code': 'SymptomsTreatmentsAndMedicalInformation',
        'name': {
            'ja': '症状・治療・検査など医療情報',
            'en': 'Symptoms, treatments, and medical information'
        }
    },
    {
        'code': 'EconomicAndWelfarePolicies',
        'name': {
            'ja': '経済・福祉政策',
            'en': 'Economic and welfare policies'
        }
    },
    {
        'code': 'Education',
        'name': {
            'ja': '教育関連',
            'en': 'Education'
        }
    },
    {
        'code': 'Other',
        'name': {
            'ja': 'その他',
            'en': 'Other'
        }
    }
]

# A map from an external topic to its corresponding internal topics
ETOPIC_ITOPICS_MAP = {
    'currentStateOfInfection': ['感染状況'],
    'preventionAndMitigationMeasures': ['予防・緊急事態宣言'],
    'SymptomsTreatmentsAndMedicalInformation': ['症状・治療・検査など医療情報'],
    'EconomicAndWelfarePolicies': ['経済・福祉政策'],
    'Education': ['休校・オンライン授業'],
    'Other': ['その他', '芸能・スポーツ']
}

# A map from an internal topic to its corresponding external topic
ITOPIC_ETOPIC_MAP = {
    itopic: etopic
    for etopic, itopics in ETOPIC_ITOPICS_MAP.items()
    for itopic in itopics
}

# A list of external topics
ETOPICS = [etopic for etopic in ETOPIC_ITOPICS_MAP.keys()]

# A list of internal topics
ITOPICS = [itopic for itopics in ETOPIC_ITOPICS_MAP.values() for itopic in itopics]

# Add a special country 'all'
ETOPIC_ITOPICS_MAP['all'] = ITOPICS
