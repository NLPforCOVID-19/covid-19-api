# COVID-19-API for [COVID-19-UI]((https://github.com/NLPforCOVID-19/covid-19-ui))

![Python Style Checker](https://github.com/NLPforCOVID-19/covid-19-api/workflows/Python%20Style%20Checker/badge.svg?branch=staging)
![Deployment](https://github.com/NLPforCOVID-19/covid-19-api/workflows/Deployment/badge.svg)

## User Guides

### [GET] /meta

- Parameters
    - lang: string
- Returns
    - application/json
- Example value

```json
{
  "countries": [
    {
      "country": "jp",
      "language": "ja",
      "name": "日本",
      "representativeSiteUrl": "https://www.kantei.go.jp/jp/headline/kansensho/coronavirus.html",
      "sources": [
        "http://fij.info",
        "http://yahoo.co.jp",
        "http://kantei.co.jp",
        "http://mhlw.go.cp",
        "http://niid.go.jp"
      ],
      "stats": {
        "confirmation_today": 3196,
        "confirmation_total": 245293,
        "death_today": 60,
        "death_total": 3429
      }
    },
    {
      "country": "cn",
      "language": "zh",
      "name": "中国",
      "representativeSiteUrl": "http://www.gov.cn/fuwu/zt/yqfkzq/index.htm",
      "sources": [
        "http://ifeng.com",
        "http://gov.cn"
      ],
      "stats": {
        "confirmation_today": 74,
        "confirmation_total": 96160,
        "death_today": 0,
        "death_total": 4784
      }
    }
  ],
  "topics": [
    "感染状況",
    "予防・防疫・緩和"
  ]
}
```

### [GET] /articles/topic

Return articles sorted by topics.

- Parameters
    - lang: string ('ja' or 'en')
    - start: string (must be able to casted to an integer)
    - limit: string (must be able to casted to an integer)
    - query: string (required when specifying `search` as `<topic>`)
- Returns
    - application/json
- Example value

```json
{
  "感染状況": {
    "jp": [
      "<article-information>",
      "<article-information>"
    ],
    "cn": [
      "<article-information>",
      "<article-information>"
    ]
  },
  "予防・防疫・緩和": {
    "jp": [
      "<article-information>",
      "<article-information>"
    ],
    "cn": [
      "<article-information>",
      "<article-information>"
    ]
  }
}
```

One `<article-information>` is like:

- Example value

```json
{
  "country": "jp",
  "displayed_country": "jp",
  "domain": "example.jp",
  "domain_label": "domain label",
  "is_about_COVID-19": 1,
  "is_about_false_rumor": 0,
  "is_checked": 1,
  "is_clear": 1,
  "is_hidden": 0,
  "is_useful": 1,
  "orig": {
    "simple_timestamp": "timestamp",
    "timestamp": "timestamp",
    "title": "title"
  },
  "topics": [
    {
      "name": "感染状況",
      "relatedness": 1.0,
      "snippet": "重症者数"
    }
  ],
  "translated": {
    "timestamp": "timestamp",
    "title": "title"
  },
  "url": "example.com"
}
```

### [GET] /articles/topic/\<topic\>

`<topic>` must be an item in the topics in the meta-data or `all` or `search`. When using `search`, specify the `query` parameter.

- Example value

```json
{
  "jp": [
    "<article-information>",
    "<article-information>"
  ],
  "cn": [
    "<article-information>",
    "<article-information>"
  ]
}
```

### [GET] /articles/topic/\<topic\>/\<country\>

`<country>` must be an item in the countries in the meta-data or `all`.

- Example value

```json
[
  "<article-information>",
  "<article-information>"
]
```

### [GET] /articles/country
### [GET] /articles/country/\<country\>
### [GET] /articles/country/\<country\>/\<topic\>

### [GET] /tweets/topic

Return tweets sorted by topics. **No topic information is available at the moment. Only special topics, `all` and `search`, are valid.**

- Parameters
    - lang: string ('ja' or 'en')
    - start: string (must be able to casted to an integer)
    - limit: string (must be able to casted to an integer)
    - query: string (required when specifying `search` as `<topic>`)
- Returns
    - application/json
- Example value

```json
{
  "all": {
    "jp": [
      "<tweet-information>",
      "<tweet-information>"
    ],
    "cn": [
      "<tweet-information>",
      "<tweet-information>"
    ]
  }
}
```

One `<tweet-information>` is like:

```json
{
  "avatar": "example.jpg",
  "contentOrig": "example text",
  "contentTrans": "translated example text",
  "id": 12345678901234567890,
  "name": "name",
  "timestamp": "timestamp",
  "username": "screen bane",
  "verified": true
}
```

### [GET] /tweets/topic/\<topic\>

- Example value

```json
{
  "jp": [
    "<tweet-information>",
    "<tweet-information>"
  ],
  "cn": [
    "<tweet-information>",
    "<tweet-information>"
  ]
}
```

### [GET] /tweets/topic/\<topic\>/\<country\>

- Example value

```json
[
  "<tweet-information>",
  "<tweet-information>"
]
```

### [GET] /tweets/country
### [GET] /tweets/country/\<country\>
### [GET] /tweets/country/\<country\>/\<topic\>

## Developer Guides

### Setup

#### Python

Install the dependencies using [poetry](https://python-poetry.org/).

```
$ poetry install
```

To activate the created virtual environment, run:

```
$ poetry shell
```

#### MongoDB

This project uses [MongoDB](https://www.mongodb.com/) to store article information.
To install MongoDB, follow [the official guide](https://docs.mongodb.com/manual/installation/).

Then start a `mongod` process.
To run it as a daemon, use the `--fork` option.

```
$ sudo mongod --dbpath <dbpath> --logpath <logpath> --port <port> [--fork]
```

#### ElasticSearch

This project uses [ElasticSearch](https://www.elastic.co/) to enable search.
The setup instruction will soon be written.

### Configuration

Run `python conf.py`.
Before running this script, set the following environment variables.

```dotenv
# Password
PASSWORD=""

# CORS
CORS_ORIGINS="*"

# LogHandler
LOG_HANDLER_LOG_DIR=""

# DBHandler
DB_HANDLER_MONGO_HOST=""
DB_HANDLER_MONGO_PORT=""
DB_HANDLER_MONGO_DB_NAME=""
DB_HANDLER_MONGO_ARTICLE_COLLECTION_NAME=""
DB_HANDLER_MONGO_TWEET_COLLECTION_NAME=""
DB_HANDLER_ES_HOST=""
DB_HANDLER_ES_PORT=""

# TwitterHandler
TWITTER_HANDLER_OAUTH_TOKEN=""
TWITTER_HANDLER_OAUTH_TOKEN_SECRET=""
TWITTER_HANDLER_OAUTH_CONSUMER_KEY=""
TWITTER_HANDLER_OAUTH_CONSUMER_SECRET=""

# SlackHandler (tokens/channels are separated by white spaces)
SLACK_HANDLER_ACCESS_TOKENS=""
SLACK_HANDLER_APP_CHANNELS=""

# Data
ARTICLE_LIST=""
TWEET_LIST=""
SITE_LIST=""
```

#### Data Initialization & Update

##### Article

Use [covid-19-extract-convert](https://github.com/NLPforCOVID-19/covid-19-extract-convert), [text-classifier](https://github.com/NLPforCOVID-19/text-classifier) and [covid-19-translate](https://github.com/NLPforCOVID-19/covid-19-translate) to prepare the data.
Then run:

```
$ python cron.py --update_database
```

#### Stats

Run:

```
$ python cron.py --update_stats
```

#### Information Source

Run:

```
$ python cron.py --update_sources
```

### Run

Run:

```
$ gunicorn app:app -b 0.0.0.0:12345 --reload
[INFO] Starting gunicorn 20.0.4
[INFO] Listening at: http://0.0.0.0:12345
```

