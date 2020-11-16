# COVID-19-API for [COVID-19-UI]((https://github.com/NLPforCOVID-19/covid-19-ui))

## Requirements

- See [pyproject.toml](pyproject.toml).

## Setup

### Python

To install all the dependencies, use `poetry`.

```
$ poetry install
```

### MongoDB

This project uses [MongoDB](https://www.mongodb.com/) to store web page information.
To install MongoDB, please follow [the official guide](https://docs.mongodb.com/manual/installation/).

Then start a `mongod` process.
To run it as a daemon, use the `--fork` option.

```
$ sudo mongod --dbpath <dbpath> --logpath <logpath> --port <port> [--fork]
```

### API Configuration

Run `poetry run python conf.py`.

Before running this script, set the following environment variables:

- ACTIVATOR: Path to `<virtual-env>/bin/activate_this.py`.
- DB_HOST: The hostname where MongoDB is running.
- DB_PORT: The port number that MongoDB is listening.
- DB_DB_NAME: The DB identifier.
- DB_COLLECTION_NAME: The collection identifier.
- ES_HOST: The hostname where ElasticSearch is running.
- ES_PORT: The hostname where ElasticSearch is listening.
- FB_LOG_FILE: Where to write feedback messages.
- SLACK_ACCESS_TOKENS: Slack access tokens.
- SLACK_APP_CHANNELS: Slack channels.
- SOURCE: Path to a text file listing information sources.

### Data Initialization & Update

#### Article

```
$ poetry run python database.py
```

#### Stats

```
$ poetry run python stats.py
```

#### Information Source

```
$ poetry run python sources.py
```

## Run

To start this API server, run the following command.

```
$ poetry run flask run
 * Running on http://127.0.0.1:5000/
```
