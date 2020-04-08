# COVID-19-API for [COVID-19-UI]((https://github.com/NLPforCOVID-19/covid-19-ui))

## Requirements

- Python 3.6 or later
- [Flask](https://pypi.org/project/Flask/)
- [Flask-Bootstrap4](https://pypi.org/project/Flask-Bootstrap4/)
- [Flask-Cors](https://pypi.org/project/Flask-Cors/)
- [pandas](https://pypi.org/project/pandas/)
- [pymongo](https://pypi.org/project/pymongo/)

## Setup

### Python

To install all the dependencies, use `pipenv`.

```
$ pipenv sync
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

Copy `config.example.json` to `config.json`, and then rewrite the following values.

- activator: The path to the script that activates the Python virtual environment.
- database.input_page_path: The path to a JSONL file created by the [text-classifier](https://github.com/NLPforCOVID-19/text-classifier).
- database.host: The hostname of the system running the `mongod` process.
- database.port: The port number that the `mongod` process is listening (default: 27017). The port number must be written as an integer.
- database.db_name: A database name (e.g., "covid19DB").
- database.collection_name: A collection name (e.g., "pages").
- database.log_path: The path to a log file.

### Data Initialization and Update

#### Web page information

Run the following script to initialize and also update the web page information.

```
$ pipenv run python database.py
```

NOTE: After the first execution of the above script, create indexes to improve search performance.

```
$ mongo --port <port>
> use <db-name>
> db[<collection_name>].createIndex({'page.timestamp': -1})
> db[<collection_name>].createIndex({'page.classes.COVID-19関連': -1})
> db[<collection_name>].createIndex({'page.classes.COVID-19関連': -1, 'page.orig.timestamp': 1})
> db[<collection_name>].createIndex({'page.classes.COVID-19関連': -1, 'page.country': -1, 'page.orig.timestamp': 1})
```

#### Statistics

Run the following script to initialize and also update the statistics.

```
$ pipenv run python stats.py
```

## Run

To start this API server, run the following command.

```
$ pipenv run flask run
 * Running on http://127.0.0.1:5000/
```

To make sure it is working properly, open `http://127.0.0.1:5000/` with a browser.
