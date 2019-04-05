# pg-statistics-to-es
pg-statistics-to-es collect statistics from PostgreSQL server and post to Elasticsearch.

## Description
This tool helps you to analyze PostgreSQL performance.

If you are troubled with PostgreSQL performance or want to monitor performance continuously, you should collect PostgreSQL statistics.  This tool compute PostgreSQL statistics into Elasticsearch and helps you.  

The statistics collected are divided into queries, for example, the number of queries executed, the time taken, and the number of rows read.

## Installation
Deployment pg-statistics-to-es to server that is easy.

Use the package manager [pip](https://pip.pypa.io/en/stable/) to setup pg-statistics-to-es.

```sh
pip install --user -r requirements.txt
```

And build binary by using py [PyInstaller](http://www.pyinstaller.org/).

```sh
./build.sh
```

`build.sh` package a ELF binary from python scripts in docker.
This script outputs `./dist/pg-statistics-to-es` and deploy this file to your server.

## Usage
Sorry, write it later.
