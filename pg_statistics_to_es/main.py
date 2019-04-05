#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import fcntl
import os
import sys
import time

import psycopg2
import psycopg2.extras
from psycopg2 import errorcodes

from pg_statistics_to_es.es import es_connect
from pg_statistics_to_es.es import es_bulk_store_record
from pg_statistics_to_es import query_statistics
from pg_statistics_to_es import user_table_statistics

APP_NAME = "pg-statistics-to-es"
CA_CERTS = "/etc/ssl/certs/ca-certificates.crt"

LOCK_FILENAME = os.path.join("/var/tmp/", APP_NAME, APP_NAME + ".lock")
QUERY_TMP_FILENAME = os.path.join("/var/tmp/", APP_NAME, "pg_query_statistics.json")
USER_TABLE_TMP_FILENAME = os.path.join("/var/tmp/", APP_NAME, "pg_user_table_statistics.json")

ES_SHARDS = 2
ES_REPLICAS = 1


def pg_connect(
        host=None,
        port=5432,
        user="postgres",
        password=None,
        database="postgres"
):
    """
    Connect to a PostgreSQL server and return
    cursor & connector.
    """

    pg_conn = psycopg2.connect(
        database=database,
        host=host,
        port=port,
        user=user,
        password=password,
        cursor_factory=psycopg2.extras.DictCursor
    )

    return pg_conn


def main():
    """
    Main function
    """
    pid = str(os.getpid())

    sys.stdout.write("[{pid}]: Start {app}\n".format(
        pid=pid,
        app=APP_NAME
    ))
    sys.stdout.flush()
    os.makedirs(os.path.join("/var/tmp", APP_NAME), exist_ok=True)

    # obtail lock to lock file
    fp = open(file=LOCK_FILENAME, mode="w")

    for i in range(5):
        try:
            fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fp.write(pid + "\n")
            break
        except Exception as err:
            sys.stderr.write("[{pid}]: {err}\n".format(
                pid=pid,
                err=str(err)
            ))
            sys.stdout.flush()
            if i == 4:
                fcntl.flock(fp, fcntl.LOCK_UN)
                fp.close()
                sys.stderr.write("[{pid}]: Failed to detect another process running\n".format(pid=pid))
                sys.exit(1)
            time.sleep(1)

    es_host = "localhost"
    if "ESHOST" in os.environ:
        es_host = os.environ["ESHOST"]
    es_scheme = "https"
    if "ES_SCHEME" in os.environ:
        es_scheme = os.envion["ES_SCHEME"]
    es_port = 9200
    if "ESPORT" in os.environ:
        es_port = int(os.environ["ESPORT"])

    pg_host = "localhost"
    if "PGHOST" in os.environ:
        pg_host = os.environ["PGHOST"]
    pg_port = 5432
    if "PGPORT" in os.environ:
        pg_port = int(os.environ["PGPORT"])
    pg_user = os.environ["PGUSER"]
    pg_password = os.environ["PGPASSWORD"]
    # pg_database in only used in user_table_statistics
    pg_database = "postgres"
    if "PGDATABASE" in os.environ:
        pg_database = os.environ["PGDATABASE"]

    try:
        es_conn = es_connect(host=es_host,
                             scheme=es_scheme,
                             port=es_port)
        if es_conn is None:
            raise("elasticsearch connet failed")

        # PostgreSQL query statistics processing
        sys.stdout.write("[{pid}]: Calculate PostgreSQL query statistics process\n".format(pid=pid))
        sys.stdout.flush()
        pg_conn = pg_connect(host=pg_host,
                             port=pg_port,
                             user=pg_user,
                             password=pg_password,
                             database="postgres")
        index_name, records = query_statistics.calc_query_statistics(
            pg_conn, tmp_filename=QUERY_TMP_FILENAME,
        )
        query_statistics.es_create_index(
            es=es_conn,
            index_name=index_name,
            shards=ES_SHARDS,
            replicas=ES_REPLICAS
        )
        sys.stdout.write("[{pid}]: Send PostgreSQL query statistics process\n".format(pid=pid))
        sys.stdout.flush()
        es_bulk_store_record(
            es=es_conn,
            index_name=index_name,
            records=records,
        )

        pg_conn.close()

        # PostgreSQL user stable statistics processing
        sys.stdout.write("[{pid}]: Calculate PostgreSQL user stable statistics\n".format(pid=pid))
        sys.stdout.flush()
        pg_conn = pg_connect(host=pg_host,
                             port=pg_port,
                             user=pg_user,
                             password=pg_password,
                             database=pg_database)
        index_name, records = user_table_statistics.calc_user_table_statistics(
            pg_conn=pg_conn, tmp_filename=USER_TABLE_TMP_FILENAME,
        )
        user_table_statistics.es_create_index(
            es=es_conn,
            index_name=index_name,
            shards=ES_SHARDS,
            replicas=ES_REPLICAS
        )
        sys.stdout.write("[{pid}]: Send PostgreSQL user stable statistics\n".format(pid=pid))
        sys.stdout.flush()
        es_bulk_store_record(
            es=es_conn,
            index_name=index_name,
            records=records,
        )

        pg_conn.close()

    except psycopg2.OperationalError as err:
        if (
            err.pgcode == errorcodes.INVALID_PASSWORD or
            str(err).strip().startswith("FATAL:  password authentication failed for user")
        ):
            sys.stderr.write("[{pid}]: invalid password\n".format(pid=pid))
        sys.stderr.write("[{pid}]: {err}\n".format(
            pid=pid,
            err=err)
        )
        fcntl.flock(fp, fcntl.LOCK_UN)
        fp.close()
        sys.exit(1)

    except Exception as err:
        sys.stderr.write("[{pid}]: {err}\n".format(
            pid=pid,
            err=err)
        )
        fcntl.flock(fp, fcntl.LOCK_UN)
        fp.close()
        sys.exit(1)

    fcntl.flock(fp, fcntl.LOCK_UN)
    fp.close()
    os.remove(LOCK_FILENAME)
    sys.stdout.write("[{pid}]: End {app}\n".format(
        pid=pid,
        app=APP_NAME,
    ))


if __name__ == "__main__":
    main()
