#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import json
import os
import time


def calc_user_table_statistics(pg_conn, tmp_filename):
    # Load previous metrics from file with json format
    now = time.time()
    previous_metrics = {"timestamp": now - 60.0}
    if os.path.exists(tmp_filename):
        with open(file=tmp_filename, mode="r") as f:
            previous_metrics = json.load(fp=f)

    records = []

    # Construct query to get user table statistics from postgres
    columns = [
        "seq_scan",
        "seq_tup_read",
        "idx_scan",
        "idx_tup_fetch",
        "n_tup_ins",
        "n_tup_upd",
        "n_tup_del",
        "n_tup_hot_upd",
        "n_live_tup",
        "n_dead_tup",
        "vacuum_count",
        "autovacuum_count",
        "analyze_count",
        "autoanalyze_count",
    ]

    q = "SELECT relname, {column} FROM pg_stat_user_tables".format(
        column=", ".join(columns)
    )

    cur = pg_conn.cursor()
    cur.execute(q)
    current_metrics = {"timestamp": now}

    for ret in cur.fetchall():
        record = {}
        record["timestamp"] = str(int(now))

        relname = ret["relname"]
        record["relname"] = relname
        current_metrics[relname] = {}

        for column in columns:
            current_metrics[relname][column] = ret[column]

            # Calculate differs with previous time metric
            record[column] = 0.0
            if relname in previous_metrics:
                if previous_metrics[relname][column] is not None:
                    v = ret[column] - previous_metrics[relname][column]
                    if v > 0.0:
                        record[column] = v

        records.append(record)

    # Write current_metrics to file as json format
    with open(file=tmp_filename, mode="w") as f:
        json.dump(obj=current_metrics, fp=f, ensure_ascii=True, indent=4)

    index_name = "pg-user-table-statistics-{date}".format(
        date=datetime.fromtimestamp(now).strftime("%Y.%m.%d")
    )

    return index_name, records


def es_create_index(es=None, index_name="", shards=5, replicas=1):
    settings = {
                "settings": {
                    "number_of_shards": shards,
                    "number_of_replicas": replicas,
                },
                "mappings": {
                    "record": {
                        "dynamic": "strict",
                        "properties": {
                            "timestamp": {
                                "type": "date",
                                "format": "epoch_second",
                            },
                            "relname": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 64
                                    }
                                }
                            },
                            "seq_scan": {
                                "type": "float"
                            },
                            "seq_tup_read": {
                                "type": "float"
                            },
                            "idx_scan": {
                                "type": "float"
                            },
                            "idx_tup_fetch": {
                                "type": "float"
                            },
                            "n_tup_ins": {
                                "type": "float"
                            },
                            "n_tup_upd": {
                                "type": "float"
                            },
                            "n_tup_del": {
                                "type": "float"
                            },
                            "n_tup_hot_upd": {
                                "type": "float"
                            },
                            "n_live_tup": {
                                "type": "float"
                            },
                            "n_dead_tup": {
                                "type": "float"
                            },
                            "vacuum_count": {
                                "type": "float"
                            },
                            "autovacuum_count": {
                                "type": "float"
                            },
                            "analyze_count": {
                                "type": "float"
                            },
                            "autoanalyze_count": {
                                "type": "float"
                            },
                        }
                    }
                }
            }

    try:
        if not es.indices.exists(index_name):
            resp = es.indices.create(index=index_name, ignore=400, body=settings)
            print(resp)
    except Exception as e:
        print(e)
    return
