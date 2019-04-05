#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import json
import os
import time


def calc_query_statistics(pg_conn, tmp_filename):
    # Load previous metrics from file with json format
    now = time.time()
    previous_metrics = {"timestamp": now - 60.0}
    if os.path.exists(tmp_filename):
        with open(file=tmp_filename, mode="r") as f:
            previous_metrics = json.load(fp=f)

    records = []
    current_metrics = {}

    # Construct query to get statistics from postgres
    columns = [
        "calls",
        "total_time",
        "rows",
        "shared_blks_hit",
        "shared_blks_read",
        "shared_blks_dirtied",
        "shared_blks_written",
        "local_blks_hit",
        "local_blks_read",
        "local_blks_dirtied",
        "local_blks_written",
        "temp_blks_read",
        "temp_blks_written",
        "blk_read_time",
        "blk_write_time",
    ]

    column = ["CAST(SUM({column}) AS double precision) AS {column}".format(column=i) for i in columns]

    q = " ".join([
        "SELECT query, {column}",
        "FROM pg_stat_statements",
        "GROUP BY query",
        "ORDER BY total_time DESC"
    ]).format(column=", ".join(column))

    cur = pg_conn.cursor()
    cur.execute(q)
    current_metrics = {"timestamp": now}

    for ret in cur.fetchall():
        record = {}
        record["timestamp"] = str(int(now))

        query = ret["query"]
        record["query"] = query
        current_metrics[query] = {}

        for column in columns:
            current_metrics[query][column] = ret[column]
            # Calculate differs with previous time metric
            record[column] = 0.0
            if query in previous_metrics:
                if previous_metrics[query][column] is not None:
                    v = ret[column] - previous_metrics[query][column]
                    if v > 0:
                        record[column] = v

        # Calculate average rows per call & average time per call
        record["avg_rows"] = 0.0
        record["avg_time"] = 0.0
        if record["calls"] > 0.0:
            if record["rows"] > 0.0:
                record["avg_rows"] = record["rows"] / record["calls"]
            if record["total_time"] > 0.0:
                record["avg_time"] = record["total_time"] / record["calls"]

        records.append(record)

    # Write current_metrics to file as json format
    with open(file=tmp_filename, mode="w") as f:
        json.dump(obj=current_metrics, fp=f, ensure_ascii=True, indent=4)

    index_name = "pg-query-statistics-{date}".format(
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
                            "query": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "calls": {
                                "type": "float"
                            },
                            "total_time": {
                                "type": "float"
                            },
                            "rows": {
                                "type": "float"
                            },
                            "shared_blks_hit": {
                                "type": "float"
                            },
                            "shared_blks_read": {
                                "type": "float"
                            },
                            "shared_blks_dirtied": {
                                "type": "float"
                            },
                            "shared_blks_written": {
                                "type": "float"
                            },
                            "local_blks_hit": {
                                "type": "float"
                            },
                            "local_blks_read": {
                                "type": "float"
                            },
                            "local_blks_dirtied": {
                                "type": "float"
                            },
                            "local_blks_written": {
                                "type": "float"
                            },
                            "temp_blks_read": {
                                "type": "float"
                            },
                            "temp_blks_written": {
                                "type": "float"
                            },
                            "blk_read_time": {
                                "type": "float"
                            },
                            "blk_write_time": {
                                "type": "float"
                            },
                            "avg_rows": {
                                "type": "float"
                            },
                            "avg_time": {
                                "type": "float"
                            }
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
