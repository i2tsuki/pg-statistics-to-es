#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from elasticsearch import Elasticsearch, helpers


def es_connect(host="localhost", scheme="https", port=9200):
    es = Elasticsearch(
        hosts=[host],
        scheme=scheme,
        port=port,
        maxsize=24,
        timeout=5
    )
    if es.ping():
        return es
    else:
        print('connection failed')
    return None


def es_bulk_store_record(es, index_name, records):
    err = None
    try:
        actions = []
        for record in records:
            actions.append({
                '_op_type': 'index',
                "_index": index_name,
                "_type": "record",
                "_source": record,
            })

        resp = helpers.bulk(
            client=es,
            actions=actions,
        )

    except Exception as e:
        sys.stderr.write("error in indexing data: {}".format(str(e)))
        err = err
    return resp, err
