import os
import json
from uuid import uuid4
from elasticsearch import Elasticsearch

INDEX_NAME = 'papers'


# TODO: extract the field names used both here as well as when index-building to a common source of truth
def search(es, query):
    res = es.search(index=INDEX_NAME, body={"query": {"query_string": {
        "query": query, "default_field": "fulltext",

    }},
        "highlight": {
            "fields": {
                "fulltext": {},
                "abstract": {},
                "authors": {},
                "author_emails": {}
            }
        }
    })
    print('Got %d Hits:' % res['hits']['total']['value'])
    # Here's your first hit
    for hit in res['hits']['hits'][:1]:
        print(hit['highlight'])


es = Elasticsearch()
search(es, "covid AND however AND sars")

