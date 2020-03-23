import os
import json
from uuid import uuid4
from elasticsearch import Elasticsearch

INDEX_NAME = 'papers'


def bulk_index(jsondocs):
    body = ''
    for doc in jsondocs:
        body += json.dumps({"index": {"_id": str(uuid4())}}) + '\n' + json.dumps(doc) + '\n'
    return es.bulk(index=INDEX_NAME, body=body)


def create_index(es):
    if es.indices.exists(INDEX_NAME):
        print("deleting '%s' index." % INDEX_NAME)
        res = es.indices.delete(index=INDEX_NAME)
        print(" resp: '%s'" % res)


    # Defining the main fields to be analyzed as english
    # thus stemmed and stopword-filtered
    # while the author field doesn't get stemmed or stopword-filtered

    mappings = {
            "properties": {
                "title": {
                    "type": "text",
                    "analyzer": "english"
                },
                "abstract": {
                    "type": "text",
                    "analyzer": "english"
                },
                "fulltext": {
                    "type": "text",
                    "analyzer": "english",
                },
                "authors": {
                    "type": "text"
                },
                "author_emails": {
                    "type": "keyword"
                }
            }
        }

    # Create a very simple index with just 1 shard and no replicas
    # If this got popular we could host this with replicas enabled

    request_body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": mappings
    }
    print("creating '%s' index..." % INDEX_NAME)
    res = es.indices.create(index=INDEX_NAME, body=request_body)
    print(" response: '%s'" % res)


def index_dataset(es):
    files = []
    for r, d, f in os.walk('datasets'):
        for file in f:
            files += [os.path.join(r, file)]
    count=0
    docs = []
    for file in files:
        with open(file) as f:
            if file.endswith('json'):
                data = json.load(f)
                abstracts = [body_text['text'] for body_text in data['abstract']]
                fulltexts = [body_text['text'] for body_text in data['body_text']]
                title = data['metadata']['title']
                authors = []
                author_emails = []
                for author_entry in data['metadata']['authors']:
                    author_name = '%s %s %s %s' %(author_entry['first'],
                                                  " ".join(author_entry['middle']),
                                                  author_entry['last'],
                                                  author_entry['suffix'])
                    authors.append(author_name.strip())
                    author_emails.append(author_entry['email'])
                doc = {
                    "fulltext": fulltexts,
                    "abstract": abstracts,
                    "title": title,
                    "authors": authors,
                    "author_emails": author_emails
                }
                docs.append(doc)
                if count % 100 == 0:
                    print('Prepared %d docs' % count)

                if len(docs) == 500:
                    bulk_index(docs)
                    docs = []
                count += 1

    # Anything that didn't fit in the last indexed batch
    bulk_index(docs)
    # This "commits" all the work and makes it searchable
    es.indices.refresh(index=INDEX_NAME)


def test_index(es):
    # Now verify that you have indeed indexed stuff
    # By default you should have a max hits of 10000 so this will not be equal to all your indexed documents!
    res = es.search(index=INDEX_NAME, body={"query": {"match_all": {}}})
    print('Got %d Hits:' % res['hits']['total']['value'])
    # Here's your first 3 hits
    for hit in res['hits']['hits'][:3]:
        print(hit['_source'])


es = Elasticsearch()
create_index(es)
index_dataset(es)
test_index(es)

