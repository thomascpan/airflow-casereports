from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
import json

es = Elasticsearch(
    ['https://search-acrobat-smsvp2rqdw7jhssq3selgvrqyi.us-west-2.es.amazonaws.com'])

count = int(es.cat.count(index='casereport').strip().split(' ')[2])
query_all = {"size":count, "query": {"match_all": {}}}

response = es.search(index="casereport", body=query_all)

all_documents = response['hits']['hits']


# save snapshot
with open('data/es.json', 'w') as fp:
    json.dump(all_documents, fp)


# delete index 
es.indices.delete(index='casereport', ignore=[400, 404])

# create
bulk_creates = []
for result in all_documents:
    bulk_creates.append(
        {
            "_index": "casereport",
            "_type": "_doc",
            "_id": result['_source']['pmID'],
            "_source": {
                "pmID": result['_source']['pmID'],
                "content": result['_source']['content']
            }
        }
    )
bulk(es, bulk_creates)

# Load json file
with open('data/es.json') as f:
    all_documents = json.load(f)


# delete
bulk_deletes = []
for result in all_documents:
    x = dict(result)
    x["_op_type"] = 'delete'
    bulk_deletes.append(x)
bulk(es, bulk_deletes)