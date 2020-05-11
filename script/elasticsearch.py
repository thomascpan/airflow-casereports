from elasticsearch import Elasticsearch

es = Elasticsearch(
    ['https://search-acrobat-smsvp2rqdw7jhssq3selgvrqyi.us-west-2.es.amazonaws.com'])

es.cat.count(index='casereport')

casereport_id = None
pm_id = None
count = int(es.cat.count(index='casereport').strip().split(' ')[2])
text = None

body = [
    {
        "index": {
            "_index": 'casereport',
            "_type": '_doc',
            "_id": count
        }
    },
    {
        "id": casereport_id,
        "pmID": pm_id,
        "content": text
    }
]

es.bulk(body=body)


