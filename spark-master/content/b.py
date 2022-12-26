from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch('http://localhost:9200')
# INDEX = 'film'
# if not es.indices.exists(index=INDEX):
#     es.indices.create(index=INDEX)
hoho = {
    'author': 'kimchy',
    'text': 'Elasticsearch: cool. bonsai cool.',
    'timestamp': datetime.now(),
}
resp = es.index(index="test-index", id=1, document=hoho)
print(resp['result'])

resp = es.get(index="test-index", id=1)
print(resp['_source'])

es.indices.refresh(index="test-index")

resp = es.search(index="test-index", query={"match_all": {}})
print("Got %d Hits:" % resp['hits']['total']['value'])
for hit in resp['hits']['hits']:
    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])