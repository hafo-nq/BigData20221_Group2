from elasticsearch import Elasticsearch
ES = Elasticsearch('elasticsearch:9200')
INDEX = 'film'
id = 'tt1389137'
print(ES.get(index=INDEX, id=id)['_source'])