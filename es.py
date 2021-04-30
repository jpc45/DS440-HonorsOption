from elasticsearch import Elasticsearch

es = Elasticsearch(host="localhost", port=1001)
es = Elasticsearch()

data = {
        "author": "Chestermo",
        "gender": "male",
        "age": "24",
        "body_fat": "15%",
        "interest": ["couch potato", "eat and sleep"]
    }

def create_index(index):
    es.indices.create(index=index, ignore=400)

def insert_one_data(_index, data):
    # index and doc_type you can customize by yourself
    res = es.index(index=_index, doc_type='authors', id=5, body=data)
    # index will return insert info: like as created is True or False
    print(res)
    """
    {'_index': 'test-index', '_type': 'authors', '_id': '5', '_version': 1, 'result': 'created', '_shards': {'total': 2, 's
uccessful': 1, 'failed': 0}, '_seq_no': 4, '_primary_term': 1}
    """

index = "test-index"
create_index(index)
insert_one_data(index, data)