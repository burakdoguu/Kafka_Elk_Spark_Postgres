from elasticsearch import Elasticsearch
import csv
import base64
import json


# es = Elasticsearch(host='localhost', port=9200)
es = Elasticsearch([{'host': 'elasticsearch'}])

body={
    "query":{
        "match_all":{}
    }
}
result = es.search(index="information-creation",body=body)
response=result["hits"]["hits"]
print(response)
global data_bytes_dict
list_ = []
for doc in response:
    data_code64 = doc["_source"]["message"]
    code64_bytes = data_code64.encode('utf-8')
    x = base64.decodebytes(code64_bytes)
    #print(x)
    data_bytes = x.decode("ascii")
    #print(data_bytes)
    data_bytes_dict = json.loads(data_bytes)
    list_.append(data_bytes_dict)
    field_names = ['name', 'company', 'msg', 'remote_ip', 'user_agent', 'date']
with open('/opt/airflow/resources/data/information_creation.csv', 'w',newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=field_names)
    writer.writeheader()
    writer.writerows(list_)