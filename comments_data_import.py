from pymongo import MongoClient
import json
from bson import ObjectId

try:
    client = MongoClient('localhost', 27017)
except:
    print("Connection error")

db = client['mflix']
print(db)
collection = db['comments']

item_list = []

f = open('/Users/shivamraj/Documents/Learning/pyMongo/comments.json', 'r')
lines = f.readlines()
for line in lines:
    hash = json.loads(line)
    hash['_id'] = ObjectId(hash['_id']['$oid'])
    hash['movie_id'] = ObjectId(hash['movie_id']['$oid'])
    hash['date'] = hash['date']['$date']['$numberLong']
    item_list.append(hash)

if isinstance(item_list, list):
    collection.insert_many(item_list)
else:
    collection.insert_one(item_list)

client.close()
