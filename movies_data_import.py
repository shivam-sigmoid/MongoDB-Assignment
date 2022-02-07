from pymongo import MongoClient
import json
from bson import ObjectId

try:
    client = MongoClient('localhost', 27017)
except:
    print("Connection error")

db = client['mflix']

collection = db['movies']

item_list = []

with open('movies.json') as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            item_list.append(my_dict)

collection.insert_many(item_list)
client.close()
