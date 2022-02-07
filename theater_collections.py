import pymongo


def insert_in_theater(collections, theater_id, location):
    collections.insert_one({'theaterId': theater_id, 'location': location})


def task_one_with_mongo_query(collections):
    pipeline = [
        {"$group": {"_id": {"city": "$location.address.city"}, "total_theaters": {"$sum": 1}}},
        {"$sort": {"total_theaters": -1}},
        {"$limit": 10},
        {"$project": {"city_name": "$_id.city", "_id": 0, "total_theaters": 1}}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_two(collections, coord):
    pipeline = [
        {"$group": {"_id": {"city": "$location.address.city"}}},
        {"$match": {"location.geo.coordinates[0]": coord[0], "location.geo.coordinates[1]": coord[1]}},
        {"$limit": 10},
        {"$project": {"city_name": "$_id.city", "_id": 0}}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def theater_driver(db):
    collections = db['theaters']
    # Insert New Theater
    # insert_in_theater(collections, '1999', {
    #     'address': {'street1': 'Thana Road Khagaul', 'city': 'Patna', 'state': 'Bihar', 'zipcode': '801105'},
    #     'geo': {'type': 'Point', 'coordinates': ['-117.4386', '56.035653']}})

    # Top 10 cities with the maximum number of theatres
    print("Top 10 cities with the maximum number of theatres")
    taskOneWithMongoQuery = task_one_with_mongo_query(collections)
    print(taskOneWithMongoQuery)
    # Top 10 theatres nearby given coordinates
    print("Top 10 theatres nearby given coordinates")
    coord = ['-93.24565', '44.85466']
    taskTwo = task_two(collections, coord)
    print(taskTwo)


if __name__ == "__main__":
    # Connecting to database
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    # Creating database
    db = client["mflix"]
    theater_driver(db)
