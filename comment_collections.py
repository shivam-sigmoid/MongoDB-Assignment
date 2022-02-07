from bson import ObjectId
import pymongo


def insert_in_comments(collections, name, email, movie_id, text, date):
    collections.insert_one({"name": name, "email": email, "movie_id": ObjectId(movie_id), "text": text, "date": date})
    print("Comment added!")


def task_one_with_mongo_query(collections):
    pipeline = [
        {
            '$group': {
                '_id': '$name',
                'total': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'total': -1
            }
        }, {
            '$limit': 10
        }
    ]

    ans = collections.aggregate(pipeline)
    usernames = []
    for i in ans:
        usernames.append(i['_id'])
    return usernames


def task_two_with_mongo_query(collections):
    pipeline = [
        {
            '$group': {
                '_id': '$movie_id',
                'total': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'total': -1
            }
        }, {
            '$limit': 10
        }, {
            '$lookup': {
                'from': 'movies',
                'localField': '_id',
                'foreignField': '_id',
                'as': 'data'
            }
        }, {
            '$unwind': {
                'path': '$data',
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$project': {
                'data.title': 1
            }
        }
    ]
    ans = collections.aggregate(pipeline)
    movies_name = []
    for i in ans:
        movies_name.append(i['data']['title'])
    return movies_name


def task_one_with_python(collections):
    hash = {}
    for row in collections.find():
        email = row['email']
        if hash.get(email):
            hash[email] += 1
        else:
            hash[email] = 1
    a = sorted(hash.items(), key=lambda o: o[1], reverse=True)
    return a[0:10]


def task_two_with_python(collections, db):
    hash = {}
    for row in collections.find():
        movie_id = row['movie_id']
        if hash.get(movie_id):
            hash[movie_id] += 1
        else:
            hash[movie_id] = 1
    a = dict(sorted(hash.items(), key=lambda o: o[1], reverse=True))
    data = []
    cnt = 0
    for k, v in a.items():
        x = db['movies'].find_one({"_id": ObjectId(k)})
        data.append(x['title'])
        cnt += 1
        if cnt == 10:
            break
    return data


def task_three_with_mongo_query(collections, year):
    pipeline = [
        {"$project": {"_id": 0, "date": {"$toDate": {"$convert": {"input": "$date", "to": "long"}}}}},
        {"$group": {
            "_id": {
                "year": {"$year": "$date"},
                "month": {"$month": "$date"}
            },
            "total_person": {"$sum": 1}}
        },
        {"$match": {"_id.year": {"$eq": year}}},
        {"$sort": {"_id.month": 1}}
    ]
    result = collections.aggregate(pipeline)
    li = []
    for i in result:
        li.append(i);
    return li


def comment_driver(db):
    collections = db['comments']
    # Insert New Comments
    # insert_in_comments(collections, name="alex", email="alex@sigmoid.com", movie_id="573a13e4559313caabdd82f3",
    #                    text="Smile",
    #                    date="")
    # Find top 10 users who made the maximum number of comments
    print("Top 10 users who made the maximum number of comments")
    # taskOneWithPython = task_one_with_python(collections)
    # print(taskOne)
    taskOneWithMongoQuery = task_one_with_mongo_query(collections)
    print(taskOneWithMongoQuery)
    # Find top 10 movies with most comments
    print("Top 10 movies with most comments")
    # taskTwoWithPython = task_two_with_python(collections, db)
    # print(taskTwoWithPython)
    taskTwoWithMongoQuery = task_two_with_mongo_query(collections)
    print(taskTwoWithMongoQuery)
    # Given a year find the total number of comments created each month in that year
    print("All comments with given year i.e. 2000")
    year = "2000"
    taskThreeWithMongoQuery = task_three_with_mongo_query(collections, 2000)
    print(taskThreeWithMongoQuery)


if __name__ == "__main__":
    # Connecting to database
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    # Creating database
    db = client["mflix"]
    comment_driver(db)
