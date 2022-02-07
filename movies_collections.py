import pymongo



def task_one(topN, collections):
    pipeline = [
        {"$project": {"_id": 0, "imdb.rating": 1, "title": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_two(topN, given_year, collections):
    pipeline = [
        {"$match": {"year": given_year}},
        {"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_three(topN, collections):
    pipeline = [
        {"$match": {"imdb.votes": {"$gt": "1000"}}},
        {"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_four(topN, collections, patt):
    pipeline = [
        {"$match": {"title": {"$regex": patt}}},
        {"$project": {"_id": 0, "title": 1, "tomatoes.viewer.rating": 1}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_five(topN, collections):
    pipeline = [
        {"$unwind": "$directors"},
        {"$group": {"_id": {"director_name": "$directors"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_six(topN, given_year, collections):
    pipeline = [
        {"$unwind": "$directors"},
        {"$group": {"_id": {"directors": "$directors", "year": "$year"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.year": given_year}},
        {"$project": {"_id.directors": 1, "no_of_films": 1}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_seven(topN, genres, collections):
    pipeline = [
        {"$unwind": "$directors"},
        {"$unwind": "$genres"},
        {"$group": {"_id": {"directors": "$directors", "genres": "$genres"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.genres": genres}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_eight(topN, collections):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_nine(topN, given_year, collections):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast", "year": "$year"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.year": given_year}},
        {"$project": {"_id.year": 0}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_ten(topN, genres, collections):
    pipeline = [
        {"$unwind": "$cast"},
        {"$unwind": "$genres"},
        {"$group": {"_id": {"cast": "$cast", "genres": "$genres"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.genres": genres}},
        {"$project": {"_id.genres": 0}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_eleven(collections,topN):
    pipeline = [
        {"$unwind": "$genres"},
        {"$project": {"rating": "$imdb.rating", "genres": "$genres", "title": "$title"}},
        {"$group": {"_id": {"genres": "$genres", "max_rating": {"$max": "$rating"}, "title": {"first": "$title"}}}},
        {"$sort": {"_id.max_rating": -1}},
        {"$limit": topN}
    ]
    li = collections.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def movies_driver(db):
    collections = db["movies"]
    print("Find top `N` movies with the highest IMDB rating")
    topN = 5
    taskOne = task_one(topN, collections)
    print(taskOne)
    print("Find top `N` movies with the highest IMDB rating in a given year")
    taskTwo = task_two(topN, "2000", collections)
    print(taskTwo)
    print("Find top `N` movies with highest IMDB rating with number of votes > 1000")
    taskThree = task_three(topN, collections)
    print(taskThree)
    print("Find top `N` movies with title matching a given pattern sorted by highest tomatoes ratings")
    taskFour = task_four(topN, collections, "Hera ")
    print(taskFour)
    print("Find top `N` directors who created the maximum number of movies")
    taskFive = task_five(topN, collections)
    print(taskFive)
    print("Find top `N` directors - who created the maximum number of movies in a given year")
    taskSix = task_six(topN, "2000", collections)
    print(taskSix)
    print("Find top `N` directors - who created the maximum number of movies for a given genre")
    taskSeven = task_seven(topN, "Documentary", collections)
    print(taskSeven)
    print("Find top `N` actors - who starred in the maximum number of movies")
    taskEight = task_eight(topN, collections)
    print(taskEight)
    print("Find top `N` actors - who starred in the maximum number of movies in a given year")
    taskNine = task_nine(topN, "2000", collections)
    print(taskNine)
    print("Find top `N` actors - who starred in the maximum number of movies for a given genre")
    taskTen = task_ten(topN, "Documentary", collections)
    print(taskTen)
    print("Find top `N` movies for each genre with the highest IMDB rating")
    taskEleven = task_eleven(collections,topN)
    # print(taskEleven)
    for task in taskEleven:
        print(task)

if __name__ == "__main__":
    # Connecting to database
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    # Creating database
    db = client["mflix"]
    movies_driver(db)
