# Tasks -

- Create a Python application to connect to MongoDB.

- Bulk load the JSON files in the individual MongoDB collections using Python. MongoDB collections -
comments
movies
theaters
users

- Create Python methods and MongoDB queries to insert new comments, movies, theatres, and users into respective MongoDB collections.

## Create Python methods and MongoDB queries to support the below operations -

### Comments collection

1. Find top 10 users who made the maximum number of comments

2. Find top 10 movies with most comments

3. Given a year find the total number of comments created each month in that year

## Movies collection
1. Find top `N` movies - 
- with the highest IMDB rating
- with the highest IMDB rating in a given year
- with highest IMDB rating with number of votes > 1000
- with title matching a given pattern sorted by highest tomatoes ratings
2. Find top `N` directors -
- who created the maximum number of movies
- who created the maximum number of movies in a given year
- who created the maximum number of movies for a given genre
3. Find top `N` actors - 
- who starred in the maximum number of movies
- who starred in the maximum number of movies in a given year
- who starred in the maximum number of movies for a given genre
4. Find top `N` movies for each genre with the highest IMDB rating

## Theatre collection
- Top 10 cities with the maximum number of theatres
- Top 10 theatres nearby given coordinates
