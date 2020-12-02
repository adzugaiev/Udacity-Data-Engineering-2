# Udacity - Data Engineering - 2
# Data Modeling with Apache Cassandra

## About / Synopsis

In this project, I apply what I've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python. To complete the project, I will need to model my data by creating tables in Apache Cassandra to run queries.

## Table of Contents
* [Project Dataset](#project-dataset)
    - [Pre-Processing](#pre-processing)
* [Queries for Song Play Analysis](#queries-for-song-play-analysis)
    - [Data Modeling for Query 1](#data-modeling-for-query-1)
    - [Data Modeling for Query 2](#data-modeling-for-query-2)
    - [Data Modeling for Query 3](#data-modeling-for-query-3)
* [Files in the Project](#files-in-the-project)
* [Running the Project](#running-the-project)
* [What I Have Learned](#what-i-have-learned)
    - [Improvements First Review](#improvements-first-review)
* [Author](#author)

## Project Dataset

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions. For this project, we'll be working with one dataset: `event_data`, the directory of CSV files partitioned by date, for example `event_data/2018-11-08-events.csv`

### Pre-Processing

First, I will process `event_data` into the `event_datafile_new.csv` dataset to create a denormalized dataset, containing the following columns:
- artist
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

The image below is a screenshot of what the denormalized data should appear like in the `event_datafile_new.csv`

![Data Sample](../media/image_event_datafile_new.jpg?raw=true)

## Queries for Song Play Analysis

Now, with Apache Cassandra I model the database tables to create queries for following three questions of the data:
1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

### Data Modeling for Query 1

_Select artist, song title, song length from music_app_history where sessionId = 338 and itemInSession = 4_

> `PRIMARY KEY (session_id, item_in_session)`

### Data Modeling for Query 2

_Select artist, song (sorted by itemInSession), user (first and last name) for userid = 10, sessionid = 182_

We need `itemInSession` as a clustering column for the sort order and to complete a unique primary key.

> `PRIMARY KEY ((user_id, session_id), item_in_session) WITH CLUSTERING ORDER BY (item_in_session DESC)`

### Data Modeling for Query 3

_Select user name (first and last) from music app history who listened to the song 'All Hands Against His Own'_

The combination of `song` title and `userId` should make a sufficient primary key for this particular query. A user may listen to the same song multiple times within the same or different sessions. We can include `sessionId` and `itemInSession` in the primary key to foresee counts of times listened.

> `PRIMARY KEY (song, user_id)`

## Files in the Project

- `Project_1B.ipynb` process the dataset, models the database tables, loads the data and tests the required queries.
- `event_datafile_new.csv` is the pre-processed denormalized dataset file.
- `README.md` provides discussion on this project.

## Running the Project

Run `Project_1B.ipynb`.

> You will have to skip the pre-processing Part I of the notebook and proceed straight to Part II since the original `event_data` directory is not included with the project but the pre-processed dataset `event_datafile_new.csv` is included.

## What I Have Learned

Through the implementation of this project, I've learned:

1) The way partition and clustering columns work in Cassandra and how they impact on the results of my queries.
2) Found that `COPY` command in Cassandra is a shell command, not CQL, so I cannot use it from `session.execute()`.
3) The [tuple trick](https://stackoverflow.com/a/38090766) when a single parameter is passed into `session.execute()`.

### Improvements First Review

Following the first project review I made the following improvements and fixes:

1) [Query 1](#data-modeling-for-query-1), removed clustering by `user_id`.
1) [Query 2](#data-modeling-for-query-2), composite partition key `(user_id, session_id)` and descending clustering order by `item_in_session`.
1) All queries, restructured [column order](https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/whereClustering.html).
1) Restructured the markdown cells between the queries.

## Author

Andrii Dzugaiev, [in:dzugaev](https://www.linkedin.com/in/dzugaev/)