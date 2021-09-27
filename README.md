
# Project: Data Warehouse

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.
<br/>

## Project Description

In this project, you'll  build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.
<br/>

## Project Datasets
<hr/>

You'll be working with two datasets that reside in S3. Here are the S3 links for each:
```
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json
```

### Song Dataset
<br/>
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

Files are nested in subdirectories under song_data/
Each file is in JSON format and contains metadata about a song and the artist of that song.


- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset
<br/>
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

- log_data/2018/11/2018-11-12-events.json
- log_data/2018/11/2018-11-13-events.json

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

```

{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}

```
## Dimensional Model Schema
<hr/>

### Fact Table :
- songplays : records in event data associated with song plays i.e. records with page NextSong
    - (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables :
- users - users in the app
    - (user_id, first_name, last_name, gender, level)
- songs - songs in music database
    - (song_id, title, artist_id, year, duration)
- artists - artists in music database
    - (artist_id, name, location, lattitude, longitude)
- time - timestamps of records in songplays broken down into specific units
    - (start_time, hour, day, week, month, year, weekday)


## File Description
<hr/>
<br/>

- create_table.py : In this file you'll create your fact and dimension tables for the star schema in Redshift.
- etl.py : It is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
- sql_queries.py : It is where you'll define you SQL statements, which will be imported into the two other files above.
- test.py : It fetch each table heads(ie.fetch first few records to validate previous steps).
- dwh.cfg : Configuration file to hold information regarding Cluster[Redshift],Storage[S3] and Cloud credentials
- README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

## Running the module
<hr/>
<br/>

To drop existing and create new database with designed Fact and Dimension tables :
```
python create_tables.py
```
For ETL flow (ie.staging the s3 data into Redshift tables and then from staging tables to dimensional models) :
```
python etl.py
```
Fetch each table heads(ie.data validity check / fetch first few records to validate previous two steps) :
```
python test.py
```