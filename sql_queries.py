import configparser

# CONFIG

config=configparser.ConfigParser()
config.read('dwh.cfg')
SONG_DATA=config.get('S3','SONG_DATA')
LOG_DATA=config.get('S3','LOG_DATA')
LOG_JSON_PATH=config.get('S3','LOG_JSONPATH')
DWH_ROLE_ARN=config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_log_events_data;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_data;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"



# CREATE TABLES

staging_events_table_create="""
create table staging_log_events_data
(
artist varchar,
auth varchar,
firstName varchar,
gender varchar,  
itemInSession varchar,
lastName varchar,
length varchar, 
level varchar,
location varchar, 
method varchar,
page varchar,
registration varchar,
sessionId varchar, 
song varchar,
status bigint,
ts bigint,
userAgent text,
userId varchar
);
"""


staging_songs_table_create="""
create table staging_songs_data
(
artist_id varchar(255) not null,
artist_latitude text,
artist_location text,
artist_longitude text,  
artist_name varchar(255) not null,
duration float not null,
num_songs int not null,
song_id varchar(255), 
title text not null,
year int not null
)
"""


songplay_table_create="""
create table songplays
(
    songplay_id int identity(0,1),
    start_time timestamp not null sortkey,
    user_id text,
    level varchar(20) not null,
    song_id varchar(255),
    artist_id varchar(255) not null,
    session_id varchar(20) not null,
    location text,
    user_agent text not null
)
diststyle all;
"""



time_table_create="""
create table time
(
    start_time timestamp not null sortkey,
    hour int not null,
    day int not null,
    week int not null,
    month int not null,
    year int not null,
    weekday int not null 
)
diststyle all;
"""


artist_table_create="""
create table artists
(
    artist_id text sortkey,
    name varchar(255) not null,
    location text,
    lattitude text,
    longitude text
)
diststyle all;
"""


song_table_create="""
create table songs
(
    song_id varchar(255) sortkey,
    title text,
    artist_id varchar(255),
    year int not null,
    duration float
)
diststyle all;
"""
user_table_create="""
create table users
(
    user_id varchar(255) sortkey,
    first_name varchar(120),
    last_name varchar(120),
    gender varchar(10) not null,
    level varchar(10) not null
)
diststyle all;
"""


# STAGING TABLES

staging_songs_table_copy="""
copy staging_songs_data(artist_id,artist_latitude,artist_location,artist_longitude,artist_name,
duration,num_songs,title,year)
from {}
credentials 'aws_iam_role={}'
format as json 'auto'
""".format(SONG_DATA,DWH_ROLE_ARN)

staging_events_table_copy="""
copy staging_log_events_data
(artist,auth,firstName,gender,itemInSession,lastName,length,level,location,method,page,registration,sessionId,song,status,ts,userAgent,userId)
from {}
credentials 'aws_iam_role={}'
format as json {} ;
""".format(LOG_DATA,DWH_ROLE_ARN,LOG_JSON_PATH)

# FINAL TABLES

'''
time_table_insert="""insert into time
(start_time,hour,day,week,month,year,weekday)
select to_timestamp(ts/1000::integer) start_time,
extract(HOUR from to_timestamp(ts/1000::integer)) as hour,
extract(DAY from to_timestamp(ts/1000::integer)) as day,
extract(WEEK from to_timestamp(ts/1000::integer)) as week,
extract(MONTH from to_timestamp(ts/1000::integer)) as month,
extract(YEAR from to_timestamp(ts/1000::integer)) as year,
(case when extract(ISODOW from to_timestamp(ts/1000::integer)) in (6,7) then 1 else 0 end) as weekend
from staging_log_events_data;
"""

'''


time_table_insert="""
INSERT INTO time (                  
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_log_events_data AS se
    WHERE se.page = 'NextSong';
"""


user_table_insert="""insert into users(user_id,first_name,last_name,gender,level)
select distinct userId,firstName,lastName,gender,level 
from staging_log_events_data as se where se.page='NextSong';
"""

artist_table_insert="""insert into artists(artist_id,name,location,lattitude,longitude) 
select distinct
artist_id,artist_name,artist_location,artist_latitude,artist_longitude
from staging_songs_data;
"""

song_table_insert="""insert into songs(song_id,title,artist_id,year,duration) 
select distinct
song_id,title,artist_id,year,duration from staging_songs_data;
"""

songplay_table_insert="""insert into songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'  AS start_time,
userId,level,s.song_id,s.artist_id,sessionId,location,userAgent 
from staging_log_events_data se join staging_songs_data s on (se.song=s.title AND se.artist=s.artist_name) and se.page='NextSong';
"""



# Select table heads

songplay_table_select = "select * from songplays limit 1;"
user_table_select = "select * from users limit 1;"
song_table_select = "select * from songs limit 1;"
artist_table_select = "select * from artists limit 1;"
time_table_select = "select * from time limit 1;"


# QUERY LISTS

drop_table_queries=[staging_events_table_drop,staging_songs_table_drop,songplay_table_drop,user_table_drop
,song_table_drop,artist_table_drop,time_table_drop]


create_table_queries=[staging_events_table_create,staging_songs_table_create,songplay_table_create,user_table_create
,song_table_create,artist_table_create,time_table_create]


copy_table_queries=[staging_songs_table_copy,staging_events_table_copy]

insert_table_queries=[time_table_insert,song_table_insert,artist_table_insert,songplay_table_insert]

select_table_queries=[time_table_select,song_table_select,artist_table_select,user_table_select,songplay_table_select]