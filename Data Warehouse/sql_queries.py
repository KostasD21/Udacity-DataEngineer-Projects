import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
num_events integer identity(0,1),
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
 sessionId integer,
 song varchar,
 status varchar,
 ts varchar,
 userAgent varchar,
 userId integer
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
song_id varchar, 
num_songs int, 
artist_id varchar, 
artist_latitude decimal, 
artist_longitude decimal, 
artist_location varchar, 
artist_name varchar, 
title varchar, 
duration decimal, 
year int
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
songplay_id bigint identity(0,1), 
start_time TIMESTAMP REFERENCES times(start_time), 
user_id int REFERENCES users(user_id), 
level varchar, 
song_id varchar REFERENCES songs(song_id), 
artist_id varchar REFERENCES artists(artist_id), 
session_id int, 
location varchar, 
user_agent varchar, 
PRIMARY KEY (songplay_id)
)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
user_id int, 
first_name varchar NOT NULL, 
last_name varchar NOT NULL, 
gender varchar, 
level varchar NOT NULL, 
PRIMARY KEY (user_id)
)
SORTKEY (user_id);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
song_id varchar, 
title varchar NOT NULL,
artist_id varchar, 
year int, 
duration DECIMAL NOT NULL, 
PRIMARY KEY (song_id)
)
SORTKEY (song_id);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id varchar, 
name varchar NOT NULL, 
location varchar, 
latitude DECIMAL, 
longitude DECIMAL, 
PRIMARY KEY (artist_id)
)
SORTKEY (artist_id);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS times (start_time TIMESTAMP, 
hour int, day int, 
week int, month int, 
year int, weekday int, 
PRIMARY KEY (start_time)
)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
FORMAT AS json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
FROM staging_songs ss
INNER JOIN staging_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users (USER_ID, FIRST_NAME, LAST_NAME, GENDER, LEVEL)
SELECT DISTINCT
                userId,
                firstName,
                lastName,
                gender,
                level
FROM staging_events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs (SONG_ID, TITLE, ARTIST_ID, YEAR, DURATION)
SELECT DISTINCT
                song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists (ARTIST_ID, NAME, LOCATION, LONGITUDE, LATITUDE)
SELECT DISTINCT
                artist_id,
                artist_name,
                artist_location,
                artist_longitude,
                artist_latitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO times (START_TIME, HOUR, DAY, WEEK, MONTH, YEAR, WEEKDAY)
SELECT DISTINCT
                TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' as start_time,
                EXTRACT(HOUR FROM start_time) AS hour,
                EXTRACT(DAY FROM start_time) AS day,
                EXTRACT(WEEKS FROM start_time) AS week,
                EXTRACT(MONTH FROM start_time) AS month,
                EXTRACT(YEAR FROM start_time) AS year,
                EXTRACT(WEEKDAY FROM start_time) AS weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, song_table_create, artist_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, artist_table_insert, song_table_insert, time_table_insert]
