import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (num_songs IDENTITY(0,1) PRIMARY KEY, artist_id VARCHAR, artist_latitude VARCHAR, artist_longitude VARCHAR, artist_location VARCHAR, artist_name VARCHAR, song_id VARCHAR, title VARCHAR, duration VARCHAR, year VARCHAR);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (event_id SERIAL PRIMARY KEY, artist VARCHAR, authenticated VARCHAR, FirstName VARCHAR, Gender VARCHAR, itemInSession INT, lastName VARCHAR, length VARCHAR, level VARCHAR, location VARCHAR, method VARCHAR, page VARCHAR, registration VARCHAR, sessionId INT, song VARCHAR, status INT, ts VARCHAR, userAgent VARCHAR, userId INT); 
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, start_time timestamp, user_id int , level VARCHAR, song_id VARCHAR, artist_id VARCHAR, session_id VARCHAR, location VARCHAR, user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY NOT NULL, title VARCHAR NOT NULL, artist_id VARCHAR, year int, duration float);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY NOT NULL, name VARCHAR NOT NULL, location VARCHAR, latitude float, longitude float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time varchar PRIMARY KEY, hour INT, day INT, week INT, month INT, year INT, weekday INT);
""")

# STAGING TABLES

staging_events_copy =  ("""COPY staging_events FROM {}
credentials 'aws_iam_role={}'
format as json {}
region 'us-west-2'
timeformat 'epochmillisecs';
""").format(config.get('S3','LOG_DATA'),
config.get('IAM_ROLE', 'ARN'),
config.get('S3','LOG_JSONPATH'))


staging_songs_copy = ("""COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
COMPUPDATE OFF
JSON AS'auto'
""").format(config.get('S3','SONG_DATA'),
config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
start_time,
user_id,
level,
song_id,
artist_id,
session_id,
location,
user_agent)
SELECT
timestamp 'epoch' + (e.ts / 1000) * interval '1 second' as start_time,
e.user_id,
e.level,
s.song_id,
s.artist_id,
e.sessionId as session_id,
e.location,
e.userAgent as user_agent
FROM staging_events as e
LEFT JOIN staging_songs s
ON e.song = s.title
AND e.artist = s.artist_name
AND e.page = 'NextSong'
WHERE s.song_id IS NOT NULL
OR s.artist_id IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT
DISTINCT(user_id),
firstName AS first_name,
lastName AS last_name,
gender,
level
FROM staging_events
WHERE user_id IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT (song_id), title, artist_id, year, duration
FROM staging_songs WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_location AS location ,artist_latitude AS latitude,
artist_longitude AS longitude, artist_name as name
FROM staging_songs WHERE song_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT a.start_time,
EXTRACT (HOUR FROM a.start_time), EXTRACT (DAY FROM a.start_time),
EXTRACT (WEEK FROM a.start_time), EXTRACT (MONTH FROM a.start_time),
EXTRACT (YEAR FROM a.start_time), EXTRACT (WEEKDAY FROM a.start_time) FROM
(SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM songplays) a;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
