import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
artist text, auth text, firstName text, gender text, itemInSession text,
lastName text, length text, level text, location text, method text, page text, 
registration text, sessionId text, song text, status text, ts text, userAgent text,
userId text
)
""")
    

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
num_songs text, artist_id text, artist_latitude text, artist_longitude text,
artist_location text, artist_name text, song_id text, title text,
duration text, year text
)
""")

#fact table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
songplay_id text PRIMARY KEY, start_time text, user_id text, level text, song_id text,
artist_id text, session_id text,  location text, user_agent text
)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS user_table (
user_id text PRIMARY KEY, first_name text, last_name text, gender text, level text )
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS song_table (
song_id text PRIMARY KEY, title text, artist_id text, year text, duration text )
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artist_table (
artist_id text PRIMARY KEY, name text, location text, latitude text, longitude text )
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time_table (
start_time text PRIMARY KEY, hour text, day text, week text, month text, year text, weekday text )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events_table from {}
    iam_role {}
    json {};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""   
    copy staging_songs_table from {}
    iam_role {}
    json 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES
songplay_table_insert = ("""INSERT INTO songplay_table (
start_time, user_id, level, song_id, artist_id, session_id, location,
user_agent) SELECT DISTINCT
e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId,
s.artist_location, e.userAgent
FROM staging_events_table e
JOIN staging_songs_table s ON (e.artist = s.artist_name)
AND (e.song = s.title)
""")

user_table_insert = ("""INSERT INTO user_table (user_id, first_name,
last_name, gender, level) SELECT DISTINCT
userId AS user_id, firstName AS first_name,
lastName AS last_name, gender, level
FROM staging_events_table WHERE page='NextSong';
""")

song_table_insert = ("""INSERT INTO song_table (song_id, title,
artist_id, year, duration) SELECT DISTINCT (song_id), title,
artist_id, year, duration FROM staging_songs_table 
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artist_table (artist_id,
name, location, latitude, longitude) SELECT DISTINCT (artist_id),
artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs_table
WHERE artist_id IS NOT NULL
""")

# REFERENCED https://knowledge.udacity.com/questions/215664 FOR HELP.
time_table_insert = ("""INSERT INTO time_table (start_time,
hour, day, week, month, year, weekday) SELECT ts, EXTRACT(HOUR FROM ts) AS hour,
EXTRACT (DAY FROM ts) AS day, EXTRACT (WEEK FROM ts) AS week, 
EXTRACT (MONTH FROM ts) AS month, EXTRACT (YEAR FROM ts) AS year,
EXTRACT (DOW FROM ts) AS weekday FROM (SELECT distinct TIMESTAMP 'epoch'
+ ts/1000 *INTERVAL '1second' AS ts FROM staging_events_table
WHERE ts IS NOT NULL)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
