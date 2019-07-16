import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events ( artist VARCHAR, auth VARCHAR, first_name VARCHAR, gender VARCHAR, item_in_session INT, last_name VARCHAR, length DECIMAL, level VARCHAR, location VARCHAR, method VARCHAR, page VARCHAR, registration VARCHAR, session_id INT, song VARCHAR, status INT, ts VARCHAR, user_agent VARCHAR, user_id INT);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs ( song_id VARCHAR(2560), num_songs INT, artist_id VARCHAR(2560), artist_latitude DECIMAL, artist_longitude DECIMAL, artist_location VARCHAR(2560), artist_name VARCHAR(2560), title VARCHAR(2560), duration DECIMAL, year INT);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (songplay_id int IDENTITY(0,1) PRIMARY KEY, start_time timestamp NOT NULL, user_id int NOT NULL, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id varchar PRIMARY KEY, first_name varchar, last_name varchar,gender varchar, level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR(2560) PRIMARY KEY, title varchar, artist_id varchar, year int, duration float(5));""") 

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar, location varchar, lattitude varchar, longitude varchar);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int);""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events
from {}
iam_role {}
json {}
REGION 'us-west-2'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    json 'auto'
    REGION 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])
    
# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT events.start_time, events.user_id, events.level, songs.song_id, songs.artist_id, events.session_id, events.location, events.user_agent 
FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, * FROM staging_events WHERE page='NextSong') events 
LEFT JOIN staging_songs songs ON events.song = songs.title AND events.artist = songs.artist_name AND events.length = songs.duration
""")

user_table_insert = (""" INSERT INTO users (SELECT DISTINCT user_id, first_name, last_name, gender, level 
FROM staging_events WHERE page = 'NextSong' );""")

song_table_insert = ("""INSERT INTO songs (SELECT DISTINCT song_id, title, artist_id, year, duration 
FROM staging_songs);""")

artist_table_insert = ("""INSERT INTO artists (SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
FROM staging_songs);""")

time_table_insert = ("""INSERT INTO time (SELECT DISTINCT timestamp as start_time, DATEPART(HOUR, timestamp) as hour, DATEPART(DAY, timestamp) as day, DATEPART(WEEK, timestamp) as week, DATEPART(MONTH, timestamp) as month, DATEPART(YEAR, timestamp) as year, DATEPART(DOW, timestamp) as weekday 
FROM (SELECT timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' as timestamp
FROM staging_events
WHERE page = 'NextSong'
));""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]