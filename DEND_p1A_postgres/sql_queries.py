# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (songplay_id serial PRIMARY KEY, start_time bigint NOT NULL, user_id int NOT NULL, level varchar, song_id varchar NOT NULL, artist_id varchar, session_id int, location varchar, user_agent varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id varchar PRIMARY KEY, first_name varchar, last_name varchar,gender varchar, level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float(5));""") 

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (artist_id varchar PRIMARY KEY, name varchar, location varchar, lattitude varchar, longitude varchar);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time bigint PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int);""")

# INSERT RECORDS

songplay_table_insert = ("INSERT INTO public.songplay(start_time, user_id, \
                        song_id, artist_id, level, session_id, \
                        location, user_agent) \
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;")

user_table_insert = ("INSERT INTO public.users(user_id, first_name, \
                    last_name, gender, level) \
                    VALUES(%s, %s, %s, %s, %s) \
                    ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;")

song_table_insert = ("""INSERT INTO public.song(song_id, title, artist_id, year, duration)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""")

artist_table_insert = ("""INSERT INTO public.artist(artist_id, name, location, lattitude, longitude)
                    VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""")

time_table_insert = ("INSERT INTO public.time(start_time, hour, day, \
                    week, month, year, weekday)\
                    VALUES(%s, %s, %s, %s, %s, %s, %s) \
                    ON CONFLICT DO NOTHING;")

# FIND SONGS

song_select = ("""SELECT s.song_id, a.artist_id FROM song s JOIN artist a ON s.artist_id = a.artist_id 
            AND s.title = (%s) 
            AND a.name = (%s) 
            AND s.duration = (%s)""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]