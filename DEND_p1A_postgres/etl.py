import os
import glob
import uuid
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime

def process_song_file(cur, filepath):
    """Reads a song data file, and insert the data into the requested tables, being them artist and song.
    
    Arguments: 
        psycopg2 cursor (cur) - Contacts postgreSQL database allowing and allows the connection.
        file path (filepath) - Data file location.
    """
    df = pd.read_json(filepath, lines=True)
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)
    song_data = list(df[['song_id','title','artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

def process_log_file(cur, filepath):
    """Read a log data file and insert the data into the requested tables, 
    being them time, users, song, artist and songplay tables.
    
    Arguments: 
        psycopg2 cursor (cur) - Contacts postgreSQL database allowing and allows the connection.
        file path (filepath) - Data file location.
    """

    df = pd.read_json(filepath, lines=True)
    df = df[df.page == "NextSong"]
    t = (df.ts/1000).apply(datetime.fromtimestamp)
    time_data = (df['ts'].tolist(), t.dt.hour.tolist(), t.dt.day.tolist(), t.dt.dayofweek.tolist(), 
                 t.dt.month.tolist(), t.dt.year.tolist(), t.dt.dayofweek.tolist())
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame({k:v for (k,v) in zip(column_labels, time_data)})
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    user_df = df[['userId','firstName','lastName','gender','level']]
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        songplay_data = ((row.ts/1000), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """Parses a file data directory and point requested tables to extract the data
    and insert it in the appropriate table.
    
    Arguments:
        psycopg2 cursor (cur) - Contacts postgreSQL database allowing and allows the connection.
        psycopg2 connection(conn) - Represent the database connection and allows data interaction.
        file path (filepath) - Data file location.
        function (func) - Function that will proccess the selected files.
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main():
    """Extract song and log data, and insert in the relevant tables(does the ETL itself)
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()

if __name__ == "__main__":
    main()
