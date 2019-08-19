import json
import pprint
import urllib.request
import pandas as pd
import configparser
import psycopg2
import glob
import os
from capstone_IBGE.sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """Runs the table creating statements
    Arguments:
        cur {cursor} -- psycopg cursor object
        conn {connection} -- psycopg connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """Runs the table data insert statements
    Arguments:
        cur {cursor} -- psycopg cursor object
        conn {connection} -- psycopg connection object
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

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
    """Setup config, establish connection to db, and call load/insert methods
    """
    config = configparser.ConfigParser()
    config.read('ibge.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CONNECTION'].values()))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()