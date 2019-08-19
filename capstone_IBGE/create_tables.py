import configparser
import psycopg2
from capstone_IBGE.sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """Connect to database
    Run imported scripts to drop tables
    Arguments:
        cur {cursor} -- psycopg cursor object
        conn {connection} -- psycopg connection object"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """Create database using sql_queries statements
    Arguments:
        cur {cursor} -- psycopg cursor object
        conn {connection} -- psycopg connection object"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """Database connection statement, connect and and calls the load/insert methods
    """
    config = configparser.ConfigParser()
    config.read('ibge.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CONNECTION'].values()))
    cur = conn.cursor()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    print (conn)
    conn.close()

if __name__ == "__main__":
    main()