import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


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


def main():
    """Setup config, establish connection to db, and call load/insert methods
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()