import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from datetime import datetime


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    dateTime = datetime.now()
    print('Time started: ', dateTime)
    load_staging_tables(cur, conn)
    dateTime = datetime.now()
    print('Loading staging tables! Time completed: ', dateTime)
    insert_tables(cur, conn)
    dateTime = datetime.now()
    print('Insert tables! Time completed: ', dateTime)

    conn.close()


if __name__ == "__main__":
    main()