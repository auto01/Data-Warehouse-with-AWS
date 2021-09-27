from sql_queries import drop_table_queries,create_table_queries
import psycopg2
import configparser

def drop_tables(conn,cur):
    for q in drop_table_queries:
        cur.execute(q)
        conn.commit()

def create_tables(conn,cur):
    for q in create_table_queries:
        cur.execute(q)
        conn.commit()

def main():
    config=configparser.ConfigParser()
    config.read('dwh.cfg')
    
    try:
        conn=psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur=conn.cursor()
        drop_tables(conn,cur)
        create_tables(conn,cur)
        conn.close()
    except psycopg2.Error as e:
        print(e)
        
if __name__=="__main__":
    main()