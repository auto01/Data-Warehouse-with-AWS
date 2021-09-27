from sql_queries import select_table_queries
import psycopg2
import configparser

def select_tables(conn,cur):
    for q in select_table_queries:
        cur.execute(q)
        row=cur.fetchone()
        while row:
            print(row)
            row=cur.fetchone()
    cur.close()

def main():
    config=configparser.ConfigParser()
    config.read('dwh.cfg')
    
    try:
        conn=psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur=conn.cursor()
        select_tables(conn, cur)
        conn.close()
    except psycopg2.Error as e:
        print(e)
        
if __name__=="__main__":
    main()
