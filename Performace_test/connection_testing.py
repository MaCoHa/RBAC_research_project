

import psycopg2
import sys
import boto3
import os


def main():

    ENDPOINT="x"
    PORT="x"
    USER="x"
    PASSWORD="x"
    REGION="x"
    DBNAME="x"
    
 

    try:
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=PASSWORD)
        cur = conn.cursor()
        cur.execute("""SELECT now()""")
        query_results = cur.fetchall()
        print(query_results)
    except Exception as e:
        print("Database connection failed due to {}".format(e)) 
    finally:
        conn.close()
        cur.close()
                        
                    