import csv

import mariadb
import psycopg2
import sql.cleanup_sql as cleanup
import sql.grant_sql as grant
import os
import snowflake.connector
from dotenv import load_dotenv


def create_log_select(file_name):
    os.makedirs('./benchmark', exist_ok=True)
    with open(f'./benchmark/{file_name}.csv', 'w') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(("test_id","query", "database", "tree_type","role_number","start_time","endtime"))
    
    

def create_log_initial(file_name):
    os.makedirs('./benchmark', exist_ok=True)
    with open(f'./benchmark/{file_name}.csv', 'w',newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(("test_id","query", "database", "tree_type", "repetition","role_number","start_time","endtime"))
    
    
def append_to_log(file_name, data):
    with open(f'./benchmark/{file_name}.csv', 'a', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(data)
        

def grant_table(db,cur,role_num,table_name):
      for query in grant.generate_grant_table_querie(db,table_name,role_num):
        if db == "Snowflake":
            cur.execute(query)
        elif db == "PostgreSql":
            cur.execute(query)
        elif db == "MariaDB":
            cur.execute(query)  
        
def remove_roles(db,cur,num_of_roles):
    # drops roles from 1 to num_of_roles
    for query in cleanup.generate_drop_role_queries(num_of_roles):
        
        if db == "Snowflake":
            #print(f"{db} : {query}")
            cur.execute(query)
        elif db == "PostgreSql":
            #print(f"{db} : {query}")
            cur.execute(query)
        elif db == "MariaDB":
            #print(f"{db} : {query}")
            cur.execute(query)

        
 



def get_query_stats(cur, query_id):
    query_id_str = f"'{query_id}'"

    stats_query = f"""
    SELECT query_id, schema_name, warehouse_size, total_elapsed_time/1000 AS time_elapsed_in_seconds, total_elapsed_time AS total_elapsed_time_milli
    FROM
        table(information_schema.query_history())
    WHERE user_name = 'CAT' and execution_status = 'SUCCESS' and query_id = {query_id_str}
    ORDER BY start_time desc;
    """
    
    cur.execute(stats_query)
    return cur.fetchall()



from configparser import ConfigParser 
  
def postgres_config(): 
    load_dotenv()
    PORT= os.getenv('postgres_port')
    USER= os.getenv('postgres_user')
    PASSWORD= os.getenv('postgres_password')
    DBNAME= os.getenv('postgres_dbname')
    
    conn = psycopg2.connect(port=PORT,host="localhost", database=DBNAME, user=USER, password=PASSWORD) 
    return conn
  
def postgres_config_remote(): 
    load_dotenv()

    ENDPOINT=  os.getenv('ENDPOINT') 
    PORT= os.getenv('PORT')
    USER= os.getenv('USER')
    PASSWORD= os.getenv('PASSWORD')
    DBNAME= os.getenv('DBNAME')
    # get section, default to postgresql 
    conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=PASSWORD) 
    return conn

def mariadb_connectionuser_config():
    load_dotenv()
    PORT= os.getenv('mariadb_port')
    USER= os.getenv('mariadb_1_user')
    PASSWORD= os.getenv('mariadb_1_password')
    DBNAME= os.getenv('mariadb_dbname')

    
    conn = mariadb.connect(host="localhost", database=DBNAME,  user=USER, password=PASSWORD) 
    return conn

def mariadb_config():


    load_dotenv()
    PORT= os.getenv('mariadb_port')
    USER= os.getenv('mariadb_user')
    PASSWORD= os.getenv('mariadb_password')
    DBNAME= os.getenv('mariadb_dbname')

    
    conn = mariadb.connect(host="localhost",database=DBNAME, user=USER, password=PASSWORD) 
    return conn
    



def create_connection(database_name, schema_name):
    load_dotenv()

    password = os.getenv('Snowflake_pass')

    snowflake_config = {
        "user": "BLUEJAY",
        "password": password,
        "account": "sfedu02-HZB12071",
        "database": database_name,
        "schema": schema_name,
        "session_parameters": {
            "USE_CACHED_RESULT": False
        }
    }

    return snowflake_config


def use_warehouse(cur, warehouse):
    cur.execute(f"use warehouse {warehouse}")