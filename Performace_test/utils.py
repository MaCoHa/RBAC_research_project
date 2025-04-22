import csv
import time

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
            writer.writerow(("test_id","query", "database", "tree_type","role_number","repetitions","start_time","endtime"))
    
    

def create_log_initial(file_name):
    os.makedirs('./benchmark', exist_ok=True)
    with open(f'./benchmark/{file_name}.csv', 'w',newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(("test_id","query", "database", "tree_type", "repetition","role_number","start_time","endtime","success","error_msg"))
    
    
def append_to_log(file_name, data):
    with open(f'./benchmark/{file_name}.csv', 'a', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(data)
        

def grant_table(db,cur,role_num,table_name):
      for query in grant.generate_grant_table_querie(db,table_name,role_num):
        if db == "Snowflake" or db == "Snowflake_EC2":
            cur.execute(query)
        elif db == "PostgreSql" or db == "PostgreSql_EC2":
            cur.execute(query)
        elif db == "MariaDB" or db == "MariaDB_EC2":
            cur.execute(query)  
        
def remove_roles(db,cur,num_of_roles):
    # drops roles from 1 to num_of_roles
    #print("Deleting roles")
    #print(f"role num {num_of_roles}" )
    #print(f"database {db}" )

    for query in cleanup.generate_drop_role_queries(num_of_roles):
            cur.execute(query)

def remove_roles_log(db,cur,num_of_roles,file_name,test_id,rep,tree_type):

    for query in cleanup.generate_drop_role_queries(num_of_roles):
        start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
        cur.execute(query)
        end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
        append_to_log(file_name,
                [test_id,
                query.replace(";",""),
                db,
                tree_type,
                rep,
                0,
                (start_query_time),
                (end_query_time)])


def add_fill_logs(file_name,db,test_id,index,tree_type,tree_sizes):
    queries = [
        "SELECT * FROM foo",
        "SHOW ROLES",
        "SELECT * FROM INFORMATION_SCHEMA.APPLICABLE_ROLES;",
        "SELECT * FROM INFORMATION_SCHEMA.ENABLED_ROLES;",
        "SELECT * FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES;"    
    ]
    for i in range(index,3):
        for q in queries:
            append_to_log(file_name,
                [test_id,
                q.replace(";",""),
                db,
                tree_type,
                tree_sizes[i],
                0,
                0])
            
     

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

    HOST=os.getenv('postgres_remote_HOST')
    USER= os.getenv('postgres_remote_USER')
    PASSWORD= os.getenv('postgres_remote_PASSWORD')
    DBNAME= os.getenv('postgres_remote_DBNAME')
    # get section, default to postgresql 
    conn = psycopg2.connect(host=HOST, database=DBNAME, user=USER, password=PASSWORD) 
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
    

def mariadb_connectionuser_config_remote():
    load_dotenv()
    HOST=os.getenv('mariadb_remote_HOST')
    USER= os.getenv('mariadb_remote_1_USER')
    PASSWORD= os.getenv('mariadb_remote_1_PASSWORD')
    #DBNAME= os.getenv('mariadb_remote_DBNAME')

    
    #conn = mariadb.connect(host=HOST, database=DBNAME,  user=USER, password=PASSWORD) 
    conn = mariadb.connect(host=HOST,  user=USER, password=PASSWORD) 
    return conn

def mariadb_config_remote():


    load_dotenv()
    HOST=os.getenv('mariadb_remote_HOST')
    USER= os.getenv('mariadb_remote_USER')
    PASSWORD= os.getenv('mariadb_remote_PASSWORD')
    DBNAME= os.getenv('mariadb_remote_DBNAME')

    conn = mariadb.connect(host=HOST, database=DBNAME,  user=USER, password=PASSWORD) 
    
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