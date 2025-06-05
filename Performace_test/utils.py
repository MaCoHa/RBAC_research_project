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
            writer.writerow(("test_id","query", "database", "tree_type", "repetition","role_number","start_time","endtime"))
    
    
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

# remove roles cleans up by deleting the roles created during the test.
# It is used to ensure that the database is in a clean state before the next test run.
def remove_roles(db,cur,num_of_roles):
    for query in cleanup.generate_drop_role_queries(num_of_roles):
            cur.execute(query)

# remove_roles_log cleans up by deleting the roles created during the test.
# It also logs the time taken for each query execution.
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

# For the Select experiments if a experiments ran for more that 15 minutes
# it is necessary to add some fill logs to ensure that the log file is not empty.
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
            
     




# Connecting to PostgreSQL hosted on a local machine / localhost  
def postgres_config(): 
    load_dotenv()
    PORT= os.getenv('postgres_port')
    USER= os.getenv('postgres_user')
    PASSWORD= os.getenv('postgres_password')
    DBNAME= os.getenv('postgres_dbname')
    
    conn = psycopg2.connect(port=PORT,host="localhost", database=DBNAME, user=USER, password=PASSWORD) 
    return conn
  
# Connecting to PostgreSQL hosted on a remote machine / RDS instance from an EC2 instance
def postgres_config_remote(): 
    load_dotenv()

    HOST=os.getenv('postgres_remote_HOST')
    USER= os.getenv('postgres_remote_USER')
    PASSWORD= os.getenv('postgres_remote_PASSWORD')
    DBNAME= os.getenv('postgres_remote_DBNAME')
    conn = psycopg2.connect(host=HOST, database=DBNAME, user=USER, password=PASSWORD) 
    return conn

# Connecting to MariaDB connectionuser hosted on a local machine / localhost
def mariadb_connectionuser_config():
    load_dotenv()
    PORT= os.getenv('mariadb_port')
    USER= os.getenv('mariadb_1_user')
    PASSWORD= os.getenv('mariadb_1_password')
    DBNAME= os.getenv('mariadb_dbname')

    
    conn = mariadb.connect(host="localhost", database=DBNAME,  user=USER, password=PASSWORD) 
    return conn

# Connecting to MariaDB root hosted on a local machine / localhost
def mariadb_config():
    load_dotenv()
    PORT= os.getenv('mariadb_port')
    USER= os.getenv('mariadb_user')
    PASSWORD= os.getenv('mariadb_password')
    DBNAME= os.getenv('mariadb_dbname')

    
    conn = mariadb.connect(host="localhost",database=DBNAME, user=USER, password=PASSWORD) 
    return conn
    
# Connecting to MariaDB connectionuser hosted on a remote machine / RDS instance from an EC2 instance
def mariadb_connectionuser_config_remote():
    load_dotenv()
    HOST=os.getenv('mariadb_remote_HOST')
    USER= os.getenv('mariadb_remote_1_USER')
    PASSWORD= os.getenv('mariadb_remote_1_PASSWORD')

    
    conn = mariadb.connect(host=HOST,  user=USER, password=PASSWORD) 
    return conn
# Connecting to MariaDB root hosted on a remote machine / RDS instance from an EC2 instance
def mariadb_config_remote():
    load_dotenv()
    HOST=os.getenv('mariadb_remote_HOST')
    USER= os.getenv('mariadb_remote_USER')
    PASSWORD= os.getenv('mariadb_remote_PASSWORD')
    DBNAME= os.getenv('mariadb_remote_DBNAME')

    conn = mariadb.connect(host=HOST, database=DBNAME,  user=USER, password=PASSWORD) 
    
    return conn

# create_connection establishes a connection to the Snowflake database using environment variables
def create_connection():
    load_dotenv()

    snowflake_config = {
        "user": os.getenv('Snowflake_user'),
        "password": os.getenv('Snowflake_pass'),
        "account": os.getenv('Snowflake_account'),
        "database": os.getenv('Snowflake_database_name'),
        "schema": os.getenv('Snowflake_schema_name'),
        "session_parameters": {
            "USE_CACHED_RESULT": False
        }
    }
    return snowflake_config


def use_warehouse(cur, warehouse):
    cur.execute(f"use warehouse {warehouse}")