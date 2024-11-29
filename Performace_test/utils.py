import csv

import mariadb
import sql.cleanup_sql as cleanup
import sql.grant_sql as grant
import os
import snowflake.connector


def create_log_select(file_name):
    os.makedirs('./benchmark', exist_ok=True)
    with open(f'./benchmark/{file_name}.csv', 'w') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(("test_id","query", "database", "tree_type","role_number","start_time","endtime"))
    
    

def create_log_initial(file_name):
    os.makedirs('./benchmark', exist_ok=True)
    with open(f'./benchmark/{file_name}.csv', 'w') as file:
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
  
  
def postgres_config(filename='database.ini', section='postgresql'): 
    # create a parser 
    parser = ConfigParser() 
    # read config file 
    parser.read(filename) 
    
    # get section, default to postgresql 
    db = {} 
    if parser.has_section(section): 
        params = parser.items(section) 
        for param in params: 
            db[param[0]] = param[1] 
    else: 
        raise Exception('Section {0} not found in the {1} file'.format(section, filename)) 
  
    return db

def mariadb_connectionuser_config():
    return mariadb.connect(
            user="ConnectionUser",
           # password=os.getenv('Mariadb_PWD'),
            host="localhost",
            port=3306,
            database="Wide_db"
    )   

def mariadb_config(db_type):
    if db_type == "Wide_db":
       return mariadb.connect(
            user="root",
            password=os.getenv('Mariadb_PWD'),
            host="localhost",
            port=3306,
            database="Wide_db"
        )
        
    elif db_type == "Deep_db":
        return mariadb.connect(
            user="ConnectionUser",
            password=os.getenv('Mariadb_PWD'),
            host="localhost",
            port=3306,
            database="Deep_db"
        )
        
    elif db_type == "Balanced_db":
        return mariadb.connect(
            user="ConnectionUser",
            password=os.getenv('Mariadb_PWD'),
            host="localhost",
            port=3306,
            database="Balanced_db"
        )
    



def create_connection(database_name, schema_name):
    password = os.getenv('SNOWSQL_PWD')

    snowflake_config = {
        "user": "CAT",
        "password": password,
        "account": "sfedu02-gyb58550",
        "database": database_name,
        "schema": schema_name,
        "session_parameters": {
            "USE_CACHED_RESULT": False
        }
    }

    return snowflake_config


def use_warehouse(cur, warehouse):
    cur.execute(f"use warehouse {warehouse}")