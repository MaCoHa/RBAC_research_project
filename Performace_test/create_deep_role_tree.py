

import datetime
import os
import sys
import time
from dotenv import load_dotenv
import mariadb
import psycopg2
import snowflake.connector
import sql.deep_role_sql as sql
import utils as util









def main(repetitions,time_limit_minutes,file_name,db):


    load_dotenv()
    # Set up the database connection based on the provided db parameter
    # Connecting to Snowflake via the local or cloud driver does not change
    if db == "Snowflake" or db == "Snowflake_EC2":
        connection_config = util.create_connection()
        conn = snowflake.connector.connect(**connection_config)
        cur = conn.cursor()
        util.use_warehouse(cur, os.getenv('Snowflake_warehouse'))
    # Connecting to PostgreSQL local experiment
    elif db == "PostgreSql":
        conn = util.postgres_config()
        conn.autocommit = True
        cur = conn.cursor() 
    # Connecting to PostgreSQL Cloud from a EC2 instance to a RDS instance
    elif db == "PostgreSql_EC2":
        
        conn = util.postgres_config_remote()
        conn.autocommit = True
        cur = conn.cursor() 
        
    # Connecting to MariaDB local experiment
    elif db == "MariaDB":
        try:
            conn = util.mariadb_config() 
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
        cur = conn.cursor()

    # Connecting to MariaDB Cloud from a EC2 instance to a RDS instance
    elif db == "MariaDB_EC2":
        try:
            conn = util.mariadb_config_remote() 
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
        cur = conn.cursor()


    
    test_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')



    time_limit_seconds = time_limit_minutes * 60
    
    util.create_log_initial(file_name)
    

    try:
        
        for i in range(repetitions):
            
           

            start_time = time.time()
            role_num = 1
            while True:
                elapsed_time = time.time() - start_time
                if elapsed_time > time_limit_seconds:
                    print("Time limit reached. Exiting loop.")
                    break
                
                for query in sql.generate_role_queries(db,f"Role{role_num}",f"Role{role_num-1}"):

                    start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    cur.execute(query)
                    end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    util.append_to_log(file_name,
                            [test_id,
                            query.replace(";",""),
                            db,
                            "Deep_tree",
                            i,
                            role_num,
                            (start_query_time),
                            (end_query_time)])  
                    
                    
                role_num += 1
                
            #util.remove_roles(db,cur,role_num)    
            util.remove_roles_log(db,cur,role_num,file_name,test_id,i,"Wide_tree")
                
    finally:
        conn.close()
        cur.close()
