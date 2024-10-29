

import datetime
import os
from collections import defaultdict
import csv
import queue
import time
import snowflake.connector
import sql.balanced_role_sql as sql
import utils as util



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

def main(repetitions,time_limit_minutes,file_name,db):
    
    connection_config = create_connection("DEEP_ROLE_DB", "PUBLIC")
    conn = snowflake.connector.connect(**connection_config)
    cur = conn.cursor()

    time_limit_seconds = time_limit_minutes * 60

    util.create_log_initial(file_name)
    try:
        use_warehouse(cur, "ANIMAL_TASK_WH")
        print("Running balanced role tree")
        
        # Run create roles
        print("Running Create Roles")
        for i in range(repetitions):
            
            if i < (repetitions - 1):
                print(f"Running repetition {1+i} out of {repetitions}", end="\r")
            else:
                print(f"Running repetition {1+i} out of {repetitions}")


            start_time = time.time()
            current = 0
            front = 0
            while True :
                
                front += 1
                elapsed_time = time.time() - start_time
                if elapsed_time > time_limit_seconds:
                    print("Time limit reached. Exiting loop.")
                    break
                            
    
                for query in sql.generate_role_queries(f"Role{current}",f"Role{(front)}"):
                    
                    start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    cur.execute(query)
                    end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    util.append_to_log(file_name,
                            [query,
                            db,
                            "Wide_tree",
                            i,
                            front,
                            (start_query_time),
                            (end_query_time)])
                                
        
            
        
                
                if (front % 4) == 0:
                    current += 1

                
            # run clean up roles  
            util.remove_roles(cur,front)
  
                

    finally:
        conn.close()
        cur.close()