

import os
import time
import psycopg2
import snowflake.connector
import sql.deep_role_sql as sql
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





def use_warehouse(cur, warehouse):
    cur.execute(f"use warehouse {warehouse}")

def main(repetitions,time_limit_minutes,file_name,db):

    if db == "Snowflake":
        print('Connecting to the Snowflake database...') 
        
        connection_config = create_connection("DEEP_ROLE_DB", "PUBLIC")
        conn = snowflake.connector.connect(**connection_config)
        cur = conn.cursor()
        use_warehouse(cur, "ANIMAL_TASK_WH")
    elif db == "PostgreSql":
        print('Connecting to the PostgreSQL database...') 
        
        params = util.postgres_config(section='postgresql_Wide') 
        # connect to the PostgreSQL server 
        conn = psycopg2.connect(**params) 
        # autocommit commits querys to the database imediatly instead of
        #storing the transaction localy
        conn.autocommit = True
        cur = conn.cursor() 
    else:
        print('Connecting to the MariaDB database...') 
        print('********************* TODO *********************') 
        return

    



    time_limit_seconds = time_limit_minutes * 60
    
    util.create_log_initial(file_name)
    

    try:
        
        print(f"Running deep role tree on {db}")
        
        # Run create roles
        print("Running Create Roles")
        for i in range(repetitions):
            
            if i < (repetitions - 1):
                print(f"Running repetition {1+i} out of {repetitions}", end="\r")
            else:
                print(f"Running repetition {1+i} out of {repetitions}")

            start_time = time.time()
            role_num = 1
            while True:
                # Check the elapsed time
                elapsed_time = time.time() - start_time
                if elapsed_time > time_limit_seconds:
                    print("Time limit reached. Exiting loop.")
                    break
                
                for query in sql.generate_role_queries(db,f"Role{role_num}",f"Role{role_num-1}"):

                    start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    cur.execute(query)
                    end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    util.append_to_log(file_name,
                            [query,
                            db,
                            "Wide_tree",
                            i,
                            role_num,
                            (start_query_time),
                            (end_query_time)])  
                    
                    
                role_num += 1
                
            # run clean up roles            
            util.remove_roles(db,cur,role_num)        
    finally:
        conn.close()
        cur.close()
