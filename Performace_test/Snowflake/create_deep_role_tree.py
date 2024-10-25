

import os
from collections import defaultdict
import csv
import snowflake.connector
import sql.deep_role_sql as sql
import sql.cleanup_sql as cleanup


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


def remove_roles(cur,num_of_roles):
    # drops roles from 0 to num_of_roles
    for query in cleanup.generate_drop_role_queries(num_of_roles):
        cur.execute_async(query)
    return 


def use_warehouse(cur, warehouse):
    cur.execute(f"use warehouse {warehouse}")

def main(repetitions,roles_to_create,measurement_points):
    stats_mapping = {}
    repetition_to_queryid = {}

    connection_config = create_connection("DEEP_ROLE_DB", "PUBLIC")
    conn = snowflake.connector.connect(**connection_config)
    cur = conn.cursor()


    try:
        use_warehouse(cur, "ANIMAL_TASK_WH")
        
        print("Running deep role tree")
        
        # Run create roles
        print("Running Create Roles")
        for i in range(repetitions):
            
            if i < (repetitions - 1):
                print(f"Running repetition {1+i} out of {repetitions}", end="\r")
            else:
                print(f"Running repetition {1+i} out of {repetitions}")

            for role_num in range(1,roles_to_create+1):
                first_query = True
                
                for query in sql.generate_role_queries(f"Role{role_num}",f"Role{role_num-1}"):
                    cur.execute(query)
                                        
                    if (first_query and role_num in measurement_points):
                        qid = cur.sfqid
                        # take the time of creating role x                
                        first_query = False
                        
                        # i = repetition
                        # role_num = measurement point.
                        repetition_to_queryid[(i,role_num)] = qid
                        stats = get_query_stats(cur, qid)
                        for qid, schema, warehouse_size, elapsed_seconds, elapsed_milli in stats:
                            stats_mapping[qid] = (schema, warehouse_size, elapsed_seconds, elapsed_milli)
                    
            # run clean up roles            
            remove_roles(cur,roles_to_create)
         
          
        
        with open('./benchmark_deep_role_tree_stats.csv', 'w') as file:
            writer = csv.writer(file, delimiter=';')


            for (i, role_num), qid in repetition_to_queryid.items():
                aggregated_elapsed_milli = sum([stats_mapping[qid][3]])
                aggregated_elapsed_seconds = aggregated_elapsed_milli / 1000
                writer.writerow((i, role_num, aggregated_elapsed_seconds, aggregated_elapsed_milli))
                
                

    finally:
        conn.close()
        cur.close()

    print("Benchmark results is written to 'benchmark_deep_role_tree_stats.csv'")