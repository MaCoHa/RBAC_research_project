

import os
from collections import defaultdict
import csv
import queue
import snowflake.connector
import sql.balanced_role_sql as sql
import sql.cleanup_sql as cleanup
from collections import deque


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

def get_query_stats(cur, query_ids):
    query_ids_str = ', '.join([f"'{qid}'" for qid in query_ids])

    stats_query = f"""
    SELECT query_id, schema_name, warehouse_size, total_elapsed_time/1000 AS time_elapsed_in_seconds, total_elapsed_time AS total_elapsed_time_milli
    FROM
        table(information_schema.query_history())
    WHERE user_name = 'CAT' and execution_status = 'SUCCESS' and query_id IN ({query_ids_str})
    ORDER BY start_time desc;
    """
    
    cur.execute(stats_query)
    return cur.fetchall()


def remove_roles(cur,num_of_roles):
    # drops roles from 0 to num_of_roles
    for query in cleanup.generate_drop_role_queries(num_of_roles):
        cur.execute(query)
    return 


def use_warehouse(cur, warehouse):
    cur.execute(f"use warehouse {warehouse}")

def main(repetitions,roles_to_create,measurement_points):
    query_ids = []
    repetition_to_queryid = {}
    queue = deque()
    connection_config = create_connection("DEEP_ROLE_DB", "PUBLIC")
    conn = snowflake.connector.connect(**connection_config)
    cur = conn.cursor()


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

            current = 0
            front = 1
            while True :
                queries = []
                save = False
                print(f"current {current}")    
                print(f"front {front}")                
                            
    
                for query in sql.generate_role_queries(f"Role{current}",f"Role{(front)}"):
                    cur.execute(query)
                                        
                    if ((front) in measurement_points):
                        qid = cur.sfqid
                        # take the time of two querys                 
                        save = True
                        query_ids.append(qid)
                        queries.append(qid)
                        
                if save:
                    repetition_to_queryid[(i,current)] = queries
                
                if (front % 4) == 0:
                    current += 1
                    
                if front == roles_to_create:
                    break
                else:
                    front += 1
                
            # run clean up roles            
            remove_roles(cur,roles_to_create)
         
          
        
        stats = get_query_stats(cur, query_ids)
        stats_mapping = {}
        for qid, schema, warehouse_size, elapsed_seconds, elapsed_milli in stats:
            stats_mapping[qid] = (schema, warehouse_size, elapsed_seconds, elapsed_milli)

        with open('./benchmark_balanced_role_tree_stats.csv', 'w') as file:
            writer = csv.writer(file, delimiter=';')


            for (i, role_num), qids in repetition_to_queryid.items():
                aggregated_elapsed_milli = sum([stats_mapping[qid][3] for qid in qids])
                aggregated_elapsed_seconds = aggregated_elapsed_milli / 1000
                writer.writerow((i, role_num, aggregated_elapsed_seconds, aggregated_elapsed_milli))
                
                

    finally:
        conn.close()

    print("Benchmark results is written to 'benchmark_balanced_role_tree_stats.csv'")