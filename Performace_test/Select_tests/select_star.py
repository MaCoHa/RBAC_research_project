
import datetime
import os
import time
import snowflake.connector
import sql.grant_sql as sql
import utils as util
import psycopg2 
import mariadb
import sys
import Select_tests.create_trees as create

databases = ["Snowflake","PostgreSql"] # mariadb will be runed in a seperate test, have to change user

tree_type = ["Wide_tree","Deep_tree","Balanced_tree"]

table = "foo"

role = 5

def main(file_name):
    
    test_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    
    util.create_log_initial(file_name)
    for db in databases:
        
        for tree in tree_type:
            
            if db == "Snowflake":
                print('Connecting to the Snowflake database...') 
                
                connection_config = util.create_connection("WIDE_ROLE_DB", "PUBLIC")
                conn = snowflake.connector.connect(**connection_config)
                cur = conn.cursor()
                util.use_warehouse(cur, "ANIMAL_TASK_WH")
            elif db == "PostgreSql":
                print('Connecting to the PostgreSQL database...') 
                
                params = util.postgres_config(section='postgresql_Wide') 
                # connect to the PostgreSQL server 
                conn = psycopg2.connect(**params) 
                # autocommit commits querys to the database imediatly instead of
                #storing the transaction localy
                conn.autocommit = True
                cur = conn.cursor() 
            elif db == "MariaDB":
                print('Mariadb should not be runn here') 
                
                sys.exit(1)
                # connect to the MariaDB server   
                print('Connecting to the MariaDB database...') 
                try:
                    # connect to the MariaDB server 
                    conn = util.mariadb_config("Wide_db") 
                except mariadb.Error as e:
                    print(f"Error connecting to MariaDB Platform: {e}")
                    sys.exit(1)
            cur = conn.cursor()
            try:
                print(f'Create tree {tree} on db : {db}') 
                if tree == "Wide_tree":
                    
                    create.wide_tree(cur,db,role)
                elif tree == "Deep_tree":
                    
                    create.deep_tree(cur,db,role)
                    
                elif tree == "Balanced_tree":
                    create.balanced_tree(cur,db,role)
                    
                else:
                    print("unkown tree line 69")
                    sys.exit(1)
                
                for query in sql.generate_grant_table_querie(db,table,role):
                    start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    cur.execute(query)
                    end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    util.append_to_log(file_name,
                            [test_id,
                            query,
                            db,
                            tree,
                            (start_query_time),
                            (end_query_time)])
                    
                    
                #****************************************************************
                #
                # Select * from foo query
                #
                #****************************************************************    
                start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                cur.execute(f"SELECT * FROM {table};")
                end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                util.append_to_log(file_name,
                        [test_id,
                        f"SELECT * FROM {table};",
                        db,
                        tree,
                        (start_query_time),
                        (end_query_time)])
                
                
                #****************************************************************
                #
                # show roles
                #
                #**************************************************************** 
                if db == "Snowflake":
                    query = f""
                elif db == "PostgreSql":
                     query = f""
                elif db == "MariaDB":
                    query = f""
                    
                start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                cur.execute(query)
                end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                util.append_to_log(file_name,
                        [test_id,
                        query,
                        db,
                        tree,
                        (start_query_time),
                        (end_query_time)])
                
                
                
                if db == "PostgreSql":
                    cur.execute(f"set role postgres;")
                    cur.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA information_schema FROM Role{role};")
                    cur.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM Role{role};")
                    cur.execute(f"REVOKE  ALL PRIVILEGES  ON  {table} FROM  Role{role};")
                elif db == "Snowflake":
                    cur.execute(f"USE ROLE TRAINING_ROLE;")
                    

                # clean tree
                util.remove_roles(db,cur,role+1)
                #close con
            finally:
                cur.close()
                conn.close()
                
