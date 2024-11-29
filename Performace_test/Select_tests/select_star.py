
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

databases = ["Snowflake","PostgreSql","MariaDB"]

tree_type = ["Wide_tree","Deep_tree","Balanced_tree"]

table = "foo"

role = 1000

def main(file_name):
    
    test_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    
    util.create_log_select(file_name)
    
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
                            query.replace(";",""),
                            db,
                            tree,
                            role,
                            (start_query_time),
                            (end_query_time)])
                
                    
                #****************************************************************
                #
                # Select * from foo query
                #
                #****************************************************************    
                if db != "MariaDB":
                    start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    cur.execute(f"SELECT * FROM {table};")
                    end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    util.append_to_log(file_name,
                            [test_id,
                            f"SELECT * FROM {table}",
                            db,
                            tree,
                            role,
                            (start_query_time),
                            (end_query_time)])
                else:
                    print('Connecting to the MariaDB ConnectionUser') 
                    try:
                        # connect to the MariaDB server 
                        conn2 = util.mariadb_connectionuser_config() 
                    
                        cur2 = conn2.cursor()
                        cur2.execute("SET ROLE Role0;")
                        
                        start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                        cur2.execute(f"SELECT * FROM {table};")
                        end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                        util.append_to_log(file_name,
                                [test_id,
                                f"SELECT * FROM {table}",
                                db,
                                tree,
                                role,
                                (start_query_time),
                                (end_query_time)])
                    except mariadb.Error as e:
                        print(f"Error connecting to MariaDB Platform: {e}")
                        sys.exit(1)
                    finally:
                        cur2.close()
                        conn2.close()
                        
                    
                
                if db == "Snowflake":
                    cur.execute("USE ROLE TRAINING_ROLE;")
                elif db == "PostgreSql":
                    cur.execute("SET ROLE postgres;")
                #****************************************************************
                #
                # show roles
                # 
                #**************************************************************** 
                
                if db == "Snowflake":
                    query = "SHOW ROLES;"
                elif db == "PostgreSql":
                    query = "SELECT * FROM pg_roles;"
                elif db == "MariaDB":
                    query = "SELECT `User` FROM mysql.user WHERE is_role='Y';"
                    
                start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                cur.execute(query)
                end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                util.append_to_log(file_name,
                        [test_id,
                        "SHOW ROLES",
                        db,
                        tree,
                        role,
                        (start_query_time),
                        (end_query_time)])


                
                #****************************************************************
                #
                # show information schema
                #
                #**************************************************************** 
               
                queries = [
                    "SELECT * FROM INFORMATION_SCHEMA.APPLICABLE_ROLES;",
                    "SELECT * FROM INFORMATION_SCHEMA.ENABLED_ROLES;",
                    "SELECT * FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES;"     
                ]
                    
                
                for q in queries:
                    start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    cur.execute(q)
                    end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    util.append_to_log(file_name,
                            [test_id,
                            q.replace(";",""),
                            db,
                            tree,
                            role,
                            (start_query_time),
                            (end_query_time)])
                
                
                # remove PRIVILEGES from role so i can be deletede
                if db == "PostgreSql":
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
                
