
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



table = "foo"
### True sizes
#tree_sizes = [1000,10_000,100_000]

### Test sizes 
tree_sizes = [1,10]

def main(file_name,database,tree_type,time_limit_minutes,repetitions):
    
    test_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    
    util.create_log_select(file_name)

    for index,tree_size in enumerate(tree_sizes):
        
        if database == "Snowflake":
            #print('Connecting to the Snowflake database...') 
            
            connection_config = util.create_connection("RBAC_EXPERIMENTS", "ACCOUNTADMIN")
            conn = snowflake.connector.connect(**connection_config)
            cur = conn.cursor()
            util.use_warehouse(cur, "ANIMAL_TASK_WH")
        elif database == "PostgreSql":
            #print('Connecting to the PostgreSQL database...') 
            
            # connect to the PostgreSQL server 
            conn = util.postgres_config()
            # autocommit commits querys to the database imediatly instead of
            #storing the transaction localy
            conn.autocommit = True
            cur = conn.cursor() 
        elif database == "PostgreSql_EC2":
            #print('Connecting to the PostgreSQL database...') 
            
            # connect to the PostgreSQL server 
            conn = util.postgres_config_remote()
            # autocommit commits querys to the database imediatly instead of
            #storing the transaction localy
            conn.autocommit = True
            cur = conn.cursor() 
            

        elif database == "MariaDB":
            # connect to the MariaDB server   
            #print('Connecting to the MariaDB database...') 
            try:
                # connect to the MariaDB server 
                conn = util.mariadb_config() 
            except mariadb.Error as e:
                print(f"Error connecting to MariaDB Platform: {e}")
                sys.exit(1)
            cur = conn.cursor()

        elif database == "MariaDB_EC2":
            # connect to the MariaDB server   
            #print('Connecting to the MariaDB database...') 
            try:
                # connect to the MariaDB server 
                conn = util.mariadb_config_remote() 
            except mariadb.Error as e:
                print(f"Error connecting to MariaDB Platform: {e}")
                sys.exit(1)
            cur = conn.cursor()


        try:
            #print(f'Create tree {tree_type} on db : {database}') 
            if tree_type == "Wide_tree":
                
                control_val = create.wide_tree(cur,database,tree_size,time_limit_minutes)

                if control_val != 0:
                    print(f'Time limit exceded terminating tests for {tree_type} on db : {database}')
                    util.remove_roles(database,cur,control_val)
                    util.add_fill_logs(file_name,database,test_id,index,"Wide_tree",tree_sizes)
                    break
            elif tree_type == "Deep_tree":
                
                control_val = create.deep_tree(cur,database,tree_size,time_limit_minutes)

                if control_val != 0:
                    print(f'Time limit exceded terminating tests for {tree_type} on db : {database}')
                    util.remove_roles(database,cur,control_val)
                    util.add_fill_logs(file_name,database,test_id,index,"Deep_tree",tree_sizes)
                    break
                
            elif tree_type == "Balanced_tree":
               
                control_val = create.balanced_tree(cur,database,tree_size,time_limit_minutes)

                if control_val != 0:
                    print(f'Time limit exceded terminating tests for {tree_type} on db : {database}')
                    util.remove_roles(database,cur,control_val)
                    util.add_fill_logs(file_name,database,test_id,index,"Balanced_tree",tree_sizes)
                    break
                
            else:
                print("unkown tree")
                sys.exit(1)
                
                
                

                for query in sql.generate_grant_table_querie(database,table,tree_size):
                    start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    #print(query)
                    cur.execute(query)
                    end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    util.append_to_log(file_name,
                            [test_id,
                            query.replace(";",""),
                            database,
                            tree_type,
                            tree_size,
                            0,
                            (start_query_time),
                            (end_query_time)])
                
            for rep in range(repetitions):
                    
                #****************************************************************
                #
                # Select * from foo query
                #
                #****************************************************************    
                if database != "MariaDB":
                    start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    cur.execute(f"SELECT * FROM {table};")
                    end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                    util.append_to_log(file_name,
                            [test_id,
                            f"SELECT * FROM {table}",
                            database,
                            tree_type,
                            tree_size,
                            rep,
                            (start_query_time),
                            (end_query_time)])
                else:
                    #print('Connecting to the MariaDB ConnectionUser') 
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
                                database,
                                tree_type,
                                tree_size,
                                rep,    
                                (start_query_time),
                                (end_query_time)])
                        

                    except mariadb.Error as e:
                        print(f"Error connecting to MariaDB Platform: {e}")
                        sys.exit(1)
                    finally:
                        cur2.close()
                        conn2.close()
                        
                    
                
                if database == "Snowflake":
                    cur.execute("USE ROLE TRAINING_ROLE;")
                elif database == "PostgreSql" or database == "PostgreSql_EC2":
                    cur.execute("SET ROLE postgres;")
                #****************************************************************
                #
                # show roles
                # 
                #**************************************************************** 
                
                if database == "Snowflake":
                    query = "SHOW ROLES;"
                elif database == "PostgreSql" or database == "PostgreSql_EC2":
                    query = "SELECT * FROM pg_roles;"
                elif database == "MariaDB":
                    query = "SELECT `User` FROM mysql.user WHERE is_role='Y';"
                    
                start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                cur.execute(query)
                end_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
                util.append_to_log(file_name,
                        [test_id,
                        "SHOW ROLES",
                        database,
                        tree_type,
                        tree_size,
                        rep,
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
                            database,
                            tree_type,
                            tree_size,
                            rep,
                            (start_query_time),
                            (end_query_time)])
                
            
            # remove PRIVILEGES from role so i can be deletede
            if database == "PostgreSql" or database == "PostgreSql_EC2":
                cur.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA information_schema FROM Role{tree_size};")
                cur.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM Role{tree_size};")
                cur.execute(f"REVOKE  ALL PRIVILEGES  ON  {table} FROM  Role{tree_size};")
            elif database == "Snowflake":
                cur.execute(f"USE ROLE TRAINING_ROLE;")
            

            # clean tree
            util.remove_roles(database,cur,tree_size+1)
            #close con
        finally:
            cur.close()
            conn.close()
        






                
                
                    
