
import datetime
import os
import time
from dotenv import load_dotenv
import snowflake.connector
import sql.grant_sql as sql
import utils as util
import psycopg2 
import mariadb
import sys
import Select_tests.create_trees as create



# This script performs the select experiments on the chosen database and tree hierarchy type.

def main(file_name,database,tree_type,time_limit_minutes,repetitions,tree_sizes,table):
    
    test_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    
    util.create_log_select(file_name)

    for index,tree_size in enumerate(tree_sizes):
        
        
        load_dotenv()
        # Set up the database connection based on the provided db parameter
        # Connecting to Snowflake via the local or cloud driver does not change
        if database == "Snowflake" or database == "Snowflake_EC2":
            connection_config = util.create_connection()
            conn = snowflake.connector.connect(**connection_config)
            cur = conn.cursor()
            util.use_warehouse(cur, os.getenv('Snowflake_warehouse'))
        # Connecting to PostgreSQL local experiment
        elif database == "PostgreSql":
            conn = util.postgres_config()
            conn.autocommit = True
            cur = conn.cursor() 
        # Connecting to PostgreSQL Cloud from a EC2 instance to a RDS instance
        elif database == "PostgreSql_EC2":
            
            conn = util.postgres_config_remote()
            conn.autocommit = True
            cur = conn.cursor() 
            
        # Connecting to MariaDB local experiment
        elif database == "MariaDB":
            try:
                conn = util.mariadb_config() 
            except mariadb.Error as e:
                print(f"Error connecting to MariaDB Platform: {e}")
                sys.exit(1)
            cur = conn.cursor()

        # Connecting to MariaDB Cloud from a EC2 instance to a RDS instance
        elif database == "MariaDB_EC2":
            try:
                conn = util.mariadb_config_remote() 
            except mariadb.Error as e:
                print(f"Error connecting to MariaDB Platform: {e}")
                sys.exit(1)
            cur = conn.cursor()

            

        try:
            # create the role hierarchiy of tree_size if it takes more than time_limit_minutes the creating is
            # terminated and attempted test is logged
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
                
                
                
            # Grants Usage and selct on the table to last created roles
            # Addtionally for Snowflake and PostgreSQL the role is set to Role0
            for query in sql.generate_grant_table_querie(database,table,tree_size,tree_type):
                start_query_time = time.perf_counter_ns() / 1_000_000 # convert from ns to ms
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
                if database != "MariaDB" and database != "MariaDB_EC2":
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
                    try:
                        # For MariaDB the query is executed with a different user
                        # in MariaDB the Root user have read / write access to all even when using the 
                        # Set Role command
                        # so the query is executed with a user that has only read access 
                        # to the table throug the role hierarchy
                        conn2 = util.mariadb_connectionuser_config_remote() 
                    
                        cur2 = conn2.cursor()
                        cur2.execute("SET ROLE Role0;")
                        cur2.execute("Use mariadb;")

                        
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
                        
                    
                
                if database == "Snowflake" or database == "Snowflake_EC2":
                    cur.execute("USE ROLE TRAINING_ROLE;")
                elif database == "PostgreSql" or database == "PostgreSql_EC2":
                    cur.execute("SET ROLE postgres;")

                #****************************************************************
                #
                # show roles
                # 
                #**************************************************************** 
                
                # Each database has its own query to show roles
                if database == "Snowflake" or database == "Snowflake_EC2":
                    query = "SHOW ROLES;"
                elif database == "PostgreSql" or database == "PostgreSql_EC2":
                    query = "SELECT * FROM pg_roles;"
                elif database == "MariaDB" or database == "MariaDB_EC2":
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
                
            
            # Remove PRIVILEGES from role so Roles can be deletede
            if database == "PostgreSql" or database == "PostgreSql_EC2":
                cur.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA information_schema FROM Role{tree_size};")
                cur.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM Role{tree_size};")
                cur.execute(f"REVOKE  ALL PRIVILEGES  ON  {table} FROM  Role{tree_size};")
            elif database == "Snowflake" or database == "Snowflake_EC2":
                cur.execute(f"USE ROLE TRAINING_ROLE;")
            

            util.remove_roles(database,cur,tree_size+1)
        finally:
            cur.close()
            conn.close()
        






                
                
                    
