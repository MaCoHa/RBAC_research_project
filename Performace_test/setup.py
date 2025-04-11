import datetime
import os
import time
import snowflake.connector
import sql.setup_sql as sql
import utils as util
import psycopg2 
import mariadb
import sys

def main(db):
    
    if db == "Snowflake" or db == "Snowflake_EC2":
        #print('Connecting to the Snowflake database...') 
        
        connection_config = util.create_connection("RBAC_EXPERIMENTS", "ACCOUNTADMIN")
        conn = snowflake.connector.connect(**connection_config)
        cur = conn.cursor()
        util.use_warehouse(cur, "ANIMAL_TASK_WH")
    elif db == "PostgreSql":
        #print('Connecting to the PostgreSQL database...') 
        
        # connect to the PostgreSQL server 
        conn = util.postgres_config()
        # autocommit commits querys to the database imediatly instead of
        #storing the transaction localy
        conn.autocommit = True
        cur = conn.cursor() 
    elif db == "PostgreSql_EC2":
        #print('Connecting to the PostgreSQL database...') 
        
        # connect to the PostgreSQL server 
        conn = util.postgres_config_remote()
        # autocommit commits querys to the database imediatly instead of
        #storing the transaction localy
        conn.autocommit = True
        cur = conn.cursor() 
        

    elif db == "MariaDB":
        # connect to the MariaDB server   
        #print('Connecting to the MariaDB database...') 
        try:
            # connect to the MariaDB server 
            conn = util.mariadb_config() 
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
        cur = conn.cursor()

    elif db == "MariaDB_EC2":
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
       
          
        for query in sql.generate_setup_queries(db):
            cur.execute(query)
                   
                     
         
    finally:
        conn.close()
        cur.close()
