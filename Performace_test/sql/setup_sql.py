

        
def generate_setup_queries(db):
    if db == "Snowflake":
        return [
           
        ]
    elif db == "PostgreSql" or db == "PostgreSql_EC2":
        return [
            "CREATE ROLE Role0;",
            "GRANT Role0 TO postgres;",

            """CREATE TABLE FOO (   
            website_name VARCHAR(25) NOT NULL,    
            server_name VARCHAR(20),    
            creation_date DATE);""",
            """INSERT INTO FOO (website_name, server_name, creation_date)
            VALUES ('example.com', 'server01', '2024-11-06');"""

        ]
    else:
        # MariaDB
         return [
            "CREATE OR REPLACE ROLE Role0;",
            "CREATE OR REPLACE USER 'connection'@'%' IDENTIFIED BY 'mariadb_test';",

            "GRANT Role0 TO 'connection'@'%';",


            """CREATE TABLE mariadb.FOO (    
            website_name VARCHAR(25) NOT NULL,    
            server_name VARCHAR(20),    
            creation_date DATE);""",

            """INSERT INTO mariadb.FOO (website_name, server_name, creation_date)
            VALUES ('example.com', 'server01', '2024-11-06');"""
        ]

