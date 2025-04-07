
   

        
def generate_grant_table_querie(db,table,role):
    if db == "Snowflake":
        return [
            f"GRANT ALL PRIVILEGES ON DATABASE RBAC_EXPERIMENTS TO ROLE ROLE{role};",
            f"GRANT ALL PRIVILEGES ON SCHEMA RBAC_EXPERIMENTS.public TO ROLE ROLE{role};",
            f"GRANT ALL PRIVILEGES ON TABLE {table} TO ROLE ROLE{role};",
            f"GRANT OPERATE ON WAREHOUSE ANIMAL_TASK_WH TO ROLE ROLE{role};",
            f"USE ROLE ROLE0;"
            
        ]
    elif db == "PostgreSql" or db == "PostgreSql_EC2":
        return [
            f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA information_schema TO ROLE{role};"
            f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ROLE{role};",
            f"GRANT ALL PRIVILEGES ON TABLE {table} TO ROLE{role};",
            f"set role Role0;"
        ]
    else:
        # MariaDB
        print(role)
        return [
            f"GRANT all PRIVILEGES on mariadb.* to Role{role};",
            f"GRANT all PRIVILEGES on mariadb.{table} to Role{role};",
            f"SET DEFAULT ROLE Role0 FOR 'connection'@'%';",
            f"FLUSH PRIVILEGES;"
         ]


   

