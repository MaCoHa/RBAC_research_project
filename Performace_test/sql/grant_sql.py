
   

        
def generate_grant_table_querie(db,table,role):
    if db == "Snowflake":
        return [
            f"GRANT ALL PRIVILEGES ON DATABASE WIDE_ROLE_DB TO ROLE ROLE{role};",
            f"GRANT ALL PRIVILEGES ON SCHEMA WIDE_ROLE_DB.public TO ROLE ROLE{role};",
            f"GRANT ALL PRIVILEGES ON TABLE {table} TO ROLE ROLE{role};",
            f"GRANT OPERATE ON WAREHOUSE ANIMAL_TASK_WH TO ROLE ROLE{role};",
            f"USE ROLE ROLE0;"
            
        ]
    elif db == "PostgreSql":
        return [
            f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA information_schema TO ROLE{role};"
            f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ROLE{role};",
            f"GRANT ALL PRIVILEGES ON TABLE {table} TO ROLE{role};",
            f"set role Role0;"
        ]
    else:
        # MariaDB
         return [
            f"grant all on wide_db to Role{role};",
            f"grant all PRIVILEGES on {table} to Role{role};"
         ]


   

