
   

        
def generate_grant_table_querie(db,table,role,tree_type,current):
    if db == "Snowflake" or db == "Snowflake_EC2":
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
        lst = [
            f"GRANT all PRIVILEGES on mariadb.* to Role{role};",
            f"GRANT all PRIVILEGES on mariadb to Role{role};",
            f"GRANT SELECT on mariadb.{table} to Role{role};",
            f"SET DEFAULT ROLE Role0 FOR 'connection'@'%';",
            f"FLUSH PRIVILEGES;"]
        if tree_type == "Balanced_tree" and db == "MariaDB_EC2":
            if role == 1000:
                lst.extend([
                    "GRANT `Role1000` TO `Role249`;",
                    "GRANT `Role249` TO `Role62`;",
                    "GRANT `Role62` TO `Role15`;",
                    "GRANT `Role15` TO `Role3`;",
                    "GRANT `Role3` TO `Role0`;"
                ])
            elif role == 10_000:
                lst.extend([
                    "GRANT `Role10000` TO `Role2499`;",
                    "GRANT `Role2499` TO `Role624`;",
                    "GRANT `Role624` TO `Role155`;",
                    "GRANT `Role155` TO `Role38`;",
                    "GRANT `Role38` TO `Role9`;",
                    "GRANT `Role9` TO `Role2`;",
                    "GRANT `Role2` TO `Role0`;"
                ])
            elif role == 100_000:
                lst.extend([
                    "GRANT `Role100000` TO `Role24999`;",
                    "GRANT `Role24999` TO `Role6249`;",
                    "GRANT `Role6249` TO `Role1562`;",
                    "GRANT `Role1562` TO `Role390`;",
                    "GRANT `Role390` TO `Role97`;",
                    "GRANT `Role97` TO `Role24`;",
                    "GRANT `Role24` TO `Role5`;",
                    "GRANT `Role5` TO `Role1`;",
                    "GRANT `Role1` TO `Role0`;"
                ])
        return lst


   

