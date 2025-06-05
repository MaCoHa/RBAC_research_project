
   
    


# Function to generate SQL queries for creating and 
# granting roles on a balanced hierarchy
def generate_role_queries(db,head,leaf1):
    if db == "Snowflake" or db == "Snowflake_EC2":
        return [
            f"CREATE OR REPLACE ROLE {leaf1};",
            f"GRANT ROLE {leaf1} TO ROLE {head};"
        ]
    elif db == "PostgreSql" or db == "PostgreSql_EC2":
        return [
            f"CREATE ROLE {leaf1};",
            f"GRANT {leaf1} TO {head};"
        ]
    else:
        # MariaDB
        return [
            f"CREATE ROLE {leaf1};",
            f"GRANT {leaf1} TO {head};"
        ]

