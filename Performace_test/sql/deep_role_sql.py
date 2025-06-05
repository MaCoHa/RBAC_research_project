

# Function to generate SQL queries for creating and 
# granting roles on a Deep hierarchy
def generate_role_queries(db,role1,role2):
    if db == "Snowflake" or db == "Snowflake_EC2":
        return [
            f"CREATE OR REPLACE ROLE {role1};",
            f"GRANT ROLE {role1} TO ROLE {role2};"
        ]
    elif db == "PostgreSql" or db == "PostgreSql_EC2":
        return [
            f"CREATE ROLE {role1};",
            f"GRANT {role1} TO {role2};"
        ]
    else:
        # MariaDB
         return [
            f"CREATE ROLE {role1};",
            f"GRANT {role1} TO {role2};"
        ]

