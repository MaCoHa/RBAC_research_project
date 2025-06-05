

# Function to generate SQL queries for creating and 
# granting roles on a wide hierarchy
def generate_role_queries(db,name):
    if db == "Snowflake" or db == "Snowflake_EC2":
        return [
            f"CREATE OR REPLACE ROLE {name};",
            f"GRANT ROLE {name} TO Role Role0;"
        ]
    elif db == "PostgreSql":
        return [
            f"CREATE ROLE {name};",
            f"GRANT {name} TO Role0;"
        ]
    else:
        return [
            f"CREATE ROLE {name};",
            f"GRANT {name} TO Role0;"
        ]

