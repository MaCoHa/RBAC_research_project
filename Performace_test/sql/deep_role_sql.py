

        
def generate_role_queries(db,role1,role2):
    if db == "Snowflake":
        return [
            f"CREATE OR REPLACE ROLE {role1};",
            f"GRANT ROLE {role1} TO ROLE {role2};"
        ]
    elif db == "PostgreSql":
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

