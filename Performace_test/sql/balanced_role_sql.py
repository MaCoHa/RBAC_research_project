
   
    



def generate_role_queries(db,head,leaf1):
    if db == "Snowflake":
        return [
            f"CREATE OR REPLACE ROLE {leaf1};",
            f"GRANT ROLE {leaf1} TO ROLE {head};"
        ]
    elif db == "PostgreSql":
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

