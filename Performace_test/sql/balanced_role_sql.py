
   
    



def generate_role_queries(db,head,leaf1):
    if db == "Snowflake":
        return [
            f"""
            CREATE OR REPLACE ROLE {leaf1};
            """,
            f"""
            GRANT ROLE {head} TO ROLE {leaf1};
            """
        ]
    elif db == "PostgreSql":
        return [
            f"""
            CREATE ROLE {leaf1};
            """,
            f"""
            GRANT {head} TO {leaf1};
            """
        ]
    else:
        # MariaDB
        return []

