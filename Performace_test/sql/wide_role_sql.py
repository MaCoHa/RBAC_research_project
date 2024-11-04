


def generate_role_queries(db,name):
    if db == "Snowflake":
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

