


def generate_role_queries(name):
    return [
        f"""
        CREATE OR REPLACE ROLE {name};
        """,
        f"""
        GRANT ROLE {name} TO Role Role0;
        """
    ]

