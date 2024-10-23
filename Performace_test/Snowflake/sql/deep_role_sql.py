


def generate_role_queries(role1,role2):
    return [
        f"""
        CREATE OR REPLACE ROLE {role1};
        """,
        f"""
        GRANT ROLE {role1} TO ROLE {role2};
        """
    ]

