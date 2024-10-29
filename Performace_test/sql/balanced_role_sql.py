
def generate_role_queries(head,leaf1):
    return [
        f"""
        CREATE OR REPLACE ROLE {leaf1};
        """,
        f"""
        GRANT ROLE {head} TO ROLE {leaf1};
        """
    ]