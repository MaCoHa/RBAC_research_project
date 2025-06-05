# This script generates SQL queries to drop roles in a database.
# It creates a list of queries to drop roles named Role1, Role2, ..., RoleN.
def generate_drop_role_queries(num_of_roles):
    queries = []
    for i in range(1,num_of_roles):
       queries.append(f"DROP ROLE IF EXISTS Role{i};")
    return queries

