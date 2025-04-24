
def generate_drop_role_queries(num_of_roles):
    queries = []
    for i in range(1,num_of_roles):
       queries.append(f"DROP ROLE IF EXISTS Role{i};")
    return queries

