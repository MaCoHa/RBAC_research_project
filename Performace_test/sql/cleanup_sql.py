
def generate_drop_role_queries(num_of_roles):
    queries = []
    for i in range(1,num_of_roles):
       queries.append(f"DROP ROLE Role{i};")
    return queries

