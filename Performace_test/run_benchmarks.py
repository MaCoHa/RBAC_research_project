import create_wide_role_tree as wide
import create_deep_role_tree as deep
import create_balanced_role_tree as balanced
import Select_tests.select_star as select
import connection_testing as testing
import setup as setup
# starndard are rep: 2. time: 15
select_repetitions = 5
repetitions = 2
time_limit_minutes = 15






experiments = [
    # Testing
    #("cornnection test", lambda: testing.main()),

    # Already Run

    #("Snowflake wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_snowflake_stats", db="Snowflake")),
    #("Snowflake deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_snowflake_stats", db="Snowflake")),
    #("Snowflake balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_snowflake_stats", db="Snowflake")),
    
    #("Snowflake wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_Wide_tree", database="Snowflake", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    #("Snowflake deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_Deep_tree", database="Snowflake", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    #("Snowflake balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_Balanced_tree", database="Snowflake", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),



    ("Snowflake wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_Snowflake_EC2_stats", db="Snowflake")),
    ("Snowflake deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_Snowflake_EC2_stats", db="Snowflake")),
    ("Snowflake balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_Snowflake_EC2_stats", db="Snowflake")),
    
    ("Snowflake wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_EC2_Wide_tree", database="Snowflake", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    ("Snowflake deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_EC2_Deep_tree", database="Snowflake", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    ("Snowflake balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_EC2_Balanced_tree", database="Snowflake", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),


    #("PostgreSQL wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_postgresql_stats", db="PostgreSql")),
    #("PostgreSQL deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_postgresql_stats", db="PostgreSql")),
    #("PostgreSQL balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_postgresql_stats", db="PostgreSql")),
   
    #("PostgreSQL wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_Wide_tree", database="PostgreSql", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    #("PostgreSQL deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_Deep_tree", database="PostgreSql", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    #("PostgreSQL balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_Balanced_tree", database="PostgreSql", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),

    #("MariaDB wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_mariadb_stats", db="MariaDB")),
    #("MariaDB deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_mariadb_stats", db="MariaDB")),
    #("MariaDB balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_mariadb_stats", db="MariaDB")),
   
    #("MariaDB wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_Wide_tree", database="MariaDB", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    #("MariaDB deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_Deep_tree", database="MariaDB", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    #("MariaDB balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_Balanced_tree", database="MariaDB", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),


    #("PostgreSQL EC2 Setup the database", lambda: setup.main(db="PostgreSql_EC2")),

    #("PostgreSQL EC2 wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_postgresql_ec2_stats", db="PostgreSql_EC2")),
    #("PostgreSQL EC2 deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_postgresql_ec2_stats", db="PostgreSql_EC2")),
    #("PostgreSQL EC2 balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_postgresql_ec2_stats", db="PostgreSql_EC2")),
   
    #("PostgreSQL EC2 wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_ec2_Wide_tree", database="PostgreSql_EC2", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    #("PostgreSQL EC2 deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_ec2_Deep_tree", database="PostgreSql_EC2", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    #("PostgreSQL EC2 balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_ec2_Balanced_tree", database="PostgreSql_EC2", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),


    ("MariaDB_EC2 EC2 Setup the database", lambda: setup.main(db="MariaDB_EC2")),

    #("MariaDB_EC2 wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_MariaDB_EC2_stats", db="MariaDB_EC2")),
    #("MariaDB_EC2 deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_MariaDB_EC2_stats", db="MariaDB_EC2")),
    #("MariaDB_EC2 balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_MariaDB_EC2_stats", db="MariaDB_EC2")),
   
    ("MariaDB_EC2 wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_EC2_Wide_tree", database="MariaDB_EC2", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    #("MariaDB_EC2 deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_EC2_Deep_tree", database="MariaDB_EC2", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),
    #("MariaDB_EC2 balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_EC2_Balanced_tree", database="MariaDB_EC2", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions)),


]


for index, (title, experiment) in enumerate(experiments, start=1):

    print(f"Running {title} experiment {index} out of {len(experiments)}\n", end="\r")
    experiment()  # Calls the function

print(f"******** all {len(experiments)} have run ********")


