from benchmark.create_processed import plot
import create_wide_role_tree as wide
import create_deep_role_tree as deep
import create_balanced_role_tree as balanced
import Select_tests.select_star as select
import connection_testing as testing
import setup as setup
# predefined parameters for the grant and create role experiments
repetitions = 2
time_limit_minutes = 15

# predefined parameters for the select star experiments
select_repetitions = 5
tree_sizes = [1000,10_000,100_000]
table = "foo"





    # This list contains all the experiments to run.
    # Each experiment is a tuple with a title and a lambda function that runs the experiment.
    # When running specific experiments, you can comment out the ones you don't want to run.
experiments = [

    # Running the Local driver snowflake experiment for grant and create role
    ("Snowflake wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_snowflake_stats", db="Snowflake")),
    ("Snowflake deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_snowflake_stats", db="Snowflake")),
    ("Snowflake balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_snowflake_stats", db="Snowflake")),
    
    #Running the Local driver snowflake experiment for select star
    ("Snowflake wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_Wide_tree", database="Snowflake", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("Snowflake deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_Deep_tree", database="Snowflake", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("Snowflake balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_Balanced_tree", database="Snowflake", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),


    # Running the Cloud driver snowflake experiment for grant and create role
    # No difference in the code, but different logging 
    ("Snowflake_EC2 wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_Snowflake_EC2_stats", db="Snowflake_EC2")),
    ("Snowflake_EC2 deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_Snowflake_EC2_stats", db="Snowflake_EC2")),
    ("Snowflake_EC2 balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_Snowflake_EC2_stats", db="Snowflake_EC2")),
    
    #Running the Cloud driver snowflake experiment for select star
    # No difference in the code, but different logging
    ("Snowflake_EC2 wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_EC2_Wide_tree", database="Snowflake_EC2", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("Snowflakedeep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_EC2_Deep_tree", database="Snowflake_EC2", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("Snowflake_EC2 balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_Snowflake_EC2_Balanced_tree", database="Snowflake_EC2", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),

    # Running the Local PostgreSQL experiment for grant and create role
    ("PostgreSQL wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_postgresql_stats_error", db="PostgreSql")),
    ("PostgreSQL deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_postgresql_stats_error", db="PostgreSql")),
    ("PostgreSQL balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_postgresql_stats_error", db="PostgreSql")),
   
    #Running the Local PostgreSQL experiment for select star
    ("PostgreSQL wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_Wide_tree", database="PostgreSql", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("PostgreSQL deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_Deep_tree", database="PostgreSql", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("PostgreSQL balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_Balanced_tree", database="PostgreSql", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),

    # Running the Local MariaDB experiment for grant and create role
    ("MariaDB wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_mariadb_stats", db="MariaDB")),
    ("MariaDB deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_mariadb_stats", db="MariaDB")),
    ("MariaDB balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_mariadb_stats", db="MariaDB")),
   
    #Running the Local MariaDB experiment for select star
    ("MariaDB wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_Wide_tree", database="MariaDB", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("MariaDB deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_Deep_tree", database="MariaDB", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("MariaDB balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_Balanced_tree", database="MariaDB", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),


    # Running the Setupd for running Cloud PostgreSQL
    ("PostgreSQL EC2 Setup the database", lambda: setup.main(db="PostgreSql_EC2")),

    # Running the Cloud PostgreSQL experiment for grant and create role
    # The conncetion and logging is different, but the code is the same 
    ("PostgreSQL EC2 wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_postgresql_ec2_stats", db="PostgreSql_EC2")),
    ("PostgreSQL EC2 deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_postgresql_ec2_Big_stats", db="PostgreSql_EC2")),
    ("PostgreSQL EC2 balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_postgresql_ec2_Big_stats", db="PostgreSql_EC2")),
   
    # Running the Cloud PostgreSQL experiment for select star
    # The conncetion and logging is different, but the code is the same
    ("PostgreSQL EC2 wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_ec2_Big_Wide_tree", database="PostgreSql_EC2", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("PostgreSQL EC2 deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_ec2_Big_Deep_tree", database="PostgreSql_EC2", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("PostgreSQL EC2 balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_postgresql_ec2_Big_Balanced_tree", database="PostgreSql_EC2", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),

    # Running the Setup for running Cloud MariaDB
    ("MariaDB_EC2 EC2 Setup the database", lambda: setup.main(db="MariaDB_EC2")),

    # Running the Cloud MariaDB experiment for grant and create role
    # The conncetion and logging is different, but the code is the same
    ("MariaDB_EC2 wide_tree Create Test", lambda: wide.main(repetitions, time_limit_minutes, file_name="benchmark_wide_tree_MariaDB_EC2_stats", db="MariaDB_EC2")),
    ("MariaDB_EC2 deep_tree Create Test", lambda: deep.main(repetitions, time_limit_minutes, file_name="benchmark_deep_tree_MariaDB_EC2_stats", db="MariaDB_EC2")),
    ("MariaDB_EC2 balanced_tree Create Test", lambda: balanced.main(repetitions, time_limit_minutes, file_name="benchmark_balanced_tree_MariaDB_EC2_stats", db="MariaDB_EC2")),
   
    # Running the Cloud MariaDB experiment for select star
    # The conncetion and logging is different, but the code is the same
    ("MariaDB_EC2 wide_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_EC2_Wide_tree", database="MariaDB_EC2", tree_type="Wide_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("MariaDB_EC2 deep_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_EC2_Deep_tree", database="MariaDB_EC2", tree_type="Deep_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),
    ("MariaDB_EC2 balanced_tree Select Test", lambda: select.main(file_name="benchmark_select_star_MariaDB_EC2_Balanced_tree", database="MariaDB_EC2", tree_type="Balanced_tree", time_limit_minutes=time_limit_minutes,repetitions=select_repetitions,tree_sizes=tree_sizes,table=table)),




    # This is Code for plotting the prossed data from the experiments
    """
    ("Create Snowflake data graphs", lambda: plot.old_plot_line_graph("Snowflake median grant and create Role over 15 min","Snowflake_grant_data.csv","Snowflake_grant_data")),
    ("Create PostgreSql data graphs", lambda: plot.old_plot_line_graph("PostgreSql median grant and create Role over 15 min","PostgreSql_grant_data.csv","PostgreSql_grant_data")),
    ("Create MariaDb data graphs", lambda: plot.plot_line_graph("MariaDb median grant and create Role over 15 min","MariaDb_grant_data.csv","MariaDb_grant_data")),
    ("Create Snowflake_EC2 data graphs", lambda: plot.old_plot_line_graph("Snowflake_EC2 median grant and create Role over 15 min","Snowflake_EC2_grant_data.csv","Snowflake_EC2_grant_data")),
    ("Create PostgreSql_EC2 data graphs", lambda: plot.plot_line_graph("PostgreSql_EC2 median grant and create Role over 15 min","PostgreSql_EC2_grant_data.csv","PostgreSql_EC2_grant_data")),
    ("Create MariaDb_EC2 data graphs", lambda: plot.plot_line_graph("MariaDb_EC2 median grant and create Role over 15 min","MariaDb_EC2_grant_data.csv","MariaDb_EC2_grant_data")),
    ("Create PostgreSql_EC2_big data graphs", lambda: plot.plot_line_graph("PostgreSql_EC2 median grant and create Role over 15 min","PostgreSql_EC2_5d_large_grant_data.csv","PostgreSql_EC2_5d_large_grant_data")),
    
    ("Create Local Iops data graphs", lambda: plot.plot_local_iops()),
    ("Create Online Iops data graphs", lambda: plot.plot_online_iops()),
    """
    

    




]


for index, (title, experiment) in enumerate(experiments, start=1):

    print(f"Running {title} experiment {index} out of {len(experiments)}\n", end="\r")
    experiment()  # Calls the function

print(f"******** all {len(experiments)} have run ********")


