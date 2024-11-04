import create_wide_role_tree as wide
import create_deep_role_tree as deep
import create_balanced_role_tree as balanced
# starndard are rep: 2. time: 15
repetitions = 1
time_limit_minutes = 1



#wide.main(repetitions,time_limit_minutes,file_name="benchmark_wide_tree_snowflake_stats",db="Snowflake")
#deep.main(repetitions,time_limit_minutes,file_name="benchmark_deep_tree_snowflake_stats",db="Snowflake")
#balanced.main(repetitions,time_limit_minutes,file_name="benchmark_balanced_tree_snowflake_stats",db="Snowflake")
#wide.main(repetitions,time_limit_minutes,file_name="benchmark_wide_tree_postgresql_stats",db="PostgreSql")
#deep.main(repetitions,time_limit_minutes,file_name="benchmark_deep_tree_postgresql_stats",db="PostgreSql")
#balanced.main(repetitions,time_limit_minutes,file_name="benchmark_balanced_tree_postgresql_stats",db="PostgreSql")

# V V V TO run  ////   /\ /\ /\ have run
wide.main(repetitions,time_limit_minutes,file_name="benchmark_wide_tree_mariadb_stats",db="MariaDB")
deep.main(repetitions,time_limit_minutes,file_name="benchmark_deep_tree_mariadb_stats",db="MariaDB")
balanced.main(repetitions,time_limit_minutes,file_name="benchmark_balanced_tree_mariadb_stats",db="MariaDB")


