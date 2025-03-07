import create_wide_role_tree as wide
import create_deep_role_tree as deep
import create_balanced_role_tree as balanced
import Select_tests.select_star as select
# starndard are rep: 2. time: 15
repetitions = 2
time_limit_minutes = 15

### Snowflake

### ************** have runn ************************
wide.main(repetitions,time_limit_minutes,file_name="benchmark_wide_tree_snowflake_stats",db="Snowflake")

### ************** needs to runn ************************
#deep.main(repetitions,time_limit_minutes,file_name="benchmark_deep_tree_snowflake_stats",db="Snowflake")
#balanced.main(repetitions,time_limit_minutes,file_name="benchmark_balanced_tree_snowflake_stats",db="Snowflake")

#select.main(file_name="benchmark_select_star_Snowflake_Wide_tree",database="Snowflake",tree_type="Wide_tree",time_limit_minutes = time_limit_minutes)
#select.main(file_name="benchmark_select_star_Snowflake_Deep_tree",database="Snowflake",tree_type="Deep_tree",time_limit_minutes = time_limit_minutes)
#select.main(file_name="benchmark_select_star_Snowflake_Balanced_tree",database="Snowflake",tree_type="Balanced_tree",time_limit_minutes = time_limit_minutes)



### PostgreSQL

#wide.main(repetitions,time_limit_minutes,file_name="benchmark_wide_tree_postgresql_stats",db="PostgreSql")
#deep.main(repetitions,time_limit_minutes,file_name="benchmark_deep_tree_postgresql_stats",db="PostgreSql")
#balanced.main(repetitions,time_limit_minutes,file_name="benchmark_balanced_tree_postgresql_stats",db="PostgreSql")

### MariaDB

#wide.main(repetitions,time_limit_minutes,file_name="benchmark_wide_tree_mariadb_stats",db="MariaDB")
#deep.main(repetitions,time_limit_minutes,file_name="benchmark_deep_tree_mariadb_stats",db="MariaDB")
#balanced.main(repetitions,time_limit_minutes,file_name="benchmark_balanced_tree_mariadb_stats",db="MariaDB")





