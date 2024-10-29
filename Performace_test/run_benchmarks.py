import create_wide_role_tree as wide
import create_deep_role_tree as deep
import create_balanced_role_tree as balanced
import utils as util

repetitions = 1
time_limit_minutes = 1



#wide.main(repetitions,time_limit_minutes,file_name="benchmark_wide_tree_snowflake_stats",db="Snowflake")
#deep.main(repetitions,time_limit_minutes,file_name="benchmark_deep_tree_snowflake_stats",db="Snowflake")
#balanced.main(repetitions,time_limit_minutes,file_name="benchmark_balanced_tree_snowflake_stats",db="Snowflake")

wide.main(repetitions,time_limit_minutes,file_name="benchmark_wide_tree_postgresql_stats",db="PostgreSql")
