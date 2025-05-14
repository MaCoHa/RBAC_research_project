

SELECT DISTINCT(database) FROM EXPERIMENT_UNPROCESSED;
-- MariaDB_EC2
-- MariaDB
-- PostgreSql
-- PostgreSql_EC2
-- Snowflake
-- 
SET database_varible ='MariaDB_EC2'; 
SET database_varible ='MariaDB'; 
SET database_varible ='PostgreSql'; 
SET database_varible ='PostgreSql_EC2'; 
SET database_varible ='Snowflake'; 

SET tree_type_varible = 'Deep_tree';
SET tree_type_varible = 'Wide_tree';
SET tree_type_varible = 'balanced_tree';


Select * FROM EXPERIMENT_UNPROCESSED;

SELECT (end_time-start_time)/1000 as time,repetition,role_number FROM EXPERIMENT_UNPROCESSED
WHERE DATABASE LIKE $database_varible
AND TREE_TYPE Like $tree_type_varible
AND QUERY LIKE 'CREATE%'
ORDER BY ROLE_NUMBER
LIMIT 10000;


SELECT (end_time-start_time)/1000 as time,repetition,role_number FROM EXPERIMENT_UNPROCESSED
WHERE DATABASE LIKE $database_varible
AND TREE_TYPE Like $tree_type_varible
AND QUERY LIKE 'GRANT%'
ORDER BY ROLE_NUMBER
LIMIT 10000;


SELECT (end_time-start_time)/1000 as time,repetition,role_number  FROM STAR_EXPERIMENT_UNPROCESSED
WHERE DATABASE LIKE $database_varible
AND TREE_TYPE Like $tree_type_varible
AND QUERY LIKE '%FROM foo%'
ORDER BY ROLE_NUMBER;

SELECT (end_time-start_time)/1000 as time,repetition,role_number   FROM STAR_EXPERIMENT_UNPROCESSED
WHERE DATABASE LIKE $database_varible
AND TREE_TYPE Like $tree_type_varible
AND QUERY LIKE '%APPLICABLE_ROLES%'
ORDER BY ROLE_NUMBER;

SELECT (end_time-start_time)/1000 as time,repetition,role_number   FROM STAR_EXPERIMENT_UNPROCESSED
WHERE DATABASE LIKE $database_varible
AND TREE_TYPE Like $tree_type_varible
ORDER BY ROLE_NUMBER
AND QUERY LIKE '%SHOW ROLES%';

SELECT (end_time-start_time)/1000 as time,repetition,role_number   FROM STAR_EXPERIMENT_UNPROCESSED
WHERE DATABASE LIKE $database_varible
AND TREE_TYPE Like $tree_type_varible
ORDER BY ROLE_NUMBER
AND QUERY LIKE '%TABLE_PRIVILEGES%';

SELECT (end_time-start_time)/1000 as time,repetition,role_number   FROM STAR_EXPERIMENT_UNPROCESSED
WHERE DATABASE LIKE $database_varible
AND TREE_TYPE Like $tree_type_varible
ORDER BY ROLE_NUMBER
AND QUERY LIKE '%ENABLED_ROLES%';

