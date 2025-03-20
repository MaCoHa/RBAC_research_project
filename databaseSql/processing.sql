
SELECT 
    ANY_VALUE(database) AS database,
    test_id, 
    MIN(start_time) AS min_start_time,
    ANY_VALUE(tree_type) AS tree_type
FROM processed_data
GROUP BY test_id;

SELECT * from processed_data;

SET Snowflake_wide = 599987540.1972; 
SET Snowflake_deep =602772842.011; 
SET Snowflake_balanced =771948308.2365; 

SET Postgresql_wide =4806227.3785; 
SET Postgresql_deep =6911718.6783; 
SET Postgresql_balanced =9021357.1884; 

SET mariadb_wide =88241182.7929; 
SET mariadb_deep =93864552.1271; 
SET mariadb_balanced =10; 

DROP TABLE IF EXISTS processed_temp_data_per_second;
CREATE TABLE processed_temp_data_per_second (
    test_id INT,
    query STRING,
    database STRING,
    tree_type STRING,
    role_number INT,
    identify STRING,
    second INT,
    average_time FLOAT
);


SELECT * from processed_temp_data_per_second where database = 'Snowflake';
-- Snowflake wide tree
INSERT INTO processed_temp_data_per_second (test_id, query, database, tree_type, role_number, identify, second, average_time)
SELECT 
    test_id,
    query,
    database,
    tree_type,
    role_number,
    CASE 
        WHEN query LIKE 'CREATE OR REPLACE%' THEN 'Wide_tree create role'
        WHEN query LIKE 'GRANT ROLE%' THEN 'Wide_tree grant role'
    END AS identify,
    FLOOR((start_time - $Snowflake_wide)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'Snowflake'
AND tree_type = 'Wide_tree';


-- Snowflake deep tree
INSERT INTO processed_temp_data_per_second (test_id, query, database, tree_type, role_number, identify, second, average_time)
SELECT 
    test_id,
    query,
    database,
    tree_type,
    role_number,
    CASE 
        WHEN query LIKE 'CREATE OR REPLACE%' THEN 'Deep_tree create role'
        WHEN query LIKE 'GRANT ROLE%' THEN 'Deep_tree grant role'
    END AS identify,
    FLOOR((start_time - $Snowflake_deep)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'Snowflake'
AND tree_type = 'Deep_tree';


-- Snowflake balanced tree
INSERT INTO processed_temp_data_per_second (test_id, query, database, tree_type, role_number, identify, second, average_time)
SELECT 
    test_id,
    query,
    database,
    tree_type,
    role_number,
    CASE 
        WHEN query LIKE 'CREATE OR REPLACE%' THEN 'Balanced_tree create role'
        WHEN query LIKE 'GRANT ROLE%' THEN 'Balanced_tree grant role'
    END AS identify,
    FLOOR((start_time - $Snowflake_balanced)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'Snowflake'
AND tree_type = 'balanced_tree';

-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************

-- PostgreSql wide tree
INSERT INTO processed_temp_data_per_second (test_id, query, database, tree_type, role_number, identify, second, average_time)
SELECT 
    test_id,
    query,
    database,
    tree_type,
    role_number,
    CASE 
        WHEN query LIKE 'CREATE%' THEN 'Wide_tree create role'
        WHEN query LIKE 'GRANT%' THEN 'Wide_tree grant role'
    END AS identify,
    FLOOR((start_time - $Postgresql_wide)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'PostgreSql'
AND tree_type = 'Wide_tree';


-- PostgreSql deep tree
INSERT INTO processed_temp_data_per_second (test_id, query, database, tree_type, role_number, identify, second, average_time)
SELECT 
    test_id,
    query,
    database,
    tree_type,
    role_number,
    CASE 
        WHEN query LIKE 'CREATE%' THEN 'Deep_tree create role'
        WHEN query LIKE 'GRANT%' THEN 'Deep_tree grant role'
    END AS identify,
    FLOOR((start_time - $Postgresql_deep)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'PostgreSql'
AND tree_type = 'Deep_tree';


-- PostgreSql balanced tree
INSERT INTO processed_temp_data_per_second (test_id, query, database, tree_type, role_number, identify, second, average_time)
SELECT 
    test_id,
    query,
    database,
    tree_type,
    role_number,
    CASE 
        WHEN query LIKE 'CREATE%' THEN 'Balanced_tree create role'
        WHEN query LIKE 'GRANT%' THEN 'Balanced_tree grant role'
    END AS identify,
    FLOOR((start_time - $Postgresql_balanced)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'PostgreSql'
AND tree_type = 'balanced_tree';


DROP TABLE IF EXISTS processed_data_per_second;

CREATE TABLE processed_data_per_second (
    test_id INT,
    database STRING,
    tree_type STRING,
    identify STRING,
    second INT,
    average_total_time FLOAT
);

INSERT INTO processed_data_per_second (
    test_id, database, tree_type, identify, second, average_total_time
)
SELECT 
    test_id,
    database,
    tree_type,
    identify,
    second,
    AVG(average_time) AS average_total_time
FROM processed_temp_data_per_second
GROUP BY test_id,database, tree_type, identify, second;



SELECT * FROM processed_data_per_second WHERE
database = 'Snowflake'
AND SECOND >= 600;

SELECT * FROM processed_data_per_second WHERE 
database = 'PostgreSql'
AND SECOND >= 600;


