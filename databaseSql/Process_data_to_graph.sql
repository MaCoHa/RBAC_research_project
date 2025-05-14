SELECT * 
FROM processed_data
WHERE (DATABASE LIKE 'PostgreSql%' OR DATABASE LIKE 'MariaDB%')
  AND ROLE_NUMBER = 23893
  AND TREE_TYPE LIKE 'Wide_tree'
  AND QUERY LIKE 'GRANT%';


SELECT * FROM processed_data_per_second
WHERE DATABASE LIKE

SELECT DISTINCT(query) FROM processed_data;

SELECT 
    ANY_VALUE(database) AS database,
    test_id, 
    MIN(start_time) AS min_start_time,
    ANY_VALUE(tree_type) AS tree_type
FROM processed_data
GROUP BY test_id
Order By database;


SET Snowflake_wide = 599987540.1972; 
SET Snowflake_deep =602772842.011; 
SET Snowflake_balanced =771948308.2365; 

-- down scaled local experiment
--SET Postgresql_wide =4005267.7681; 

SET Postgresql_wide =4806227.3785; 
SET Postgresql_deep =6911718.6783; 
SET Postgresql_balanced =9021357.1884; 

SET Mariadb_wide =88241182.7929; 
SET Mariadb_deep =93864552.1271; 
SET Mariadb_balanced =272273302.7947; 


SET Mariadb_EC2_wide =1678905.188008; 
SET Mariadb_EC2_deep =11186143.3543; 
SET Mariadb_EC2_balanced =17393089.280905;

-- Bigger hardware test
SET Postgresql_EC2_wide =718144.328991; 
SET Postgresql_EC2_deep =4238406.45062; 
SET Postgresql_EC2_balanced =6125754.212215; 

-- Smaller hardware test
--SET Postgresql_EC2_wide =1603717.574688; 
--SET Postgresql_EC2_deep =7347080.805076; 
--SET Postgresql_EC2_balanced =9234431.48123; 

SET Snowflake_EC2_wide = 1033693.749775; 
SET Snowflake_EC2_deep =3764397.576671; 
SET Snowflake_EC2_balanced =6429986.723072; 

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


-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************

-- MariaDB wide tree
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
    FLOOR((start_time - $Mariadb_wide)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'MariaDB'
AND tree_type = 'Wide_tree';


-- MariaDB deep tree
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
    FLOOR((start_time - $Mariadb_deep)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'MariaDB'
AND tree_type = 'Deep_tree';


-- MariaDB balanced tree
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
        WHEN query LIKE 'DROP%' THEN 'Balanced_tree drop role'
    END AS identify,
    FLOOR((start_time - $Mariadb_balanced)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'MariaDB'
AND tree_type = 'balanced_tree';



-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************

-- MariaDB_EC2 wide tree
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
        WHEN query LIKE 'DROP%' THEN 'Wide_tree drop role'
        
    END AS identify,
    FLOOR((start_time - $Mariadb_EC2_wide)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'MariaDB_EC2'
AND tree_type = 'Wide_tree';


-- MariaDB_EC2 deep tree
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
        WHEN query LIKE 'DROP%' THEN 'Deep_tree drop role'
        
    END AS identify,
    FLOOR((start_time - $Mariadb_EC2_deep)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'MariaDB_EC2'
AND tree_type = 'Deep_tree';


-- MariaDB_EC2 balanced tree
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
        WHEN query LIKE 'DROP%' THEN 'Balanced_tree drop role'
    END AS identify,
    FLOOR((start_time - $Mariadb_EC2_balanced)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'MariaDB_EC2'
AND tree_type = 'balanced_tree';




-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************

-- PostgreSQL_EC2 wide tree
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
        WHEN query LIKE 'DROP%' THEN 'Wide_tree drop role'
        
    END AS identify,
    FLOOR((start_time - $Postgresql_EC2_wide)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'PostgreSql_EC2'
AND tree_type = 'Wide_tree';


-- PostgreSQL_EC2 deep tree
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
        WHEN query LIKE 'DROP%' THEN 'Deep_tree drop role'
        
    END AS identify,
    FLOOR((start_time - $Postgresql_EC2_deep)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'PostgreSql_EC2'
AND tree_type = 'Deep_tree';


-- PostgreSQL_EC2 balanced tree
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
        WHEN query LIKE 'DROP%' THEN 'Balanced_tree drop role'
    END AS identify,
    FLOOR((start_time - $Postgresql_EC2_balanced)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'PostgreSql_EC2'
AND tree_type = 'balanced_tree';




-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************


-- Snowflake_EC2 wide tree
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
        WHEN query LIKE 'DROP%' THEN 'Wide_tree drop role'
        
    END AS identify,
    FLOOR((start_time - $Snowflake_EC2_wide)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'Snowflake_EC2'
AND tree_type = 'Wide_tree';


-- Snowflake_EC2 deep tree
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
        WHEN query LIKE 'DROP%' THEN 'Deep_tree drop role'
        
    END AS identify,
    FLOOR((start_time - $Snowflake_EC2_deep)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'Snowflake_EC2'
AND tree_type = 'Deep_tree';


-- Snowflake_EC2 balanced tree
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
        WHEN query LIKE 'DROP%' THEN 'Balanced_tree drop role'
    END AS identify,
    FLOOR((start_time - $Snowflake_EC2_balanced)/1000) AS second,
    average_time
FROM processed_data
WHERE database = 'Snowflake_EC2'
AND tree_type = 'balanced_tree';




-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************
-- *************************************************************************************************************






DROP TABLE IF EXISTS processed_data_per_second;
CREATE TABLE processed_data_per_second (
    test_id INT,
    database STRING,
    tree_type STRING,
    identify STRING,
    second INT,
    median_time FLOAT
);

INSERT INTO processed_data_per_second (
    test_id, database, tree_type, identify, second, median_time
)
SELECT 
    test_id,
    database,
    tree_type,
    identify,
    second,
    MEDIAN(average_time) AS median_time
FROM processed_temp_data_per_second
GROUP BY test_id,database, tree_type, identify, second;



SELECT * FROM processed_data_per_second WHERE
database = 'Snowflake';
AND SECOND >= 600;

SELECT * FROM processed_data_per_second WHERE 
database = 'PostgreSql'
AND SECOND >= 600;

SELECT * FROM processed_data_per_second WHERE 
database = 'MariaDB'
AND SECOND >= 600
AND SECOND <= 900;

SELECT * FROM processed_data_per_second WHERE 
database = 'PostgreSql_EC2'
AND SECOND >= 600
AND SECOND <= 900;

SELECT * FROM processed_data_per_second WHERE 
database = 'MariaDB_EC2'
AND SECOND >= 600
AND SECOND <= 900;

SELECT * FROM processed_data_per_second WHERE 
database = 'Snowflake_EC2';
AND SECOND >= 600
AND SECOND <= 900
AND MEDIAN_TIME <= 1500;

SELECT DISTINCT(Identify) FROm processed_data_per_second;





SELECT * FROM processed_data_per_second WHERE
database = 'Snowflake';

SELECT * FROM processed_data_per_second WHERE 
database = 'PostgreSql';

SELECT * FROM processed_data_per_second WHERE 
database = 'MariaDB';

SELECT * FROM processed_data_per_second WHERE 
database = 'PostgreSql_EC2';

SELECT * FROM processed_data_per_second WHERE 
database = 'MariaDB_EC2';

SELECT * FROM processed_data_per_second WHERE 
database = 'Snowflake_EC2';

