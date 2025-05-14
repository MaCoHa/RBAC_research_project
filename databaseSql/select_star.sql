COPY INTO star_experiment_unprocessed (test_id, query,database,tree_type,role_number,repetition,start_time, end_time)  -- Specify the exact column names in the table
FROM @BlueJayStarStage
FILE_FORMAT = (
TYPE = 'CSV' 
FIELD_DELIMITER = ';'
SKIP_HEADER = 1);

Drop TABLE star_experiment_unprocessed;
CREATE or REPLACE TABLE star_experiment_unprocessed (
    test_id INT,
    query STRING,
    database STRING,
    tree_type STRING,
    role_number INT,
    repetition INT,
    start_time FLOAT,
    end_time FLOAT);


Drop TABLE star_experiment_temp_unprocessed;
CREATE or REPLACE TABLE star_experiment_temp_unprocessed (
    test_id INT,
    query STRING,
    database STRING,
    tree_type STRING,
    role_number INT,
    repetition INT,
    time FLOAT);


Drop TABLE star_experiment_processed;
CREATE or REPLACE TABLE star_experiment_processed (
    test_id INT,
    query STRING,
    database STRING,
    tree_type STRING,
    role_number INT,
    median_time FLOAT,
    avearage FLOAT);


INSERT INTO star_experiment_temp_unprocessed (test_id, query, database, tree_type, role_number,repetition, time)
SELECT 
    test_id,
    query,
    database,
    tree_type,
    role_number,
    repetition,
    end_time - start_time as time
FROM star_experiment_unprocessed;


INSERT INTO star_experiment_processed (test_id, query, database, tree_type, role_number,median_time, avearage)
SELECT 
    test_id,
    query,
    database,
    tree_type,
    role_number,
    MEDIAN(time) as median_time,
    AVG(time) as avearage
FROM star_experiment_temp_unprocessed
GROUP BY test_id,query,database,tree_type,role_number;



SELECT * 
FROM star_experiment_processed
Where QUERY LIKE 'SELECT * FROM foo'
ORDER BY ROLE_NUMBER,TREE_TYPE,DATABASE;

SELECT * 
FROM star_experiment_processed
Where QUERY LIKE 'SHOW ROLES'
ORDER BY ROLE_NUMBER,TREE_TYPE,DATABASE;


SELECT * 
FROM star_experiment_processed
Where QUERY LIKE 'SELECT * FROM INFORMATION_SCHEMA.APPLICABLE_ROLES'
ORDER BY ROLE_NUMBER,TREE_TYPE,DATABASE;


SELECT * 
FROM star_experiment_processed
Where QUERY LIKE 'SELECT * FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES'
ORDER BY ROLE_NUMBER,TREE_TYPE,DATABASE;


SELECT * 
FROM star_experiment_processed
Where QUERY LIKE 'SELECT * FROM INFORMATION_SCHEMA.ENABLED_ROLES'
ORDER BY ROLE_NUMBER,TREE_TYPE,DATABASE;







SELECT * FROM STAR_EXPERIMENT_PROCESSED
WHERE QUERY LIKE 'SELECT *%'
OR QUERY LIKE 'SHOW%'
ORDER BY QUERY,DATABASE,TREE_TYPE,ROLE_NUMBER;

