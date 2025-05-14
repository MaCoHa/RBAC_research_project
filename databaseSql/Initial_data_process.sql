Drop TABLE experiment_unprocessed;

CREATE or REPLACE TABLE experiment_unprocessed (
    test_id INT,
    query STRING,
    database STRING,
    tree_type STRING,
    repetition INT,
    role_number INT,
    start_time FLOAT,
    end_time FLOAT);
    
COPY INTO experiment_unprocessed (test_id, query,database,tree_type,repetition,role_number,start_time, end_time)  -- Specify the exact column names in the table
FROM @BlueJayStage
FILE_FORMAT = (
TYPE = 'CSV' 
FIELD_DELIMITER = ';'
SKIP_HEADER = 1);




drop table if exists data_rep1;
CREATE TABLE data_rep1 (
    test_id VARCHAR,
    query VARCHAR,
    database VARCHAR,
    tree_type VARCHAR,
    role_number INT,
    start_time DOUBLE,
    end_time DOUBLE
 );

drop table if exists data_rep2;
 
 CREATE TABLE data_rep2 (
    test_id VARCHAR,
    query VARCHAR,
    database VARCHAR,
    tree_type VARCHAR,
    role_number INT,
    start_time DOUBLE,
    end_time DOUBLE
 );

 
DROP TABLE if exists processed_data;
  CREATE TABLE processed_data (
    test_id VARCHAR,
    query VARCHAR,
    database VARCHAR,
    tree_type VARCHAR,
    role_number INT,
    start_time DOUBLE,
    end_time DOUBLE,
    average_time DOUBLE 
 );



INSERT INTO data_rep1 (test_id, query, database, tree_type, role_number, start_time, end_time)
SELECT 
    row1.test_id,
    row1.query,
    row1.database,
    row1.tree_type,
    row1.role_number,
    row1.start_time,
    row1.end_time,
FROM 
    experiment_unprocessed AS row1
WHERE
    row1.repetition = 0;

INSERT INTO data_rep2 (test_id, query, database, tree_type, role_number, start_time, end_time)
SELECT 
    row1.test_id,
    row1.query,
    row1.database,
    row1.tree_type,
    row1.role_number,
    row1.start_time,
    row1.end_time,
FROM 
    experiment_unprocessed AS row1
WHERE
    row1.repetition = 1;



INSERT INTO processed_data (test_id, query, database, tree_type, role_number, start_time, end_time, average_time)
SELECT 
    COALESCE(row1.test_id, row2.test_id) AS test_id,
    COALESCE(row1.query, row2.query) AS query,
    COALESCE(row1.database, row2.database) AS database,
    COALESCE(row1.tree_type, row2.tree_type) AS tree_type,
    COALESCE(row1.role_number, row2.role_number) AS role_number,
    COALESCE(row1.start_time, row2.start_time) AS start_time,
    COALESCE(row1.end_time, row2.end_time) AS end_time,
    CASE 
        WHEN row1.start_time IS NOT NULL AND row2.start_time IS NOT NULL THEN 
            ( (row1.end_time - row1.start_time) + (row2.end_time - row2.start_time) ) / 2
        ELSE 
            COALESCE(row1.end_time - row1.start_time, row2.end_time - row2.start_time)
    END AS average_time
FROM 
    data_rep1 AS row1
LEFT JOIN 
    data_rep2 AS row2
ON 
    row1.test_id = row2.test_id
    AND row1.query = row2.query
    AND row1.role_number = row2.role_number;



