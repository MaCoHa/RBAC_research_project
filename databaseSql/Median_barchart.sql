
SELECT * FROM PROCESSED_DATA_PER_SECOND WHERE Tree_type like 'Wide_tree';

DROP TABLE IF EXISTS median_table;
CREATE TABLE median_table (
    database STRING,
    identify STRING,
    median_val FLOAT  
);



INSERT INTO median_table (
    database, identify, median_val
)
SELECT  
    database,
    identify,
    MEDIAN(MEDIAN_TIME)
FROM processed_data_per_second
GROUP BY database, tree_type, identify;

SELECT * FROM median_table;

SELECT * FROM median_table
WHERE IDENTIFY NOT LIKE '%drop%'
AND IDENTIFY LIKE '%grant%'
AND DATABASE LIKE 'PostgreS%'
ORDER BY IDENTIFY, DATABASE;


SELECT * FROM median_table
WHERE IDENTIFY NOT LIKE '%drop%'
AND IDENTIFY LIKE '%create%'
ORDER BY IDENTIFY, DATABASE;
