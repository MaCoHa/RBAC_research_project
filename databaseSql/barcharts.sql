SELECT 
    database,
    tree_type,
    MAX(role_number) AS max_role_number
FROM experiment_unprocessed
GROUP BY database, tree_type
ORDER BY TREE_TYPE, database;


SELECT 
    test_id,
    MAX(role_number) AS max_role_number
FROM experiment_unprocessed
WHERE test_id LIKE '20250314112115'
OR test_id LIKE '20250425140555'
GROUP BY test_id;
