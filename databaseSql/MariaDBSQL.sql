DROP role Role18757;

flush privileges;

CREATE ROLE Role6;
CREATE ROLE research2;

GRANT research1 TO research2;

SELECT * FROM information_schema.applicable_roles where GRANTEE like "root%";


SHOW GRANTS FOR research1;