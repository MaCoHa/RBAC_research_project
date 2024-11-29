DROP role Role18757;

flush privileges;

CREATE ROLE Role6;
CREATE ROLE research2;

GRANT research1 TO research2;

SELECT * FROM information_schema.applicable_roles where GRANTEE like "root%";

SELECT * FROM INFORMATION_SCHEMA.APPLICABLE_ROLES;
SELECT * FROM INFORMATION_SCHEMA.ENABLED_ROLES;
SELECT * FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES;


SHOW GRANTS FOR research1;



drop table Foo;

CREATE TABLE FOO (
    website_name VARCHAR(25) NOT NULL,
    server_name VARCHAR(20),
    creation_date DATE
);

INSERT INTO FOO (website_name, server_name, creation_date)
VALUES ('example.com', 'server01', '2024-11-06');

SELECT * FROM FOO;
