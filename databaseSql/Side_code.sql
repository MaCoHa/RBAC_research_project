create database RBAC_experiments;

use RBAC_experiments;

create table foo (
name STRING,
age INTEGER
);

insert into foo (name,age)
VALUES ('person1',26);
insert into foo (name,age)
VALUES ('person2',28);
insert into foo (name,age)
VALUES ('person3',30);

SELECT * from foo;

create Role Role0;

USE role Role0;

GRANT ROLE Role0 TO Role ACCOUNTADMIN;

show roles;
drop role ROLE10;

SELECT CURRENT_REGION();