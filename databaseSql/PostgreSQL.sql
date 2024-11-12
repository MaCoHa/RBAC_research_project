SELECT * FROM pg_roles;




drop table Foo;

CREATE TABLE FOO (
    website_name VARCHAR(25) NOT NULL,
    server_name VARCHAR(20),
    creation_date DATE
);






INSERT INTO FOO (website_name, server_name, creation_date)
VALUES ('example.com', 'server01', '2024-11-06');


 set role Role0;
 drop role research1;
 

 revoke research1 from ROLE0;
 GRANT research1 TO ROLE0;

 GRANT  ALL PRIVILEGES  ON  FOO TO  research1;
 REVOKE  ALL PRIVILEGES  ON  FOO FROM  research1;


 GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO research1;
 REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM research1;

 select * from FOO; 

SELECT *
  FROM information_schema.role_table_grants;

 show role;

set role postgres;
set role research1
  