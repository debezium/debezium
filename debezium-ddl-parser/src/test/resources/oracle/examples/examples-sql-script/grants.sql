-- 4 Roles for scott
GRANT DEFAULT_ROLE TO scott WITH ADMIN OPTION;
GRANT CONNECT TO scott;
GRANT USR_DEFAULT_CONNECT TO scott WITH ADMIN OPTION;
ALTER USER scott DEFAULT ROLE CONNECT;
 -- 3 System Privileges for scott
GRANT ALTER USER TO scott;
GRANT CREATE TABLE TO scott;
GRANT CREATE USER TO scott;
 -- 6 Object Privileges for scott
GRANT INSERT, SELECT ON example.green_table TO scott;
GRANT ALL ON example.blue_table TO scott;
-- In block
BEGIN
    GRANT ALL ON employees TO john;
END;