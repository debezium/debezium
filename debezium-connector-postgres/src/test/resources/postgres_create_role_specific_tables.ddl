-- Reset test environment
DROP PUBLICATION IF EXISTS dbz_publication;
DROP SCHEMA IF EXISTS s1 CASCADE;
DROP SCHEMA IF EXISTS s2 CASCADE;

-- ...drop role_1
DO
$do$
BEGIN
   IF EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'role_1') THEN

      REASSIGN OWNED BY role_1 TO postgres;
      DROP OWNED BY role_1;
      DROP ROLE IF EXISTS role_1;
   END IF;
END
$do$;


-- ... drop role_2
DO
$do$
BEGIN
   IF EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'role_2') THEN

      REASSIGN OWNED BY role_2 TO postgres;
      DROP OWNED BY role_2;
      DROP ROLE IF EXISTS role_2;
   END IF;
END
$do$;


-- Create schema
CREATE SCHEMA s1;
CREATE SCHEMA s2;

-- Create roles
CREATE ROLE role_1;
GRANT ALL ON SCHEMA s1 TO role_1;
GRANT ALL ON SCHEMA s2 TO role_1;
GRANT CREATE ON DATABASE postgres TO role_1;

CREATE ROLE role_2 WITH REPLICATION LOGIN PASSWORD 'role_2_pass';
GRANT ALL ON SCHEMA s1 TO role_2;
GRANT ALL ON SCHEMA s2 TO role_2;
GRANT CONNECT ON DATABASE postgres TO role_2;

-- Create tables using r1
SET ROLE role_1;
CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));
CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));
CREATE PUBLICATION dbz_publication FOR TABLE s1.a, s2.a;
RESET ROLE;