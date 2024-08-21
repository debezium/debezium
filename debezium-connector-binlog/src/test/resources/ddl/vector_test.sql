-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  vector_test
-- ----------------------------------------------------------------------------------------------------------------
-- The integration test for this database expects to scan all of the binlog events associated with this database
-- without error or problems. The integration test does not modify any records in this database, so this script
-- must contain all operations to these tables.
--
-- This relies upon MySQL 9.0's vector datatypes.

CREATE TABLE dbz_8157 (
  id INT AUTO_INCREMENT NOT NULL,
  f_vector_null VECTOR DEFAULT NULL,
  f_vector_default VECTOR DEFAULT NULL,
  f_vector_cons VECTOR(128) DEFAULT NULL,
  PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;
INSERT INTO dbz_8157 VALUES (default, string_to_vector('[1.1,2.2]'),string_to_vector('[11.5,22.6]'),string_to_vector('[31,32]'));
