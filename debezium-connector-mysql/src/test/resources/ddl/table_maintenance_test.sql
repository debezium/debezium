-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  table_maintenance_test
-- ----------------------------------------------------------------------------------------------------------------
-- The integration test for this database expects to scan all of the binlog events associated with this database
-- without error or problems. The integration test does not modify any records in this database, so this script
-- must contain all operations to these tables.
--

CREATE TABLE dbz_253_table_maintenance_test (
      id INT AUTO_INCREMENT NOT NULL,
      other int,
      PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

ANALYZE TABLE dbz_253_table_maintenance_test;
OPTIMIZE TABLE dbz_253_table_maintenance_test;
REPAIR TABLE dbz_253_table_maintenance_test;