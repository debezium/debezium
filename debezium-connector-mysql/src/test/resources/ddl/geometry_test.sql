-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  geometry_test
-- ----------------------------------------------------------------------------------------------------------------
-- The integration test for this database expects to scan all of the binlog events associated with this database
-- without error or problems. The integration test does not modify any records in this database, so this script
-- must contain all operations to these tables.
--
-- This relies upon MySQL 5.7's Geometries datatypes.

-- DBZ-222 handle POINT column types ...
CREATE TABLE dbz_222_point (
  id INT AUTO_INCREMENT NOT NULL,
  point POINT DEFAULT NULL,
  expected_x FLOAT,
  expected_y FLOAT,
  PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;
INSERT INTO dbz_222_point VALUES (default,GeomFromText('POINT(1 1)'), 1.0, 1.0);
INSERT INTO dbz_222_point VALUES (default,GeomFromText('POINT(8.25554554 3.22124447)'), 8.25554554, 3.22124447);
INSERT INTO dbz_222_point VALUES (default,GeomFromText('POINT(0 0)'), 0.0, 0.0);
INSERT INTO dbz_222_point VALUES (default,NULL, 0.0, 0.0);