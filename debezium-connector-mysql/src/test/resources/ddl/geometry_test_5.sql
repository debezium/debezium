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
  expected_srid INT,
  PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;
INSERT INTO dbz_222_point VALUES (default,ST_GeomFromText('POINT(1 1)'), 1.0, 1.0, NULL);
INSERT INTO dbz_222_point VALUES (default,ST_GeomFromText('POINT(8.25554554 3.22124447)', 4326), 8.25554554, 3.22124447, 4326);
INSERT INTO dbz_222_point VALUES (default,ST_GeomFromText('POINT(0 0)', 1234), 0.0, 0.0, 1234);
INSERT INTO dbz_222_point VALUES (default,NULL, NULL, NULL, NULL);

CREATE TABLE dbz_507_geometry (
    id INT NOT NULL,
    geom GEOMETRY,
    linestring LINESTRING,
    polygon POLYGON,
    collection GEOMETRYCOLLECTION,
    PRIMARY KEY (id)
);
INSERT INTO dbz_507_geometry VALUES (1, ST_GeomFromText('POINT(1 1)', 4326), ST_GeomFromText('LINESTRING(0 0, 1 1)', 3187), ST_GeomFromText('POLYGON((0 0, 1 1, 1 0, 0 0))'), ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1))', 4326));
INSERT INTO dbz_507_geometry VALUES (2, ST_GeomFromText('LINESTRING(0 0, 1 1)'), NULL, NULL, NULL);
