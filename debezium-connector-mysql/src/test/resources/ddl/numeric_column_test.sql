-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  numeric_column_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbz_751_numeric_column_test (
      id INT AUTO_INCREMENT NOT NULL,
      rating1 NUMERIC,
      rating2 NUMERIC(8, 4),
      rating3 NUMERIC(7),
      rating4 NUMERIC(6, 0),
      PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO dbz_751_numeric_column_test VALUES (default, 123, 123.4567, 234.5, 345.6);
