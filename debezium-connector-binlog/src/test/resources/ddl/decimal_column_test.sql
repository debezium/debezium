-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  decimal_column_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbz_751_decimal_column_test (
      id INT AUTO_INCREMENT NOT NULL,
      rating1 DECIMAL,
      rating2 DECIMAL(8, 4),
      rating3 DECIMAL(7),
      rating4 DECIMAL(6, 0),
      PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO dbz_751_decimal_column_test VALUES (default, 123, 123.4567, 234.5, 345.6);
