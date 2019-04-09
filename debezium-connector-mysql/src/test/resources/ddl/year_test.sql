-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  year_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbz_1143_year_test (
      id INT AUTO_INCREMENT NOT NULL,
      y18 YEAR,
      y0018 YEAR,
      y2018 YEAR,
      d18 DATE,
      d0018 DATE,
      d2018 DATE,
      dt18 DATETIME,
      dt0018 DATETIME,
      dt2018 DATETIME,
      y78 YEAR,
      y0078 YEAR,
      y1978 YEAR,
      d78 DATE,
      d0078 DATE,
      d1978 DATE,
      dt78 DATETIME,
      dt0078 DATETIME,
      dt1978 DATETIME,
     PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO dbz_1143_year_test VALUES (
    default,
    '18',
    '0018',
    '2018',
    '18-04-01',
    '0018-04-01',
    '2018-04-01',
    '18-04-01 12:34:56',
    '0018-04-01 12:34:56',
    '2018-04-01 12:34:56',
    '78',
    '0078',
    '1978',
    '78-04-01',
    '0078-04-01',
    '1978-04-01',
    '78-04-01 12:34:56',
    '0078-04-01 12:34:56',
    '1978-04-01 12:34:56'
);
