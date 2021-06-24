-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  datetime_key_test
-- ----------------------------------------------------------------------------------------------------------------

SET sql_mode='';
CREATE TABLE dbz_1194_datetime_key_test (
      id INT AUTO_INCREMENT NOT NULL,
      dtval DATETIME NOT NULL,
      dval DATE NOT NULL,
      tval TIME NOT NULL,
      PRIMARY KEY (id, dtval, dval, tval)
) DEFAULT CHARSET=utf8;

INSERT INTO dbz_1194_datetime_key_test VALUES (default, '0000-00-00 00:00:00', '0000-00-00', '00:00:00');
