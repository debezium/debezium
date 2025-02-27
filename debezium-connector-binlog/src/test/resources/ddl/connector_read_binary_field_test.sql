-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  connector_read_binary_field_test
-- ----------------------------------------------------------------------------------------------------------------

-- Create a table, mainly MySQL time type fields
CREATE TABLE binary_field
(
    id             INT AUTO_INCREMENT PRIMARY KEY,
    now_time       TIME,
    now_date       DATE,
    now_date_time  DATETIME,
    now_time_stamp TIMESTAMP
) ENGINE = innodb
  AUTO_INCREMENT = 1
  DEFAULT CHARSET = utf8;

INSERT INTO binary_field
VALUES (default, now(), now(), now(), now());
