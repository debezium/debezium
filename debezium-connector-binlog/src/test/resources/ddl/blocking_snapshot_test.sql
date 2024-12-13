-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  incremental_snapshot_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE a (
  pk INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  aa INTEGER
) AUTO_INCREMENT = 1;

CREATE TABLE b (
  pk INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  aa INTEGER
) AUTO_INCREMENT = 1;

CREATE TABLE debezium_signal (
  id varchar(64),
  type varchar(32),
  data varchar(2048)
);

DROP DATABASE IF EXISTS emptydb;
CREATE DATABASE emptydb;
