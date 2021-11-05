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

CREATE TABLE a4 (
  pk1 integer,
  pk2 integer,
  pk3 integer,
  pk4 integer,
  aa integer,
  PRIMARY KEY(pk1, pk2, pk3, pk4)
);

CREATE TABLE a42 (
  pk1 integer,
  pk2 integer,
  pk3 integer,
  pk4 integer,
  aa integer
);

CREATE TABLE debezium_signal (
  id varchar(64),
  type varchar(32),
  data varchar(2048)
);

CREATE DATABASE IF NOT EXISTS emptydb;
