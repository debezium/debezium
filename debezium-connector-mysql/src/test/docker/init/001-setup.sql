-- In production you would almost certainly limit the replication user must be on the follower (replica) machine,
-- to prevent other clients accessing the log from other machines. For example, 'replicator'@'follower.acme.com'.
-- However, in this database we'll grant 3 users different privileges:
--
-- 1) 'replicator' - all privileges required by the binlog reader (setup through 'readbinlog.sql')
-- 2) 'snapper' - all privileges required by the snapshot reader AND binlog reader
-- 3) 'mysqluser' - all privileges
--
CREATE USER 'replicator' IDENTIFIED BY 'replpass';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator';
CREATE USER 'snapper'@'%' IDENTIFIED WITH mysql_native_password BY 'snapperpass';
CREATE USER 'snapper'@'localhost' IDENTIFIED WITH mysql_native_password BY 'snapperpass';
GRANT SELECT, INSERT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'snapper'@'%';
GRANT SELECT, INSERT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'snapper'@'localhost';
CREATE USER 'cloud' IDENTIFIED BY 'cloudpass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES  ON *.* TO 'cloud'@'%';
-- Ensure snapper can connect from any interface
CREATE USER IF NOT EXISTS 'snapper'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY 'snapperpass';
GRANT ALL PRIVILEGES ON *.* TO 'snapper'@'127.0.0.1';
CREATE USER 'mysql'@'%' IDENTIFIED BY 'mysqlpw';
CREATE USER 'mysql'@'localhost' IDENTIFIED BY 'mysqlpw';
GRANT ALL PRIVILEGES ON *.* TO 'mysql'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'mysql'@'localhost';

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  emptydb
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE emptydb;
RESET MASTER; -- MySQL 8.x
RESET BINARY LOGS AND GTIDS; -- MySQL 9.x
CREATE DATABASE testing;
CREATE DATABASE signal_test_db;
CREATE TABLE testing.testing (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY);
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();
INSERT INTO testing.testing VALUES ();


-- Ensure all privileges are flushed
FLUSH PRIVILEGES;
