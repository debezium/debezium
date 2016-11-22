-- In production you would almost certainly limit the replication user must be on the follower (slave) machine,
-- to prevent other clients accessing the log from other machines. For example, 'replicator'@'follower.acme.com'.
-- However, in this database we'll grant 3 users different privileges:
--
-- 1) 'replicator' - all privileges required by the binlog reader (setup through 'readbinlog.sql')
-- 2) 'snapper' - all privileges required by the snapshot reader AND binlog reader
-- 3) 'mysqluser' - all privileges
--
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator' IDENTIFIED BY 'replpass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'snapper'@'%' IDENTIFIED BY 'snapperpass';
GRANT ALL PRIVILEGES ON *.* TO 'mysqlreplica'@'%';

-- Start the GTID-based replication ...
CHANGE MASTER TO MASTER_HOST='database', MASTER_PORT=3306, MASTER_USER='replicator', MASTER_PASSWORD = 'replpass', MASTER_AUTO_POSITION=1;

-- And start the slave ...
START SLAVE;