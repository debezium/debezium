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
CREATE USER 'snapper' IDENTIFIED BY 'snapperpass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'snapper'@'%';
CREATE USER 'cloud' IDENTIFIED BY 'cloudpass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES  ON *.* TO 'cloud'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'mysqlreplica'@'%';

-- Start the GTID-based replication ...
CHANGE REPLICATION SOURCE TO SOURCE_HOST='database-gtids', SOURCE_PORT=3306, SOURCE_USER='replicator', SOURCE_PASSWORD = 'replpass', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1;

-- And start the replica ...
START REPLICA;
