GRANT ALL ON tbl TO admin@localhost;
GRANT ALL ON tbl TO admin;
GRANT ALL PRIVILEGES ON tbl TO admin;
GRANT ALL ON *.* TO admin;
GRANT USAGE ON *.* TO foo2@test IDENTIFIED BY 'mariadb';
GRANT USAGE ON *.* TO foo2@test IDENTIFIED BY PASSWORD '*54958E764CE10E50764C2EECBB71D01F08549980';
GRANT USAGE ON *.* TO `admin`@`%` IDENTIFIED VIA pam;
GRANT USAGE ON *.* TO foo2@test IDENTIFIED VIA pam USING 'mariadb';
CREATE USER safe@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
CREATE USER safe@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret') OR unix_socket;
GRANT SESSION_VARIABLES_ADMIN on *.* to 'u2';
GRANT 'SESSION_VARIABLES_ADMIN' on *.* to 'u2';
GRANT `SESSION_VARIABLES_ADMIN` on *.* to 'u2';
GRANT "SESSION_VARIABLES_ADMIN" on *.* to 'u2';
GRANT BACKUP_ADMIN ON *.* TO `admin`@`%`;
GRANT CREATE ROLE, DROP ROLE ON *.* TO `admin`@`localhost`;
GRANT AUDIT_ADMIN, BACKUP_ADMIN, BINLOG_ADMIN, BINLOG_ENCRYPTION_ADMIN, BINLOG_MONITOR, BINLOG_REPLAY, CLONE_ADMIN, CONNECTION_ADMIN,
ENCRYPTION_KEY_ADMIN, FEDERATED_ADMIN, FIREWALL_ADMIN, FIREWALL_USER, GROUP_REPLICATION_ADMIN, INNODB_REDO_LOG_ARCHIVE,
NDB_STORED_USER, PERSIST_RO_VARIABLES_ADMIN, READ_ONLY_ADMIN, REPLICATION_APPLIER, REPLICATION_MASTER_ADMIN, REPLICATION_SLAVE_ADMIN, RESOURCE_GROUP_ADMIN,
RESOURCE_GROUP_USER, ROLE_ADMIN, SESSION_VARIABLES_ADMIN, SET_USER_ID, SHOW_ROUTINE, SYSTEM_VARIABLES_ADMIN,
TABLE_ENCRYPTION_ADMIN, VERSION_TOKEN_ADMIN, XA_RECOVER_ADMIN ON *.* TO `admin`@`localhost`;
GRANT ALTER ON *.* TO 'mysqluser'@'localhost'
GRANT ALTER ROUTINE ON *.* TO 'mysqluser'@'localhost'
GRANT CREATE ON *.* TO 'mysqluser'@'localhost'
GRANT CREATE TEMPORARY TABLES ON *.* TO 'mysqluser'@'localhost'
GRANT CREATE ROUTINE ON *.* TO 'mysqluser'@'localhost'
GRANT CREATE VIEW ON *.* TO 'mysqluser'@'localhost'
GRANT CREATE USER ON *.* TO 'mysqluser'@'localhost'
GRANT CREATE TABLESPACE ON *.* TO 'mysqluser'@'localhost'
GRANT CREATE ROLE ON *.* TO 'mysqluser'@'localhost'
GRANT DELETE ON *.* TO 'mysqluser'@'localhost'
GRANT DROP ON *.* TO 'mysqluser'@'localhost'
GRANT DROP ROLE ON *.* TO 'mysqluser'@'localhost'
GRANT EVENT ON *.* TO 'mysqluser'@'localhost'
GRANT EXECUTE ON *.* TO 'mysqluser'@'localhost'
GRANT FILE ON *.* TO 'mysqluser'@'localhost'
GRANT GRANT OPTION ON *.* TO 'mysqluser'@'localhost'
GRANT INDEX ON *.* TO 'mysqluser'@'localhost'
GRANT INSERT ON *.* TO 'mysqluser'@'localhost'
GRANT LOCK TABLES ON *.* TO 'mysqluser'@'localhost'
GRANT PROCESS ON *.* TO 'mysqluser'@'localhost'
GRANT PROXY ON *.* TO 'mysqluser'@'localhost'
GRANT REFERENCES ON *.* TO 'mysqluser'@'localhost'
GRANT RELOAD ON *.* TO 'mysqluser'@'localhost'
GRANT REPLICATION CLIENT ON *.* TO 'mysqluser'@'localhost'
GRANT REPLICATION SLAVE ON *.* TO 'mysqluser'@'localhost'
GRANT SELECT ON *.* TO 'mysqluser'@'localhost'
GRANT SHOW VIEW ON *.* TO 'mysqluser'@'localhost'
GRANT SHOW DATABASES ON *.* TO 'mysqluser'@'localhost'
GRANT SHUTDOWN ON *.* TO 'mysqluser'@'localhost'
GRANT SUPER ON *.* TO 'mysqluser'@'localhost'
GRANT TRIGGER ON *.* TO 'mysqluser'@'localhost'
GRANT UPDATE ON *.* TO 'mysqluser'@'localhost'
GRANT USAGE ON *.* TO 'mysqluser'@'localhost'
GRANT APPLICATION_PASSWORD_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT AUDIT_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT BACKUP_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT BINLOG_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT BINLOG_ENCRYPTION_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT CLONE_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT CONNECTION_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT ENCRYPTION_KEY_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT FIREWALL_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT FIREWALL_USER ON *.* TO 'mysqluser'@'localhost'
GRANT FLUSH_OPTIMIZER_COSTS ON *.* TO 'mysqluser'@'localhost'
GRANT FLUSH_STATUS ON *.* TO 'mysqluser'@'localhost'
GRANT FLUSH_TABLES ON *.* TO 'mysqluser'@'localhost'
GRANT FLUSH_USER_RESOURCES ON *.* TO 'mysqluser'@'localhost'
GRANT GROUP_REPLICATION_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT INNODB_REDO_LOG_ARCHIVE ON *.* TO 'mysqluser'@'localhost'
GRANT INNODB_REDO_LOG_ENABLE ON *.* TO 'mysqluser'@'localhost'
GRANT NDB_STORED_USER ON *.* TO 'mysqluser'@'localhost'
GRANT PERSIST_RO_VARIABLES_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT REPLICATION_APPLIER ON *.* TO 'mysqluser'@'localhost'
GRANT REPLICATION_SLAVE_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT RESOURCE_GROUP_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT RESOURCE_GROUP_USER ON *.* TO 'mysqluser'@'localhost'
GRANT ROLE_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT SERVICE_CONNECTION_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT SESSION_VARIABLES_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT SET_USER_ID ON *.* TO 'mysqluser'@'localhost'
GRANT SHOW_ROUTINE ON *.* TO 'mysqluser'@'localhost'
GRANT SYSTEM_USER ON *.* TO 'mysqluser'@'localhost'
GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT TABLE_ENCRYPTION_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT VERSION_TOKEN_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT XA_RECOVER_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT reader TO 'mysqluser'@'localhost'
GRANT reader TO topreader
REVOKE reader FROM 'mysqluser'@'localhost'
REVOKE reader FROM topreader

-- MariaDB
GRANT BINLOG_MONITOR ON *.* TO 'mysqluser'@'localhost'
GRANT BINLOG_REPLAY ON *.* TO 'mysqluser'@'localhost'
GRANT FEDERATED_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT READ_ONLY_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT REPLICATION_MASTER_ADMIN ON *.* TO 'mysqluser'@'localhost'
GRANT REPLICATION REPLICA ON *.* TO 'mysqluser'@'localhost'
