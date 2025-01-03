GRANT ALL ON *.* TO `foo2` @`%`;
GRANT ALL ON *.* TO `foo2` @test;
GRANT ALL ON tbl TO admin@localhost;
GRANT ALL ON tbl TO admin;
GRANT ALL PRIVILEGES ON tbl TO admin;
GRANT ALL ON *.* TO admin;
GRANT ALL ON *.* TO `admin`;
GRANT ALL ON *.* TO 'admin';
GRANT ALL ON *.* TO "admin";
GRANT ALL ON db.* TO "admin";
GRANT ALL on db.tbl to 'admin';
GRANT ALL on `db`.`tbl` to 'admin';
GRANT ALL on `db`.tbl to 'admin';
GRANT ALL on db.`tbl` to 'admin';
GRANT SESSION_VARIABLES_ADMIN on *.* to 'u2';
GRANT 'SESSION_VARIABLES_ADMIN' on *.* to 'u2';
GRANT `SESSION_VARIABLES_ADMIN` on *.* to 'u2';
GRANT "SESSION_VARIABLES_ADMIN" on *.* to 'u2';
GRANT BACKUP_ADMIN ON *.* TO `admin`@`%`;
GRANT CREATE ROLE, DROP ROLE ON *.* TO `admin`@`localhost`;
GRANT AUDIT_ADMIN, BACKUP_ADMIN, BINLOG_ADMIN, BINLOG_ENCRYPTION_ADMIN, CLONE_ADMIN, CONNECTION_ADMIN,
ENCRYPTION_KEY_ADMIN, FIREWALL_ADMIN, FIREWALL_USER, GROUP_REPLICATION_ADMIN, INNODB_REDO_LOG_ARCHIVE,
NDB_STORED_USER, PERSIST_RO_VARIABLES_ADMIN, REPLICATION_APPLIER, REPLICATION_SLAVE_ADMIN, RESOURCE_GROUP_ADMIN,
RESOURCE_GROUP_USER, ROLE_ADMIN, SESSION_VARIABLES_ADMIN, SET_USER_ID, SHOW_ROUTINE, SYSTEM_VARIABLES_ADMIN, AUTHENTICATION_POLICY_ADMIN,
TABLE_ENCRYPTION_ADMIN, VERSION_TOKEN_ADMIN, XA_RECOVER_ADMIN, AUDIT_ABORT_EXEMPT, FIREWALL_EXEMPT, SKIP_QUERY_REWRITE, TP_CONNECTION_ADMIN ON *.* TO `admin`@`localhost`;
GRANT SELECT, INSERT, UPDATE ON *.* TO u4 AS u1 WITH ROLE r1;
GRANT SELECT, RELOAD, REPLICATION SLAVE, REPLICATION CLIENT, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'xuser1'@'%', 'xuser2'@'%'
AS 'root'@'%' WITH ROLE 'cloudsqlsuperuser'@'%';
GRANT ALTER ON *.* TO 'admin'@'localhost'
GRANT ALTER ROUTINE ON *.* TO 'admin'@'localhost'
GRANT CREATE ON *.* TO 'admin'@'localhost'
GRANT CREATE TEMPORARY TABLES ON *.* TO 'admin'@'localhost'
GRANT CREATE ROUTINE ON *.* TO 'admin'@'localhost'
GRANT CREATE VIEW ON *.* TO 'admin'@'localhost'
GRANT CREATE USER ON *.* TO 'admin'@'localhost'
GRANT CREATE TABLESPACE ON *.* TO 'admin'@'localhost'
GRANT CREATE ROLE ON *.* TO 'admin'@'localhost'
GRANT DELETE ON *.* TO 'admin'@'localhost'
GRANT DROP ON *.* TO 'admin'@'localhost'
GRANT DROP ROLE ON *.* TO 'admin'@'localhost'
GRANT EVENT ON *.* TO 'admin'@'localhost'
GRANT EXECUTE ON *.* TO 'admin'@'localhost'
GRANT FILE ON *.* TO 'admin'@'localhost'
GRANT GRANT OPTION ON *.* TO 'admin'@'localhost'
GRANT INDEX ON *.* TO 'admin'@'localhost'
GRANT INSERT ON *.* TO 'admin'@'localhost'
GRANT LOCK TABLES ON *.* TO 'admin'@'localhost'
GRANT PROCESS ON *.* TO 'admin'@'localhost'
GRANT PROXY ON *.* TO 'admin'@'localhost'
GRANT REFERENCES ON *.* TO 'admin'@'localhost'
GRANT RELOAD ON *.* TO 'admin'@'localhost'
GRANT REPLICATION CLIENT ON *.* TO 'admin'@'localhost'
GRANT REPLICATION SLAVE ON *.* TO 'admin'@'localhost'
GRANT SELECT ON *.* TO 'admin'@'localhost'
GRANT SHOW VIEW ON *.* TO 'admin'@'localhost'
GRANT SHOW DATABASES ON *.* TO 'admin'@'localhost'
GRANT SHUTDOWN ON *.* TO 'admin'@'localhost'
GRANT SUPER ON *.* TO 'admin'@'localhost'
GRANT TRIGGER ON *.* TO 'admin'@'localhost'
GRANT UPDATE ON *.* TO 'admin'@'localhost'
GRANT USAGE ON *.* TO 'admin'@'localhost'
GRANT APPLICATION_PASSWORD_ADMIN ON *.* TO 'admin'@'localhost'
GRANT AUDIT_ADMIN ON *.* TO 'admin'@'localhost'
GRANT BACKUP_ADMIN ON *.* TO 'admin'@'localhost'
GRANT BINLOG_ADMIN ON *.* TO 'admin'@'localhost'
GRANT BINLOG_ENCRYPTION_ADMIN ON *.* TO 'admin'@'localhost'
GRANT CLONE_ADMIN ON *.* TO 'admin'@'localhost'
GRANT CONNECTION_ADMIN ON *.* TO 'admin'@'localhost'
GRANT ENCRYPTION_KEY_ADMIN ON *.* TO 'admin'@'localhost'
GRANT FIREWALL_ADMIN ON *.* TO 'admin'@'localhost'
GRANT FIREWALL_USER ON *.* TO 'admin'@'localhost'
GRANT FLUSH_OPTIMIZER_COSTS ON *.* TO 'admin'@'localhost'
GRANT FLUSH_STATUS ON *.* TO 'admin'@'localhost'
GRANT FLUSH_TABLES ON *.* TO 'admin'@'localhost'
GRANT FLUSH_USER_RESOURCES ON *.* TO 'admin'@'localhost'
GRANT GROUP_REPLICATION_ADMIN ON *.* TO 'admin'@'localhost'
GRANT INNODB_REDO_LOG_ARCHIVE ON *.* TO 'admin'@'localhost'
GRANT INNODB_REDO_LOG_ENABLE ON *.* TO 'admin'@'localhost'
GRANT NDB_STORED_USER ON *.* TO 'admin'@'localhost'
GRANT PERSIST_RO_VARIABLES_ADMIN ON *.* TO 'admin'@'localhost'
GRANT REPLICATION_APPLIER ON *.* TO 'admin'@'localhost'
GRANT REPLICATION_SLAVE_ADMIN ON *.* TO 'admin'@'localhost'
GRANT RESOURCE_GROUP_ADMIN ON *.* TO 'admin'@'localhost'
GRANT RESOURCE_GROUP_USER ON *.* TO 'admin'@'localhost'
GRANT ROLE_ADMIN ON *.* TO 'admin'@'localhost'
GRANT SERVICE_CONNECTION_ADMIN ON *.* TO 'admin'@'localhost'
GRANT SESSION_VARIABLES_ADMIN ON *.* TO 'admin'@'localhost'
GRANT SET_USER_ID ON *.* TO 'admin'@'localhost'
GRANT SHOW_ROUTINE ON *.* TO 'admin'@'localhost'
GRANT SYSTEM_USER ON *.* TO 'admin'@'localhost'
GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'admin'@'localhost'
GRANT TABLE_ENCRYPTION_ADMIN ON *.* TO 'admin'@'localhost'
GRANT VERSION_TOKEN_ADMIN ON *.* TO 'admin'@'localhost'
GRANT XA_RECOVER_ADMIN ON *.* TO 'admin'@'localhost'
GRANT reader TO 'mysqluser'@'localhost'
GRANT reader TO topreader
GRANT reader TO topreader WITH ADMIN OPTION;
GRANT 'db_old_ro'@'%' TO 'oghalawinji'@'%'
GRANT FLUSH_OPTIMIZER_COSTS, FLUSH_STATUS, FLUSH_TABLES, FLUSH_USER_RESOURCES, PASSWORDLESS_USER_ADMIN ON *.* TO "@"
REVOKE reader FROM 'mysqluser'@'localhost'
REVOKE reader FROM topreader
REVOKE `cloudsqlsuperuser`@`%` FROM `sarmonitoring`@`10.90.29.%`
REVOKE IF EXISTS SELECT ON test.t1 FROM jerry@localhost;
REVOKE IF EXISTS Bogus ON test FROM jerry@localhost IGNORE UNKNOWN USER;

-- Set Role
SET ROLE DEFAULT;
SET ROLE 'role1', 'role2';
SET DEFAULT ROLE 'admin', 'developer' TO 'joe'@'10.0.0.1';
SET DEFAULT ROLE `admin`@'%' to `dt_user`@`%`;
-- MySQL on Amazon RDS
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA ON *.* TO 'debezium_user'@'127.0.0.1';
