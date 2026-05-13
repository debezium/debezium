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
GRANT SENSITIVE_VARIABLES_OBSERVER ON *.* TO `admin`@`%`;
#NB: GRANT SELECT, INSERT, UPDATE ON *.* TO u4 AS u1 WITH ROLE r1;
#NB: GRANT SELECT, RELOAD, REPLICATION SLAVE, REPLICATION CLIENT, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'xuser1'@'%', 'xuser2'@'%'
# AS 'root'@'%' WITH ROLE 'cloudsqlsuperuser'@'%';
GRANT ALTER ON *.* TO 'admin'@'localhost' ; #NB
GRANT ALTER ROUTINE ON *.* TO 'admin'@'localhost' ; #NB
GRANT CREATE ON *.* TO 'admin'@'localhost' ; #NB
GRANT CREATE TEMPORARY TABLES ON *.* TO 'admin'@'localhost' ; #NB
GRANT CREATE ROUTINE ON *.* TO 'admin'@'localhost' ; #NB
GRANT CREATE VIEW ON *.* TO 'admin'@'localhost' ; #NB
GRANT CREATE USER ON *.* TO 'admin'@'localhost' ; #NB
GRANT CREATE TABLESPACE ON *.* TO 'admin'@'localhost' ; #NB
GRANT CREATE ROLE ON *.* TO 'admin'@'localhost' ; #NB
GRANT DELETE ON *.* TO 'admin'@'localhost' ; #NB
GRANT DROP ON *.* TO 'admin'@'localhost' ; #NB
GRANT DROP ROLE ON *.* TO 'admin'@'localhost' ; #NB
GRANT EVENT ON *.* TO 'admin'@'localhost' ; #NB
GRANT EXECUTE ON *.* TO 'admin'@'localhost' ; #NB
GRANT FILE ON *.* TO 'admin'@'localhost' ; #NB
GRANT GRANT OPTION ON *.* TO 'admin'@'localhost' ; #NB
GRANT INDEX ON *.* TO 'admin'@'localhost' ; #NB
GRANT INSERT ON *.* TO 'admin'@'localhost' ; #NB
GRANT LOCK TABLES ON *.* TO 'admin'@'localhost' ; #NB
GRANT PROCESS ON *.* TO 'admin'@'localhost' ; #NB
GRANT PROXY ON *.* TO 'admin'@'localhost' ; #NB
GRANT REFERENCES ON *.* TO 'admin'@'localhost' ; #NB
GRANT RELOAD ON *.* TO 'admin'@'localhost' ; #NB
GRANT REPLICATION CLIENT ON *.* TO 'admin'@'localhost' ; #NB
GRANT REPLICATION SLAVE ON *.* TO 'admin'@'localhost' ; #NB
GRANT SELECT ON *.* TO 'admin'@'localhost' ; #NB
GRANT SHOW VIEW ON *.* TO 'admin'@'localhost' ; #NB
GRANT SHOW DATABASES ON *.* TO 'admin'@'localhost' ; #NB
GRANT SHUTDOWN ON *.* TO 'admin'@'localhost' ; #NB
GRANT SUPER ON *.* TO 'admin'@'localhost' ; #NB
GRANT TRIGGER ON *.* TO 'admin'@'localhost' ; #NB
GRANT UPDATE ON *.* TO 'admin'@'localhost' ; #NB
GRANT USAGE ON *.* TO 'admin'@'localhost' ; #NB
GRANT APPLICATION_PASSWORD_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT AUDIT_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT BACKUP_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT BINLOG_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT BINLOG_ENCRYPTION_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT CLONE_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT CONNECTION_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT ENCRYPTION_KEY_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT FIREWALL_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT FIREWALL_USER ON *.* TO 'admin'@'localhost' ; #NB
GRANT FLUSH_OPTIMIZER_COSTS ON *.* TO 'admin'@'localhost' ; #NB
GRANT FLUSH_STATUS ON *.* TO 'admin'@'localhost' ; #NB
GRANT FLUSH_TABLES ON *.* TO 'admin'@'localhost' ; #NB
GRANT FLUSH_USER_RESOURCES ON *.* TO 'admin'@'localhost' ; #NB
GRANT GROUP_REPLICATION_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT INNODB_REDO_LOG_ARCHIVE ON *.* TO 'admin'@'localhost' ; #NB
GRANT INNODB_REDO_LOG_ENABLE ON *.* TO 'admin'@'localhost' ; #NB
GRANT NDB_STORED_USER ON *.* TO 'admin'@'localhost' ; #NB
GRANT PERSIST_RO_VARIABLES_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT REPLICATION_APPLIER ON *.* TO 'admin'@'localhost' ; #NB
GRANT REPLICATION_SLAVE_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT RESOURCE_GROUP_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT RESOURCE_GROUP_USER ON *.* TO 'admin'@'localhost' ; #NB
GRANT ROLE_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT SERVICE_CONNECTION_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT SESSION_VARIABLES_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT SET_USER_ID ON *.* TO 'admin'@'localhost' ; #NB
GRANT SHOW_ROUTINE ON *.* TO 'admin'@'localhost' ; #NB
GRANT SYSTEM_USER ON *.* TO 'admin'@'localhost' ; #NB
GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT TABLE_ENCRYPTION_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT VERSION_TOKEN_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT XA_RECOVER_ADMIN ON *.* TO 'admin'@'localhost' ; #NB
GRANT reader TO 'mysqluser'@'localhost' ; #NB
GRANT reader TO topreader ; #NB
GRANT reader TO topreader WITH ADMIN OPTION;
GRANT 'db_old_ro'@'%' TO 'oghalawinji'@'%' ; #NB
GRANT FLUSH_OPTIMIZER_COSTS, FLUSH_STATUS, FLUSH_TABLES, FLUSH_USER_RESOURCES, PASSWORDLESS_USER_ADMIN ON *.* TO "@" ; #NB
REVOKE reader FROM 'mysqluser'@'localhost' ; #NB
REVOKE reader FROM topreader ; #NB
REVOKE `cloudsqlsuperuser`@`%` FROM `sarmonitoring`@`10.90.29.%` ; #NB
REVOKE IF EXISTS SELECT ON test.t1 FROM jerry@localhost;
REVOKE IF EXISTS Bogus ON test FROM jerry@localhost IGNORE UNKNOWN USER;

-- Set Role
SET ROLE DEFAULT;
SET ROLE 'role1', 'role2';
SET ROLE ALL;
SET ROLE ALL EXCEPT 'role1', 'role2';
-- Set Default Role
SET DEFAULT ROLE 'admin', 'developer' TO 'joe'@'10.0.0.1';
SET DEFAULT ROLE `admin`@'%' to `dt_user`@`%`;
-- MySQL on Amazon RDS
#NB: GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA ON *.* TO 'debezium_user'@'127.0.0.1';
