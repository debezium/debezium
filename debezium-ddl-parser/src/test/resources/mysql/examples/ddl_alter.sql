#begin
-- Alter Table
alter table ship_class add column ship_spec varchar(150) first, add somecol int after start_build, algorithm=instant;
alter table t3 add column (c2 decimal(10, 2) comment 'comment`' null, c3 enum('abc', 'cba', 'aaa')), add index t3_i1 using btree (c2) comment 'some index';
alter table t3 add column (c4 decimal(10, 2) comment 'comment`' null), add index t3_i2 using btree (c4) comment 'some index';
alter table t3 add column if not exists (c2 decimal(10, 2), c3 int);
alter table t2 add constraint t2_pk_constraint primary key (1c), alter column `_` set default 1;
alter table t2 drop constraint t2_pk_constraint;
alter table ship_class change column somecol col_for_del tinyint first;
alter table ship_class drop col_for_del;
alter table t3 drop index t3_i1;
alter table t3 drop index if exists t3_i2;
alter table childtable drop index fk_idParent_parentTable;
alter table t2 drop primary key;
alter table t3 rename to table3column;
alter table childtable add constraint `fk1` foreign key (idParent) references parenttable(id) on delete restrict on update cascade;
alter table table3column default character set = cp1251;
alter table `test` change `id` `id` varchar(10) character set utf8mb4 collate utf8mb4_bin not null;
alter table `test` change `id` `id` varchar(10) character set utf8mb4 binary not null;
alter table `test` change `id` `id` varchar(10) character set utf8mb4 binary null default null;
alter table t1 stats_auto_recalc=default stats_sample_pages=50;
alter table t1 stats_auto_recalc=default, stats_sample_pages=50.0;
alter table t1 stats_auto_recalc=default, stats_sample_pages=default;
alter table table1 add primary key (id);
alter table table1 add primary key table_pk (id);
alter table table1 add primary key `table_pk` (id);
alter table table1 add primary key `table_pk` (`id`);
alter table table1 add column yes varchar(255)  default '' null;
alter table add_test add column if not exists col1 varchar(255);
alter table add_test add column if not exists col4 varchar(255);
alter table add_test add index if not exists ix_add_test_col1 using btree (col1) comment 'test index';
alter table add_test add index if not exists ix_add_test_col4 using btree (col4) comment 'test index';
ALTER TABLE `deals` ADD INDEX `idx_custom_field_30c4f4a7c529ccf0825b2fac732bebfd843ed764` ((cast(json_unquote(json_extract(`custom_fields`,_utf8mb4'$."30c4f4a7c529ccf0825b2fac732bebfd843ed764".value')) as DOUBLE)));
ALTER TABLE `deals` ADD INDEX `idx_custom_field_30c4f4a7c529ccf0825b2fac732bebfd843ed764` ((cast(json_unquote(json_extract(`custom_fields`,_utf8mb4'$."30c4f4a7c529ccf0825b2fac732bebfd843ed764".value')) as FLOAT)));
alter table add_test alter index ix_add_test_col1 invisible;
alter table add_test alter index ix_add_test_col1 visible;
alter table add_test change column if exists col8 col9 tinyint;
alter table add_test change column if exists col3 col5 tinyint;
alter table add_test modify column if exists col9 tinyint;
alter table add_test modify column if exists col5 varchar(255);
alter table add_test drop column if exists col99;
alter table add_test drop column if exists col5;
alter table add_test add column optional bool default 0 null;
alter table add_test add column empty varchar(255);
alter table add_test add column geometry int;
alter table add_test drop foreign key fk;
alter table add_test drop foreign key if exists fk;
alter table add_test drop constraint if exists cons;
alter table add_test wait 100 add column col1 int not null;
alter table default.task add column xxxx varchar(200) comment 'cdc test';
alter table `some_table` add unique if not exists `id_unique` (`id`);
ALTER TABLE `hcore`.comments COLLATE='utf8mb4_general_ci', CONVERT TO CHARSET UTF8MB4;
ALTER TABLE T1 ADD FOREIGN KEY ( I )  REFERENCES TT ( I ) ON DELETE SET DEFAULT;
ALTER TABLE T1 ADD FOREIGN KEY ( I ) REFERENCES TT ( I ) ON UPDATE SET DEFAULT;
ALTER TABLE T1 ADD CHECK (id + 6 > 10) ENFORCED;
ALTER TABLE T1 ADD CHECK (ID + 6 > 10) NOT ENFORCED;
ALTER TABLE T1 ALTER CHECK C_CONS ENFORCED;
ALTER TABLE T1 ALTER CHECK C_CONS NOT ENFORCED;
ALTER TABLE T1 ALTER I SET VISIBLE;
ALTER TABLE T1 ALTER I SET INVISIBLE;
ALTER TABLE IF EXISTS `add_test` ADD COLUMN IF NOT EXISTS `new_col` TEXT DEFAULT 'my_default';
alter table user_details add index if not exists `country_id_index` (country_id), algorithm=NOCOPY;
-- # MariaDB Specific
ALTER TABLE t1 PARTITION BY SYSTEM_TIME INTERVAL 1 HOUR;
ALTER TABLE t1 PARTITION BY SYSTEM_TIME INTERVAL 1 HOUR AUTO;
#end
#begin
-- Alter database
alter database test default character set = utf8;
alter database test_1 default encryption = 'Y' read only = 1;
alter schema somedb_name upgrade data directory name;
alter database test_2 /*!40100 DEFAULT CHARACTER SET utf8mb4 */
#end
#begin
-- Alter event
alter definer = current_user event someevent on schedule at current_timestamp + interval 30 minute;
alter definer = 'ivan'@'%' event someevent on completion preserve;
alter definer = 'ivan'@'%' event someevent rename to newsomeevent;
alter event newsomeevent enable comment 'some comment';
-- delimiter //
alter definer = current_user event newsomeevent on schedule at current_timestamp + interval 2 hour
rename to someevent disable
do begin update test.t2 set 1c = 1c + 1; end; -- //
-- delimiter ;
#end
#begin
-- Alter function/procedure
alter function f_name comment 'some funct' language sql sql security invoker;
alter function one_more_func contains sql sql security definer;
alter procedure p_name comment 'some funct' language sql sql security invoker;
alter procedure one_more_proc contains sql sql security definer;
#end
#begin
-- Alter logfile group
-- http://dev.mysql.com/doc/refman/5.6/en/alter-logfile-group.html
ALTER LOGFILE GROUP lg_3 ADD UNDOFILE 'undo_10.dat' INITIAL_SIZE=32M ENGINE=NDBCLUSTER;
ALTER LOGFILE GROUP lg_1 ADD UNDOFILE 'undo_10.dat' wait ENGINE=NDB;
#end
#begin
-- Alter server
-- http://dev.mysql.com/doc/refman/5.6/en/alter-server.html
ALTER SERVER s OPTIONS (USER 'sally');
#end
#begin
-- Alter tablespace
alter tablespace tblsp_1 add datafile 'filename' engine = ndb;
alter tablespace tblsp_2 drop datafile 'deletedfilename' wait engine ndb;
#end
#begin
-- Alter view
alter view my_view1 as select 1 union select 2 limit 0,5;
alter algorithm = merge view my_view2(col1, col2) as select * from t2 with check option;
alter definer = 'ivan'@'%' view my_view3 as select count(*) from t3;
alter definer = current_user sql security invoker view my_view4(c1, 1c, _, c1_2)
	as select * from  (t1 as tt1, t2 as tt2) inner join t1 on t1.col1 = tt1.col1;
#end
#begin
-- Alter user
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password history default;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password history 90;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password reuse interval default;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password reuse interval 360 DAY;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password require current;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password require current optional;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password require current default;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock failed_login_attempts 5;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password_lock_time 2;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password_lock_time unbounded;
alter user 'user'@'%' identified by 'newpassword' retain current password;
alter user if exists 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password history default;
ALTER USER 'mattias.hultman' DEFAULT ROLE `prod-spain-mysql-read-only`@`%`;
rename user user1@100.200.1.1 to user2@100.200.1.2;
rename user user1@100.200.1.1 to user2@2001:0db8:85a3:0000:0000:8a2e:0370:7334;
rename user user1@100.200.1.1 to user2@::1;
ALTER USER 'test_dual_pass'@'%' IDENTIFIED BY RANDOM PASSWORD RETAIN CURRENT PASSWORD;
ALTER USER 'test_dual_pass'@'%' IDENTIFIED BY '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19' RETAIN CURRENT PASSWORD;
ALTER USER 'test_dual_pass'@'%' IDENTIFIED WITH 'mysql_native_password';
ALTER USER 'test_dual_pass'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19';
ALTER USER 'test_dual_pass'@'%' IDENTIFIED WITH 'mysql_native_password' AS 'REDACTED' RETAIN CURRENT PASSWORD;
ALTER USER 'test_dual_pass'@'%' IDENTIFIED WITH 'mysql_native_password' BY '2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19' REPLACE 'current_auth_string' RETAIN CURRENT PASSWORD;
ALTER USER 'test_dual_pass'@'%' IDENTIFIED WITH 'mysql_native_password' BY RANDOM PASSWORD REPLACE 'current_auth_string' RETAIN CURRENT PASSWORD;
#end
ALTER TABLE t1 ADD PARTITION (PARTITION p3 VALUES LESS THAN (2002));
ALTER TABLE t1 ADD PARTITION IF NOT EXISTS (PARTITION p3 VALUES LESS THAN (2002));
-- Alter sequence
ALTER SEQUENCE IF EXISTS s2 start=100;
ALTER SEQUENCE s1 CACHE=1000 NOCYCLE RESTART WITH 1;
ALTER TABLE `TABLE_NAME` DROP FOREIGN KEY `TABLE_NAME`.`FK_COLUMN`;
ALTER TABLE `order` ADD cancelled TINYINT(1) DEFAULT 0 NOT NULL, ADD delivered TINYINT(1) DEFAULT 0 NOT NULL, ADD returning TINYINT(1) DEFAULT 0 NOT NULL
