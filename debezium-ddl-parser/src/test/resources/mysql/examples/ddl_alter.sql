#begin
-- Alter Table
alter table ship_class add column ship_spec varchar(150) first, add somecol int after start_build;
alter table t3 add column (c2 decimal(10, 2) comment 'comment`' null, c3 enum('abc', 'cba', 'aaa')), add index t3_i1 using btree (c2) comment 'some index';
alter table t3 add column (c2 decimal(10, 2), c3 int);
ALTER TABLE `deals` ADD INDEX `idx_custom_field_30c4f4a7c529ccf0825b2fac732bebfd843ed764` ((cast(json_unquote(json_extract(`custom_fields`,_utf8mb4'$."30c4f4a7c529ccf0825b2fac732bebfd843ed764".value')) as DOUBLE)));
ALTER TABLE `deals` ADD INDEX `idx_custom_field_30c4f4a7c529ccf0825b2fac732bebfd843ed764` ((cast(json_unquote(json_extract(`custom_fields`,_utf8mb4'$."30c4f4a7c529ccf0825b2fac732bebfd843ed764".value')) as FLOAT)));
alter table t3 alter index t3_i1 invisible;
alter table t3 alter index t3_i1 visible;
alter table t3 rename index t3_i1 to t3_i2;
alter table t2 add constraint t2_pk_constraint primary key (1c), alter column `_` set default 1;
alter table t2 drop constraint t2_pk_constraint;
alter table ship_class change column somecol col_for_del tinyint first;
alter table t5 rename column old to new;
alter table ship_class drop col_for_del;
alter table t3 drop index t3_i1;
alter table t3 drop index t3_i2;
alter table childtable drop index fk_idParent_parentTable;
alter table t2 drop primary key;
alter table t3 rename to table3column;
alter table db2.t3 rename to db2.table3column;
alter table childtable add constraint `fk1` foreign key (idParent) references parenttable(id) on delete restrict on update cascade;
alter table table3column default character set = cp1251;
alter table `test` change `id` `id` varchar(10) character set utf8mb4 collate utf8mb4_bin not null;
alter table `test` change `id` `id` varchar(10) character set utf8mb4 binary not null;
alter table `test` change `id` `id` varchar(10) character set utf8mb4 binary null default null;
alter table with_check add constraint check (c1 in (1, 2, 3, 4));
alter table with_check add constraint c2 check (c1 in (1, 2, 3, 4));
alter table with_check add check (c1 in (1, 2, 3, 4));
alter table with_partition add partition (partition p201901 values less than (737425) engine = InnoDB);
alter table with_partition add partition (partition p1 values less than (837425) engine = InnoDB, partition p2 values less than (MAXVALUE) engine = InnoDB);
alter table t1 stats_auto_recalc=default stats_sample_pages=50;
alter table t1 stats_auto_recalc=default, stats_sample_pages=50;
alter table t1 stats_auto_recalc=default, stats_sample_pages=default;
alter table t1 modify column c1 enum('abc','cba','aaa') character set 'utf8' collate 'utf8_unicode_ci' not null default 'abc';
alter table table1 add primary key (id);
alter table table1 add primary key table_pk (id);
alter table table1 add primary key `table_pk` (id);
alter table table1 add primary key `table_pk` (`id`);
alter table table1 drop foreign key fk_name;
alter table table1 drop constraint cons;
alter table table1 add column yes varchar(255)  default '' null;
alter table add_test add column col1 int not null;
alter table `some_table` add (primary key `id` (`id`),`k_id` int unsigned not null,`another_field` smallint not null,index `k_id` (`k_id`));
alter table `some_table` add column (unique key `another_field` (`another_field`));
alter table default.task add column xxxx varchar(200) comment 'cdc test';
ALTER TABLE `hcore`.comments COLLATE='utf8mb4_general_ci', CONVERT TO CHARSET UTF8MB4;
ALTER TABLE T1 ADD FOREIGN KEY ( I )  REFERENCES TT ( I ) ON DELETE SET DEFAULT;
ALTER TABLE T1 ADD FOREIGN KEY ( I ) REFERENCES TT ( I ) ON UPDATE SET DEFAULT;
ALTER TABLE T1 ADD CHECK (id + 6 > 10) ENFORCED;
ALTER TABLE T1 ADD CHECK (ID + 6 > 10) NOT ENFORCED;
ALTER TABLE T1 ALTER CHECK C_CONS ENFORCED;
ALTER TABLE T1 ALTER CHECK C_CONS NOT ENFORCED;
ALTER TABLE T1 ALTER I SET VISIBLE;
ALTER TABLE T1 ALTER I SET INVISIBLE;
ALTER TABLE `order` ADD cancelled TINYINT(1) DEFAULT 0 NOT NULL, ADD delivered TINYINT(1) DEFAULT 0 NOT NULL, ADD returning TINYINT(1) DEFAULT 0 NOT NULL;
#end
#begin
-- Alter database
alter database test default character set = utf8;
alter schema somedb_name upgrade data directory name;
alter database test_1 default encryption = 'Y' read only = 1;
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
-- Alter user
ALTER USER 'mattias.hultman' DEFAULT ROLE `prod-spain-mysql-read-only`@`%`;
rename user user1@100.200.1.1 to user2@100.200.1.2;
rename user user1@100.200.1.1 to user2@2001:0db8:85a3:0000:0000:8a2e:0370:7334;
rename user user1@100.200.1.1 to user2@::1;
alter user 'user'@'%' IDENTIFIED BY 'newpassword' RETAIN CURRENT PASSWORD;
ALTER USER 'test_dual_pass'@'%' IDENTIFIED BY RANDOM PASSWORD RETAIN CURRENT PASSWORD;
ALTER USER 'test_dual_pass'@'%' IDENTIFIED BY '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19' RETAIN CURRENT PASSWORD;
ALTER USER 'test_dual_pass'@'%' IDENTIFIED WITH 'mysql_native_password';
ALTER USER 'test_dual_pass'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19';
ALTER USER 'test_dual_pass'@'%' IDENTIFIED WITH 'mysql_native_password' AS 'REDACTED' RETAIN CURRENT PASSWORD;
ALTER USER 'test_dual_pass'@'%' IDENTIFIED WITH 'mysql_native_password' BY '2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19' REPLACE 'current_auth_string' RETAIN CURRENT PASSWORD;
ALTER USER 'test_dual_pass'@'%' IDENTIFIED WITH 'mysql_native_password' BY RANDOM PASSWORD REPLACE 'current_auth_string' RETAIN CURRENT PASSWORD;
#end