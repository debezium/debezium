create table DEBEZIUM
(id number(19) not null,
col1 number(4,2),
col2 varchar2(255) default 'debezium' not null ,
col3 nvarchar2(255) not null,
col4 char(4),
col5 nchar(4),
col6 float(126),
col7 date,
col8 timestamp,
col9 blob,
col10 clob,
primary key (id));
