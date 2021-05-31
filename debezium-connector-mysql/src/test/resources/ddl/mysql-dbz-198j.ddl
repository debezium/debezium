-- Alter the table, but the parser doesn't really keep track of indexes or foreign keys and should ignore those expressions
Alter table `NextTimeTable`.`TIMETABLE_SUBJECT_GROUP_MAPPING`
drop column `SUBJECT_ID`,
drop index `FK69atxmt7wrwpb4oekyravsx9l`,
drop foreign key `FK69atxmt7wrwpb4oekyravsx9l`

create table `db1`.`table1` ( pk1 int not null, `id` int not null, `other` int );
