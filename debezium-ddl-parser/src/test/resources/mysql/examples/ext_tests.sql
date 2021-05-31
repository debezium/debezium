#begin
-- Intersections
-- -- Binary: charset and datatype
select _binary 'hello' as c1;
create table t1(col1 binary(20));
create table t2(col varchar(10) binary character set cp1251);
create table t2(col varchar(10) binary character set binary);
#end
#begin
-- -- Keywords, which can be ID. Intersect that keywords and ID
#end
#begin
-- Expression test
select +-!1 as c;
select 0 in (20 = any (select col1 from t1)) is not null is not unknown as t;
select 0 in (20 = any (select col1 from t1)) is not unknown as t;
select 20 = any (select col1 from t1) is not unknown as t;
select 20 = any (select col1 from t1) as t;
-- select sqrt(20.5) not in (sqrt(20.5) not in (select col1 from t1), 1 in (1, 2, 3, 4)) as c;
select 20 in (10 in (5 in (1, 2, 3, 4, 5), 1, 1, 8), 8, 8, 8);
select (1 in (2, 3, 4)) in (0, 1, 2) as c;
select 1 and (5 between 1 and 10) as c;

select 1 = 16/4 between 3 and 5 as c;
select 1 = 16/4 between 5 and 6 as c;
select 17 member of('[23, "abc", 17, "ab", 10]');
#end
#begin
-- Functions test
select *, sqrt(a), lower(substring(str, 'a', length(str)/2)) as col3 from tab1 where a is not \N;
#end
#begin
-- Spatial data type tests
INSERT INTO geom VALUES (GeomFromWKB(0x0101000000000000000000F03F000000000000F03F));
select y(point(1.25, 3.47)) as y, x(point(1.25, 3.47)) as x;
#end
#begin
-- Signal tests
SIGNAL SQLSTATE '06660' SET MESSAGE_TEXT = 'Database is in read-only mode!';
SIGNAL specialty SET MESSAGE_TEXT = 'An error occurred';
SIGNAL SQLSTATE '01000' SET MESSAGE_TEXT = 'A warning occurred', MYSQL_ERRNO = 1000;
SIGNAL SQLSTATE '77777';
SIGNAL divide_by_zero;
-- Diagnostics tests
RESIGNAL SQLSTATE '06660' SET MESSAGE_TEXT = 'Database is in read-only mode!';
RESIGNAL specialty SET MESSAGE_TEXT = 'An error occurred';
RESIGNAL SQLSTATE '01000' SET MESSAGE_TEXT = 'A warning occurred', MYSQL_ERRNO = 1000;
RESIGNAL SQLSTATE '77777';
RESIGNAL divide_by_zero;
RESIGNAL SET MESSAGE_TEXT = 'Database is in read-only mode!';
RESIGNAL SET MESSAGE_TEXT = 'An error occurred';
RESIGNAL SET MESSAGE_TEXT = 'A warning occurred', MYSQL_ERRNO = 1000;
RESIGNAL;
GET DIAGNOSTICS @p1 = NUMBER, @p2 = ROW_COUNT;
GET DIAGNOSTICS CONDITION 1 @p1 = MYSQL_ERRNO;
GET DIAGNOSTICS CONDITION 1 @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
GET DIAGNOSTICS CONDITION 1 @p3 = RETURNED_SQLSTATE, @p4 = MESSAGE_TEXT;
GET DIAGNOSTICS CONDITION 1 @p5 = SCHEMA_NAME, @p6 = TABLE_NAME;
GET DIAGNOSTICS CONDITION 1 @errno = MYSQL_ERRNO;
GET DIAGNOSTICS @cno = NUMBER;
GET DIAGNOSTICS CONDITION @cno @errno = MYSQL_ERRNO;
GET CURRENT DIAGNOSTICS CONDITION 1 errno = MYSQL_ERRNO, msg = MESSAGE_TEXT;
GET STACKED DIAGNOSTICS CONDITION 1 errno = MYSQL_ERRNO, msg = MESSAGE_TEXT;
GET CURRENT DIAGNOSTICS errcount = NUMBER;
#end
