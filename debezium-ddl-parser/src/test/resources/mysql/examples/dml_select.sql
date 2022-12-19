#begin
-- common conustructions tests
-- -- Literals
-- -- -- String literal
SELECT 'hello world';
select N'testing conflict on N - spec symbol and N - as identifier' as n;
select n'abc' as tstConstrN;
select N'abc' "bcd" 'asdfasdf' as tstConstNAndConcat;
select 'afdf' "erwhg" "ads" 'dgs' "rter" as tstDiffQuoteConcat;
select 'some string' COLLATE latin1_danish_ci as tstCollate;
select _latin1'some string' COLLATE latin1_danish_ci as tstCollate;
select '\'' as c1, '\"' as c2, '\b' as c3, '\n' as c4, '\r' as c5, '\t' as c6, '\Z' as c7, '\\' as c8, '\%' as c9, '\_' as c10;
select * from t1 for update skip locked;
select * from t1 lock in share mode nowait;
#end
#begin
-- -- -- String literal spec symbols
-- bug: two symbols ' afer each other: ''
select '\'Quoted string\'' col1, 'backslash \\ ' ', two double quote "" ' ', two single quote ''' as col2;
select '\'Quoted string\' ' col1, 'backslash \\ ' ', two double quote "" ' ', two single quote ''' as col2;
select * from `select` where `varchar` = 'abc \' ' and `varchar2` = '\'bca';
#end
#begin
-- -- -- Number literal
SELECT 1;
select 1.e-3 as 123e;
select del1.e123 as c from del1;
select -1, 3e-2, 2.34E0;
SELECT -4.1234e-2, 0.2e-3 as c;
SELECT .1e10;
SELECT -.1e10;
select 15e3, .2e5 as col1;
select .2e3 c1, .2e-4 as c5;
#end
#begin
-- -- -- Number float collision test
select t1e2 as e1 from t;
select 1e2t as col from t;
#end
#begin
-- -- -- Hexadecimal literal
select X'4D7953514C';
select x'4D7953514C';
select 0x636174;
select 0x636174 c1;
select x'4D7953514C' c1, 0x636174 c2;
select x'79' as `select`, 0x2930 cc, 0x313233 as c2;

#end
#begin
-- -- -- Null literal
SELECT null;
SELECT not null;
select \N;
select ((\N));
select not ((\N));
#end
#begin
-- -- -- mixed literals
select \N as c1, null as c2, N'string';
select 4e15 colum, 'hello, ' 'world', X'53514C';
select 'abc' ' bcd' ' \' \' ' as col, \N c2, -.1e-3;
#end

#begin
-- -- Variables
SELECT @myvar;
#end

#begin
-- select_column tests
select * from `select`;
select *, `select`.*, `select`.* from `select`;
select *, 'abc' from `select`;
select *, 1, \N, N'string' 'string2' from `select`;
#end

#begin
-- UNION tests
select 1 union select 2 limit 0,5;
select * from (select 1 union select 2 union select 0) as t order by 1 limit 0,10;
select col1 from t1 union select * from (select 1 as col2) as newt;
select col1 from t1 union (select * from (select 1 as col2) as newt);
select 1 as c1 union (((select 2)));
#end
#begin
-- -- -- subquery in UNION
select 1 union select * from (select 2 union select 3) as table1;
select 1 union (select * from (select 2 union select 3) as table1);
#end
#begin
-- subquery FROM
select * from (((((((select col1 from t1) as ttt))))));
select ship_power.gun_power, ship_info.*
FROM
	(
		select s.name as ship_name, sum(g.power) as gun_power, max(callibr) as max_callibr
		from
			ships s inner join ships_guns sg on s.id = sg.ship_id inner join guns g on g.id = sg.guns_id
		group by s.name
	) ship_power
	inner join
	(
		select s.name as ship_name, sc.class_name, sc.tonange, sc.max_length, sc.start_build, sc.max_guns_size
		from
			ships s inner join ship_class sc on s.class_id = sc.id
	) ship_info using (ship_name)
order by ship_power.ship_name;
#end
#begin
-- JOIN
-- -- -- join condition
select * from t1 inner join (t1 as tt1, t2 as tt2) on t1.col1 = tt1.col1;
select * from  (t1 as tt1, t2 as tt2) inner join t1 on t1.col1 = tt1.col1;
select * from  t1 as tt1, t2 as tt2 inner join t1 on true;
#end
#begin
-- where_condition test
select col1 from t1 inner join t2 on (t1.col1 = t2.col2);
#end
#begin
-- identifiers tests
select 1 as 123e;
#end
#begin
-- not latin1 literals
select CONVERT( LEFT( CONVERT( '自動下書き' USING binary ), 100 ) USING utf8 ) AS x_0;
select CONVERT( LEFT( CONVERT( '自動' USING binary ), 6 ) USING utf8 ) AS x_0;
select  t.*, tt.* FROM wptests_terms AS t  INNER JOIN wptests_term_taxonomy AS tt ON t.term_id = tt.term_id WHERE tt.taxonomy IN ('category') AND t.name IN ('远征手记') ORDER BY t.name ASC;
#end
#begin
-- cast as integer
SELECT CAST('1' AS INT);
SELECT CAST('1' AS INTEGER);
#end
#begin
-- JSON functions
SELECT JSON_ARRAY(1, "abc", NULL, TRUE, CURTIME());
SELECT JSON_OBJECT('id', 87, 'name', 'carrot');
SELECT JSON_QUOTE('null'), JSON_QUOTE('"null"');
SELECT JSON_CONTAINS(@j, @j2, '$.a');
SELECT JSON_CONTAINS_PATH(@j, 'one', '$.a', '$.e');
SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]');
SELECT JSON_KEYS('{"a": 1, "b": {"c": 30}}');
SELECT JSON_OVERLAPS("[1,3,5,7]", "[2,5,7]");
SELECT JSON_SEARCH(@j, 'one', 'abc');
SELECT JSON_ARRAY_APPEND(@j, '$[1]', 1);
SELECT JSON_ARRAY_INSERT(@j, '$[1]', 'x');
SELECT JSON_INSERT(@j, '$.a', 10, '$.c', '[true, false]');
SELECT JSON_MERGE('[1, 2]', '[true, false]');
SELECT JSON_MERGE_PATCH('[1, 2]', '[true, false]');
SELECT JSON_MERGE_PRESERVE('[1, 2]', '[true, false]');
SELECT JSON_REMOVE(@j, '$[1]');
SELECT JSON_REPLACE(@j, '$.a', 10, '$.c', '[true, false]');
SELECT JSON_SET(@j, '$.a', 10, '$.c', '[true, false]');
SELECT @j, JSON_UNQUOTE(@j);
SELECT JSON_DEPTH('{}'), JSON_DEPTH('[]'), JSON_DEPTH('true');
SELECT JSON_LENGTH('[1, 2, {"a": 3}]');
SELECT JSON_TYPE(@j);
SELECT JSON_VALID('{"a": 1}');
SELECT JSON_SCHEMA_VALID(@schema, @document);
SELECT JSON_SCHEMA_VALIDATION_REPORT(@schema, @document);
SELECT JSON_PRETTY('123');
SELECT JSON_STORAGE_FREE(jcol), JSON_STORAGE_FREE(jcol) FROM jtable;
SELECT o_id, JSON_ARRAYAGG(attribute) AS attributes FROM t3 GROUP BY o_id;
SELECT o_id, JSON_OBJECTAGG(attribute, value) FROM t3 GROUP BY o_id;
#end
SELECT trigger.num FROM test `trigger`;
-- Valid when SELECT is in stored procedure
SELECT * FROM test LIMIT LIMIT1,LIMIT2;
-- Functions
SELECT mod(3,2);
SELECT SCHEMA();
-- Non Aggregate Functions
SELECT pk, LEAD(pk) OVER (ORDER BY pk) AS l;
SELECT COALESCE(LAG(last_eq.end_variation) OVER (PARTITION BY eq.account_id, eq.execution_name_id, eq.currency ORDER BY eq.start_date), 0) AS start_variation FROM t1;

#begin
-- Window Functions
SELECT
  year, country, product, profit,
  SUM(profit) OVER() AS total_profit,
  SUM(profit) OVER(PARTITION BY country) AS country_profit
FROM sales
  ORDER BY country, year, product, profit;
SELECT
  year, country, product, profit,
  ROW_NUMBER() OVER(PARTITION BY country) AS row_num1,
  ROW_NUMBER() OVER(PARTITION BY country ORDER BY year, product) AS row_num2
FROM sales;
SELECT
    e.id,
    SUM(e.bin_volume) AS bin_volume,
    SUM(e.bin_volume) OVER(PARTITION BY id, e.bin_volume ORDER BY id) AS bin_volume_o,
    COALESCE(bin_volume, 0) AS bin_volume2,
    COALESCE(LAG(e.bin_volume) OVER(PARTITION BY id ORDER BY e.id), 0) AS bin_volume3,
    FIRST_VALUE(id) OVER() AS fv,
    DENSE_RANK() OVER(PARTITION BY bin_name ORDER BY id) AS drk,
    RANK() OVER(PARTITION BY bin_name) AS rk,
    ROW_NUMBER ( ) OVER(PARTITION BY bin_name) AS rn,
    NTILE(2) OVER() AS nt
FROM table1 e;
SELECT
    id,
    SUM(bin_volume) OVER w AS bin_volume_o,
    LAG(bin_volume) OVER w AS bin_volume_l,
    LAG(bin_volume, 2) OVER w AS bin_volume_l2,
    FIRST_VALUE(id) OVER w2 AS fv,
    GROUP_CONCAT(bin_volume order by id) AS `rank`
FROM table2
    WINDOW w AS (PARTITION BY id, bin_volume ORDER BY id ROWS UNBOUNDED PRECEDING),
           w2 AS (PARTITION BY id, bin_volume ORDER BY id DESC ROWS 10 PRECEDING);
#end

#begin
-- https://dev.mysql.com/doc/refman/8.0/en/lateral-derived-tables.html
SELECT
  salesperson.name,
  max_sale.amount,
  max_sale.customer_name
FROM
  salesperson,
  LATERAL
  (SELECT amount, customer_name
    FROM all_sales
    WHERE all_sales.salesperson_id = salesperson.id
    ORDER BY amount DESC LIMIT 1)
  AS max_sale;
#end

#begin
-- From MariaDB 10.1.2, pre-query variables are supported
-- src: https://mariadb.com/kb/en/set-statement/
SET STATEMENT some_statement=60 FOR SELECT a FROM some_table;
#end

-- Index hints: https://dev.mysql.com/doc/refman/8.0/en/index-hints.html
SELECT * FROM table1 USE INDEX (col1_index,col2_index) WHERE col1=1 AND col2=2 AND col3=3;
SELECT * FROM table1 FORCE INDEX (col1_index,col2_index) WHERE col1=1 AND col2=2 AND col3=3;
SELECT * FROM t1 USE INDEX (PRIMARY) ORDER BY a;
SELECT * FROM t1 FORCE INDEX (PRIMARY) ORDER BY a;

-- JSON_TABLE
-- https://dev.mysql.com/doc/refman/8.0/en/json-table-functions.html
SELECT *
    FROM
        JSON_TABLE (
           '[{"a":"3"},{"a":2},{"b":1},{"a":0},{"a":[1,2]}]',
           "$[*]"
         COLUMNS (
           rowid FOR ORDINALITY,
           ac VARCHAR(100) PATH "$.a" DEFAULT '111' ON EMPTY DEFAULT '999' ON ERROR,
           aj JSON PATH "$.a" DEFAULT '{"x": 333}' ON EMPTY,
           bx INT EXISTS PATH "$.b",
           NESTED PATH '$.b[*]' COLUMNS (b INT PATH '$')
         )
        ) AS tt;

