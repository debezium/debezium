#begin
-- insert on select
insert into t1 select * from t2;
insert into some_ship_info
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
	) ship_info using (ship_name);
	
INSERT INTO l4stal13prema00.`fusion` ( `partition en` , `classe` , `segment` , `F tot` , `F loc` , `indice specif` ) 
SELECT * FROM f3p1 WHERE 1;
#end
#begin
-- insert base syntax
insert ignore into t1(col1, col2, col3) values ('abc', 0, .12), ('adfasdf',23432, -.12);
INSERT INTO test_auto_inc () VALUES ();
-- http://dev.mysql.com/doc/refman/5.6/en/insert.html
INSERT INTO tbl_name (col1,col2) VALUES(col2*2, 15);
INSERT INTO tbl_name (col1,col2) VALUES(15,col1*2);
INSERT INTO logs (`site_id`, `time`,`hits`) VALUES (1,"2004-08-09", 15) ON DUPLICATE KEY UPDATE hits=hits+15;
INSERT INTO t2 (b, c)
	VALUES ((SELECT a FROM t1 WHERE b='Chip'), 'shoulder'),
	((SELECT a FROM t1 WHERE b='Chip'), 'old block'),
	((SELECT a FROM t1 WHERE b='John'), 'toilet'),
	((SELECT a FROM t1 WHERE b='John'), 'long,silver'),
	((SELECT a FROM t1 WHERE b='John'), 'li''l');
INSERT INTO tbl_test (FirstName)
SELECT 'Aleem' UNION ALL SELECT 'Latif' UNION ALL SELECT 'Mughal';

#end
#begin
-- not latin1 literals
insert into t values ('кириллица', 2, 3);
insert INTO `wptests_posts` (`post_author`, `post_date`, `post_date_gmt`, `post_content`, `post_content_filtered`, `post_title`, `post_excerpt`, `post_status`, `post_type`, `comment_status`, `ping_status`, `post_password`, `post_name`, `to_ping`, `pinged`, `post_modified`, `post_modified_gmt`, `post_parent`, `menu_order`, `post_mime_type`, `guid`) VALUES (7, '2016-09-06 16:49:51', '2016-09-06 16:49:51', '', '', 'صورة', '', 'inherit', 'attachment', 'open', 'closed', '', '%d8%b5%d9%88%d8%b1%d8%a9', '', '', '2016-09-06 16:49:51', '2016-09-06 16:49:51', 0, 0, 'image/jpeg', '');
#end
insert into sql_log values(retGUID,log_type,log_text,0,0,current_user,now());
insert into sql_log values(retGUID,log_type,log_text,0,0,current_user(),now());
#begin
CREATE TABLE tbl (tbl.a BIGINT);
CREATE TABLE tbl (.a BIGINT);
INSERT INTO tbl (tbl.a) SELECT * FROM another_table;
INSERT INTO tbl (.tbl.a) SELECT * FROM another_table;
#end

#begin
---https://dev.mysql.com/doc/refman/8.0/en/insert.html
INSERT INTO t1 (a,b,c) VALUES (1,2,3),(4,5,6) AS new ON DUPLICATE KEY UPDATE c = new.a+new.b; 
#end
#begin
INSERT IGNORE INTO provider_email_address
  (
    org_id,
    provider_id,
    email_address_id,
    is_selected,is_communication_allowed,
    is_refunded,
    refunded_time,
    is_system_credentials_used
  )
  SELECT
    u.org_id,
    char_to_binary(jt.provider_id),
    pea.id,
    tbe.is_selected,
    IFNULL(tbe.is_communication_allowed, TRUE),
    tbe.is_refunded,
    IF(tbe.is_refunded, UTC_TIMESTAMP(6), null),
    tbe.is_system_credentials_used
  FROM temp_bulk_emails tbe
  JOIN person_email_address pea ON pea.email_address = tbe.email_address
  JOIN user u ON u.id = tbe.binary_user_id
  JOIN JSON_TABLE(tbe.providers, '$[*]'
  COLUMNS (provider_id VARCHAR(255) PATH '$')) jt ON 1 = 1
  WHERE tbe.providers IS NOT NULL;
#endif