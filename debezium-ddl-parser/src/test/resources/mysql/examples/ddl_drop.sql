#begin
-- Drop table
drop temporary table if exists temp_t1;
drop temporary table `some_temp_table`;
-- drop table if exists `one_more 1343 *&&^ table`;
drop table antlr_all_tokens, antlr_function_tokens, antlr_keyword_tokens, antlr_tokens, childtable, guns, log_table, new_t, parenttable, ship_class, ships, ships_guns, t1, t2, t3, t4, tab1;
drop table if exists order;
drop index index1 on t1;
drop table tbl_name;
#end
#begin
-- Drop database
drop index index1 on t1 algorithm=default;
drop index index2 on t2 algorithm=default lock none;
drop index index3 on antlr_tokens algorithm default lock=none;
drop index index4 on antlr_tokens lock default;
drop index index5 on antlr_tokens algorithm default;
drop index index6 on antlr_tokens algorithm default lock default;
drop index index7 on antlr_tokens lock default algorithm default;
#end
#begin
-- Drop logfile group
-- Drop Role
DROP ROLE 'admin', 'developer';
DROP ROLE 'webapp'@'localhost';
#end