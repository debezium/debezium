CREATE DEFINER=`root`@`localhost` PROCEDURE `drop_old_tables_kenshoointernal`(p_int_to_delete INT)
BEGIN
    INSERT INTO log_tbl (log_name,log_type,message) VALUES ('drop_old_tables_kenshoointernal','debug','end');
END
