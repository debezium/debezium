CREATE DEFINER=`vazim`@`%` PROCEDURE `GET_DAILY_DB_ACTIVITY_REPORT`(IN date_from date,IN date_latest date)
BEGIN
IF(date_latest is NULL) THEN
    IF (( select (UP_TIME/60/60/24) FROM DAILY_DB_ACTIVITY where FROM_DATE=(SELECT DATE_ADD(date_from, INTERVAL 1 DAY)) ) < 1) THEN
        SELECT DATE(d1.FROM_DATE) as DATE,
        (d2.Com_select) AS Com_select,
        (d2.Com_insert) AS Com_insert,
        (d2.Com_insert_select) AS Com_insert_select,
        (d2.Com_update) AS Com_update,
        (d2.Com_update_multi) AS Com_update_multi,
        (d2.Com_delete )  AS Com_delete,
        (d2.Com_delete_multi ) AS Com_delete_multi,
        (d2.Com_Questions )  AS Com_Questions,
        (d2.Com_Queries ) AS Com_Queries,
        (d2.Com_commit) AS Com_commit,
        (d2.Com_rollback) AS Com_rollback,
        (d2.Com_call_procedure) AS Com_call_procedure,
        (d2.Created_tmp_disk_tables) AS Created_tmp_disk_tables,
        (d2.Created_tmp_files ) AS Created_tmp_files,
        (d2.Created_tmp_tables) AS Created_tmp_tables,
        (d2.Bytes_received ) AS Bytes_received,
        (d2.Bytes_sent) AS Bytes_sent,
        (d2.Slow_queries) AS SLOW_QUERIES,
        (d2.Max_used_connections - 0) AS MAX_USED_CONNECTIONS
        from DAILY_DB_ACTIVITY d1, DAILY_DB_ACTIVITY d2
        where d1.FROM_DATE=date_from and d2.FROM_DATE=DATE_ADD(date_from, INTERVAL 1 DAY);
        SELECT CONCAT("THE SERVER WAS RESTARTED AND HAD BEEN UP FOR ", ROUND(UP_TIME/60/60,2) ," HOURS ON ",(SELECT FROM_DATE FROM DAILY_DB_ACTIVITY WHERE FROM_DATE=date_from), ". HENCE, THE REPORT SHOWS MYSQL QUERY PARAMETERS SINCE THE SERVER RESTARTED ONLY. ") AS `SERVER RESTART INFORMATION` FROM DAILY_DB_ACTIVITY where (FROM_DATE=(SELECT DATE_ADD(date_from, INTERVAL 1 DAY))) ;
    ELSEIF ( select (UP_TIME/60/60/24) FROM DAILY_DB_ACTIVITY where FROM_DATE=(SELECT DATE_ADD(date_from, INTERVAL 1 DAY)) ) >= 1 THEN
        SELECT DATE(d1.FROM_DATE) as DATE,
        (d2.Com_select-d1.Com_select) AS Com_select,
        (d2.Com_insert - d1.Com_insert) AS Com_insert,
        (d2.Com_insert_select - d1.Com_insert_select) AS Com_insert_select,
        (d2.Com_update - d1.Com_update) AS Com_update,
        (d2.Com_update_multi - d1.Com_update_multi) AS Com_update_multi,
        (d2.Com_delete - d1.Com_delete)  AS Com_delete,
        (d2.Com_delete_multi - d1.Com_delete_multi) AS Com_delete_multi,
        (d2.Com_Questions - d1.Com_Questions)  AS Com_Questions,
        (d2.Com_Queries - d1.Com_Queries ) AS Com_Queries,
        (d2.Com_commit - d1.Com_commit ) AS Com_commit,
        (d2.Com_rollback - d1.Com_rollback) AS Com_rollback,
        (d2.Com_call_procedure - d1.Com_call_procedure) AS Com_call_procedure,
        (d2.Created_tmp_disk_tables - d1.Created_tmp_disk_tables) AS Created_tmp_disk_tables,
        (d2.Created_tmp_files - d1.Created_tmp_files) AS Created_tmp_files,
        (d2.Created_tmp_tables - d1.Created_tmp_tables) AS Created_tmp_tables,
        (d2.Bytes_received - d1.Bytes_received) AS Bytes_received,
        (d2.Bytes_sent - d1.Bytes_sent) AS Bytes_sent,
        (d2.Slow_queries - d1.Slow_queries) AS SLOW_QUERIES,
        (d2.Max_used_connections - 0) AS MAX_USED_CONNECTIONS
        from DAILY_DB_ACTIVITY d1, DAILY_DB_ACTIVITY d2
        where d1.FROM_DATE=date_from and d2.FROM_DATE=DATE_ADD(date_from, INTERVAL 1 DAY);
    END IF;
ELSEIF(date_latest is NOT NULL) THEN
    SELECT DATE(d1.FROM_DATE) as DATE,
    (d2.Com_select) AS Com_select,
    (d2.Com_insert ) AS Com_insert,
    (d2.Com_insert_select) AS Com_insert_select,
    (d2.Com_update) AS Com_update,
    (d2.Com_update_multi) AS Com_update_multi,
    (d2.Com_delete)  AS Com_delete,
    (d2.Com_delete_multi) AS Com_delete_multi,
    (d2.Com_Questions)  AS Com_Questions,
    (d2.Com_Queries ) AS Com_Queries,
    (d2.Com_commit  ) AS Com_commit,
    (d2.Com_rollback) AS Com_rollback,
    (d2.Com_call_procedure ) AS Com_call_procedure,
    (d2.Created_tmp_disk_tables ) AS Created_tmp_disk_tables,
    (d2.Created_tmp_files) AS Created_tmp_files,
    (d2.Created_tmp_tables) AS Created_tmp_tables,
    (d2.Bytes_received) AS Bytes_received,
    (d2.Bytes_sent) AS Bytes_sent,
    (d2.Slow_queries) AS SLOW_QUERIES,
    (d2.Max_used_connections - 0) AS MAX_USED_CONNECTIONS
    from DAILY_DB_ACTIVITY d1 JOIN DAILY_DB_ACTIVITY d2 ON d2.FROM_DATE=d1.TO_DATE WHERE (d1.FROM_DATE BETWEEN date_from AND date_latest) AND (d2.UP_TIME/60/60/24 < 1) ;
    SELECT CONCAT("THE SERVER WAS RESTARTED AND HAD BEEN UP FOR ", ROUND(d2.UP_TIME/60/60,2) , " HOURS ON " , d1.FROM_DATE , ". HENCE, THE REPORT SHOWS MYSQL QUERY PARAMETERS SINCE THE SERVER RESTARTED ONLY. ") AS `SERVER RESTART INFORMATION` FROM DAILY_DB_ACTIVITY d1 JOIN DAILY_DB_ACTIVITY d2 ON d2.FROM_DATE=d1.TO_DATE WHERE (d1.FROM_DATE BETWEEN date_from AND date_latest) AND (d2.UP_TIME/60/60/24 < 1) ;
    SELECT DATE(d1.FROM_DATE) as DATE,
    (d2.Com_select-d1.Com_select) AS Com_select,
    (d2.Com_insert - d1.Com_insert) AS Com_insert,
    (d2.Com_insert_select - d1.Com_insert_select) AS Com_insert_select,
    (d2.Com_update - d1.Com_update) AS Com_update,
    (d2.Com_update_multi - d1.Com_update_multi) AS Com_update_multi,
    (d2.Com_delete - d1.Com_delete)  AS Com_delete,
    (d2.Com_delete_multi - d1.Com_delete_multi) AS Com_delete_multi,
    (d2.Com_Questions - d1.Com_Questions)  AS Com_Questions,
    (d2.Com_Queries - d1.Com_Queries ) AS Com_Queries,
    (d2.Com_commit - d1.Com_commit ) AS Com_commit,
    (d2.Com_rollback - d1.Com_rollback) AS Com_rollback,
    (d2.Com_call_procedure - d1.Com_call_procedure) AS Com_call_procedure,
    (d2.Created_tmp_disk_tables - d1.Created_tmp_disk_tables) AS Created_tmp_disk_tables,
    (d2.Created_tmp_files - d1.Created_tmp_files) AS Created_tmp_files,
    (d2.Created_tmp_tables - d1.Created_tmp_tables) AS Created_tmp_tables,
    (d2.Bytes_received - d1.Bytes_received) AS Bytes_received,
    (d2.Bytes_sent - d1.Bytes_sent) AS Bytes_sent,
    (d2.Slow_queries - d1.Slow_queries) AS SLOW_QUERIES,
    (d2.Max_used_connections - 0) AS MAX_USED_CONNECTIONS
    from DAILY_DB_ACTIVITY d1 JOIN DAILY_DB_ACTIVITY d2 ON d2.FROM_DATE=d1.TO_DATE
    WHERE (d1.FROM_DATE BETWEEN date_from AND date_latest) AND (d2.UP_TIME/60/60/24 >= 1) ;
END IF;
END
