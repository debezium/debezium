create TABLE PROCESSED AS (
select * FROM T_ORDER_PROCESSED f)
       --     WHERE
       -- TO_CHAR(to_date('20'||f.nr_ano,'YYYY'),'YYYY')||'/'||TRIM(TO_CHAR(f.nr_mes,'00')) = :refCompAcad);