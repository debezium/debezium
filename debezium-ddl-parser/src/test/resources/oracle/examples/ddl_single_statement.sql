create TABLE BKP_cc AS (
select * FROM ca.EBS_BALANCETE_CC f)
       --     WHERE
       -- TO_CHAR(to_date('20'||f.nr_ano,'YYYY'),'YYYY')||'/'||TRIM(TO_CHAR(f.nr_mes,'00')) = :refCompAcad);