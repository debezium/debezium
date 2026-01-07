-- https://stackoverflow.com/questions/10043445/oracle-connect-by-clause-after-group-by-clause
-- CONNECT BY comes after GROUP BY
SELECT deptno,
       LTRIM(MAX(SYS_CONNECT_BY_PATH(ename,','))
       KEEP (DENSE_RANK LAST ORDER BY curr),',') AS employees
FROM   (SELECT deptno,
               ename,
               ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY ename) AS curr,
               ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY ename) -1 AS prev
        FROM   emp)
GROUP BY deptno
CONNECT BY prev = PRIOR curr AND deptno = PRIOR deptno
START WITH curr = 1;
