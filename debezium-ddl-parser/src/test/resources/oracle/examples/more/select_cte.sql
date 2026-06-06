-- offset, fetch and order by is acceptable in with clause subquery
WITH t AS (SELECT dummy
             FROM dual
             ORDER BY dummy ASC NULLS LAST
             OFFSET 0 ROWS
             FETCH FIRST ROW ONLY)
SELECT dummy FROM t
ORDER BY dummy ASC NULLS LAST
OFFSET 0 ROWS
FETCH FIRST ROW ONLY;
