#begin
-- Recursive CTE
WITH RECURSIVE cte (n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM cte WHERE n < 10
)
SELECT n FROM cte;

WITH RECURSIVE cte AS (
  SELECT id, name, manager_id
  FROM employees
  WHERE id = 1
  UNION ALL
  SELECT e.id, e.name, e.manager_id
  FROM employees e
  JOIN cte ON e.manager_id = cte.id
)
SELECT * FROM cte;

WITH RECURSIVE cte AS (
  SELECT id, name, parent_id
  FROM departments
  WHERE id = 1
  UNION ALL
  SELECT d.id, d.name, d.parent_id
  FROM departments d
  JOIN cte ON d.parent_id = cte.id
)
SELECT * FROM cte;
#end
#begin
--Non-recursive Ctes
WITH cte1 AS (
  SELECT * FROM table1 WHERE col1 = 'value'
),
cte2 AS (
  SELECT * FROM table2 WHERE col2 = 'value'
)
SELECT cte1.col1, cte2.col2 FROM cte1 JOIN cte2 ON cte1.id = cte2.id;
#end
