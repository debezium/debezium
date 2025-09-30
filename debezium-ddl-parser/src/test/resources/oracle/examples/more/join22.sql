SELECT *
  FROM employees e
     , LATERAL(SELECT * FROM departments d
                WHERE e.department_id = d.department_id);