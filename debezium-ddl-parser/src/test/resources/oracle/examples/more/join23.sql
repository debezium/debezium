SELECT d.department_name, v.employee_id, v.last_name
  FROM departments d
 CROSS APPLY (SELECT * FROM employees e
               WHERE e.department_id = d.department_id) v
 WHERE d.department_name IN ('Marketing', 'Operations')
 ORDER BY d.department_name, v.employee_id;
