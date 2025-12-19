CREATE OUTLINE salaries FOR CATEGORY special
   ON SELECT last_name, salary FROM employees;
CREATE OR REPLACE PRIVATE OUTLINE my_salaries
   FROM salaries;
CREATE OR REPLACE OUTLINE public_salaries
   FROM PRIVATE my_salaries;
